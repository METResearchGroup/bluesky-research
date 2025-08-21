"""
BERTopic Wrapper for Topic Modeling Pipeline

This module provides a generic, reusable BERTopic modeling pipeline that accepts
pre-cleaned DataFrames and produces topic assignments with quality monitoring.
It implements GPU optimization, YAML configuration, and comprehensive error handling.

Author: AI Agent implementing MET-34
Date: 2025-01-20
"""

import logging
import os
import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import yaml
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BERTopicWrapper:
    """
    A wrapper class for BERTopic that provides a generic, configurable
    topic modeling pipeline with quality monitoring and GPU optimization.

    This class implements the core requirements from MET-34:
    - Generic pipeline accepting any DataFrame with text column
    - YAML configuration for all BERTopic and embedding parameters
    - Topic quality monitoring with coherence metrics
    - GPU optimization for large-scale processing
    - Reproducible results with random seed control
    """

    def __init__(
        self,
        config_path: Optional[Union[str, Path]] = None,
        config_dict: Optional[Dict[str, Any]] = None,
        random_seed: Optional[int] = None,
    ):
        """
        Initialize the BERTopic wrapper with configuration.

        Args:
            config_path: Path to YAML configuration file
            config_dict: Dictionary configuration (alternative to config_path)
            random_seed: Random seed for reproducible results

        Raises:
            FileNotFoundError: If config_path doesn't exist
            yaml.YAMLError: If YAML file is invalid
        """
        self.config = self._load_config(config_path, config_dict)
        self.random_seed = random_seed or self.config.get("random_seed", 42)

        # Set random seeds for reproducibility
        np.random.seed(self.random_seed)
        os.environ["PYTHONHASHSEED"] = str(self.random_seed)

        # Initialize components
        self.embedding_model = None
        self.topic_model = None
        self.quality_metrics = {}
        self.training_time = None
        self._training_results = {}

        # Validate configuration
        self._validate_config()

        logger.info(f"BERTopicWrapper initialized with random seed: {self.random_seed}")

    def _load_config(
        self,
        config_path: Optional[Union[str, Path]] = None,
        config_dict: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Load configuration from YAML file or dictionary.

        Args:
            config_path: Path to YAML configuration file
            config_dict: Dictionary configuration

        Returns:
            Configuration dictionary with defaults applied
        """
        if config_dict is not None:
            config = config_dict
        elif config_path is not None:
            config_path = Path(config_path)
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")

            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise yaml.YAMLError(f"Invalid YAML configuration: {e}")
        else:
            # Use default config.yaml file in the same directory
            default_config_path = Path(__file__).parent / "config.yaml"
            if not default_config_path.exists():
                raise FileNotFoundError(
                    f"Default configuration file not found: {default_config_path}"
                )

            try:
                with open(default_config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                logger.info(f"Loaded default configuration from {default_config_path}")
            except yaml.YAMLError as e:
                raise yaml.YAMLError(f"Invalid default YAML configuration: {e}")

        # Apply default configuration
        return self._apply_defaults(config)

    def _apply_defaults(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply default configuration values.

        Args:
            config: User configuration

        Returns:
            Configuration with defaults applied
        """
        defaults = {
            "embedding_model": {
                "name": "all-MiniLM-L6-v2",
                "device": "auto",  # Will auto-detect GPU if available
                "batch_size": 32,
            },
            "bertopic": {
                "top_n_words": 20,
                "min_topic_size": 15,
                "nr_topics": "auto",
                "calculate_probabilities": True,
                "verbose": True,
            },
            "quality_thresholds": {"c_v_min": 0.4, "c_npmi_min": 0.1},
            "gpu_optimization": {
                "enable": True,
                "max_batch_size": 128,
                "memory_threshold": 0.8,  # Use GPU if memory usage < 80%
            },
            "random_seed": 42,
        }

        # Deep merge configuration
        merged_config = self._deep_merge(defaults, config)
        return merged_config

    def _deep_merge(
        self, base: Dict[str, Any], update: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.

        Args:
            base: Base dictionary
            update: Update dictionary

        Returns:
            Merged dictionary
        """
        result = base.copy()
        for key, value in update.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def _validate_config(self) -> None:
        """
        Validate configuration parameters.

        Raises:
            ValueError: If configuration is invalid
        """
        # Validate embedding model
        if not isinstance(self.config["embedding_model"]["name"], str):
            raise ValueError("embedding_model.name must be a string")

        # Validate quality thresholds
        thresholds = self.config["quality_thresholds"]
        if not (0 <= thresholds["c_v_min"] <= 1):
            raise ValueError("c_v_min must be between 0 and 1")
        if not (0 <= thresholds["c_npmi_min"] <= 1):
            raise ValueError("c_npmi_min must be between 0 and 1")

        # Validate GPU optimization
        gpu_config = self.config["gpu_optimization"]
        if not isinstance(gpu_config["enable"], bool):
            raise ValueError("gpu_optimization.enable must be a boolean")
        if gpu_config["max_batch_size"] <= 0:
            raise ValueError("max_batch_size must be positive")
        if not (0 < gpu_config["memory_threshold"] <= 1):
            raise ValueError("memory_threshold must be between 0 and 1")

        logger.info("Configuration validation passed")

    def _detect_gpu(self) -> str:
        """
        Detect available GPU and return device string.

        Returns:
            Device string ('cuda', 'mps', or 'cpu')
        """
        try:
            import torch

            if torch.cuda.is_available():
                return "cuda"
            elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                return "mps"
        except ImportError:
            pass

        return "cpu"

    def _initialize_embedding_model(self) -> None:
        """
        Initialize the Sentence Transformer embedding model.
        """
        config = self.config["embedding_model"]
        device = config["device"]

        if device == "auto":
            device = self._detect_gpu()

        logger.info(
            f"Initializing embedding model '{config['name']}' on device '{device}'"
        )

        try:
            self.embedding_model = SentenceTransformer(config["name"], device=device)
            logger.info(f"Embedding model initialized successfully on {device}")
        except Exception as e:
            logger.error(f"Failed to initialize embedding model: {e}")
            raise

    def _validate_input_data(self, df: pd.DataFrame, text_column: str) -> None:
        """
        Validate input DataFrame and text column.

        Args:
            df: Input DataFrame
            text_column: Name of the text column

        Raises:
            ValueError: If input validation fails
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")

        if df.empty:
            raise ValueError("DataFrame is empty")

        if text_column not in df.columns:
            raise ValueError(f"Text column '{text_column}' not found in DataFrame")

        if df[text_column].isna().all():
            raise ValueError(f"Text column '{text_column}' contains only NaN values")

        # Check for non-string values
        non_string_mask = ~df[text_column].astype(str).str.len().isna()
        if not non_string_mask.all():
            logger.warning(
                f"Found {non_string_mask.sum()} non-string values in text column"
            )

        logger.info(
            f"Input validation passed: {len(df)} documents, text column: {text_column}"
        )

    def _generate_embeddings(
        self, texts: List[str], batch_size: Optional[int] = None
    ) -> np.ndarray:
        """
        Generate embeddings for input texts.

        Args:
            texts: List of text strings
            batch_size: Batch size for processing (uses config default if None)

        Returns:
            Embedding matrix
        """
        if self.embedding_model is None:
            self._initialize_embedding_model()

        batch_size = batch_size or self.config["embedding_model"]["batch_size"]

        logger.info(
            f"Generating embeddings for {len(texts)} documents with batch size {batch_size}"
        )

        try:
            embeddings = self.embedding_model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=True,
                convert_to_numpy=True,
            )
            logger.info(f"Embeddings generated successfully: {embeddings.shape}")
            return embeddings
        except Exception as e:
            logger.error(f"Failed to generate embeddings: {e}")
            raise

    def _calculate_coherence_metrics(
        self, topic_model: BERTopic, texts: List[str], embeddings: np.ndarray
    ) -> Dict[str, float]:
        """
        Calculate topic coherence metrics.

        Args:
            topic_model: Trained BERTopic model
            texts: Input texts
            embeddings: Text embeddings

        Returns:
            Dictionary of coherence metrics
        """
        logger.info("Calculating topic coherence metrics")

        try:
            # Get topic words
            topic_words = topic_model.get_topics()

            # Calculate c_v coherence
            c_v_scores = []
            for topic_id, words in topic_words.items():
                if topic_id != -1:  # Skip outlier topic
                    topic_word_embeddings = []
                    for word_tuple in words[:10]:  # Top 10 words per topic
                        word = word_tuple[0]  # Extract word from (word, weight) tuple
                        # Find documents containing this word
                        word_docs = [
                            i for i, text in enumerate(texts) if word in text.lower()
                        ]
                        if word_docs:
                            topic_word_embeddings.append(
                                embeddings[word_docs].mean(axis=0)
                            )

                    if len(topic_word_embeddings) > 1:
                        # Calculate cosine similarity between word embeddings
                        similarities = cosine_similarity(topic_word_embeddings)
                        c_v_scores.append(
                            np.mean(
                                similarities[np.triu_indices_from(similarities, k=1)]
                            )
                        )

            c_v_mean = np.mean(c_v_scores) if c_v_scores else 0.0

            # Calculate c_npmi (simplified version)
            c_npmi_scores = []
            for topic_id, words in topic_words.items():
                if topic_id != -1:
                    # Count co-occurrences of top words
                    word_pairs = [
                        (words[i][0], words[j][0])
                        for i in range(len(words))
                        for j in range(i + 1, len(words))
                    ]
                    if word_pairs:
                        pair_scores = []
                        for word1, word2 in word_pairs[:20]:  # Limit to top 20 pairs
                            # Count documents containing both words
                            both_count = sum(
                                1
                                for text in texts
                                if word1 in text.lower() and word2 in text.lower()
                            )
                            word1_count = sum(
                                1 for text in texts if word1 in text.lower()
                            )
                            word2_count = sum(
                                1 for text in texts if word2 in text.lower()
                            )

                            if both_count > 0 and word1_count > 0 and word2_count > 0:
                                # Simplified NPMI calculation
                                pmi = np.log(
                                    both_count
                                    * len(texts)
                                    / (word1_count * word2_count)
                                )
                                npmi = pmi / (-np.log(both_count / len(texts)))
                                pair_scores.append(npmi)

                        if pair_scores:
                            c_npmi_scores.append(np.mean(pair_scores))

            c_npmi_mean = np.mean(c_npmi_scores) if c_npmi_scores else 0.0

            metrics = {
                "c_v_mean": c_v_mean,
                "c_npmi_mean": c_npmi_mean,
                "c_v_scores": c_v_scores,
                "c_npmi_scores": c_npmi_scores,
            }

            logger.info(
                f"Coherence metrics calculated: c_v={c_v_mean:.3f}, c_npmi={c_npmi_mean:.3f}"
            )
            return metrics

        except Exception as e:
            logger.warning(f"Failed to calculate coherence metrics: {e}")
            return {
                "c_v_mean": 0.0,
                "c_npmi_mean": 0.0,
                "c_v_scores": [],
                "c_npmi_scores": [],
            }

    def _check_quality_thresholds(
        self, metrics: Dict[str, float]
    ) -> Tuple[bool, List[str]]:
        """
        Check if topic quality meets thresholds.

        Args:
            metrics: Coherence metrics

        Returns:
            Tuple of (meets_thresholds, warning_messages)
        """
        thresholds = self.config["quality_thresholds"]
        warnings = []

        c_v_ok = metrics["c_v_mean"] >= thresholds["c_v_min"]
        c_npmi_ok = metrics["c_npmi_mean"] >= thresholds["c_npmi_min"]

        if not c_v_ok:
            warnings.append(
                f"c_v score ({metrics['c_v_mean']:.3f}) below threshold ({thresholds['c_v_min']})"
            )

        if not c_npmi_ok:
            warnings.append(
                f"c_npmi score ({metrics['c_npmi_mean']:.3f}) below threshold ({thresholds['c_npmi_min']})"
            )

        meets_thresholds = c_v_ok and c_npmi_ok

        if warnings:
            logger.warning("Topic quality below thresholds: " + "; ".join(warnings))
        else:
            logger.info("Topic quality meets all thresholds")

        return meets_thresholds, warnings

    def fit(self, df: pd.DataFrame, text_column: str = "text") -> "BERTopicWrapper":
        """
        Fit the BERTopic model to the input data.

        Args:
            df: Input DataFrame containing text data
            text_column: Name of the column containing text

        Returns:
            Self for method chaining

        Raises:
            ValueError: If input validation fails
            RuntimeError: If model training fails
        """
        start_time = time.time()

        try:
            # Validate input
            self._validate_input_data(df, text_column)

            # Extract texts
            texts = df[text_column].astype(str).tolist()
            logger.info(f"Starting BERTopic training on {len(texts)} documents")

            # Generate embeddings
            embeddings = self._generate_embeddings(texts)

            # Initialize BERTopic model
            bertopic_config = self.config["bertopic"]
            self.topic_model = BERTopic(**bertopic_config)

            # Train model
            logger.info("Training BERTopic model...")
            topics, probs = self.topic_model.fit_transform(texts, embeddings)

            # Calculate quality metrics
            self.quality_metrics = self._calculate_coherence_metrics(
                self.topic_model, texts, embeddings
            )

            # Check quality thresholds
            meets_thresholds, warnings = self._check_quality_thresholds(
                self.quality_metrics
            )

            # Store results
            self.training_time = time.time() - start_time
            self._training_results = {
                "topics": topics,
                "probabilities": probs,
                "texts": texts,
                "embeddings": embeddings,
                "meets_thresholds": meets_thresholds,
                "warnings": warnings,
            }

            logger.info(
                f"BERTopic training completed in {self.training_time:.2f} seconds"
            )
            logger.info(
                f"Generated {len(set(topics)) - (1 if -1 in topics else 0)} topics"
            )

            if warnings:
                logger.warning("Quality warnings: " + "; ".join(warnings))

            return self

        except Exception as e:
            logger.error(f"BERTopic training failed: {e}")
            raise RuntimeError(f"Model training failed: {e}")

    def get_topics(self) -> Dict[int, List[Tuple[str, float]]]:
        """
        Get the learned topics.

        Returns:
            Dictionary mapping topic IDs to lists of (word, weight) tuples

        Raises:
            RuntimeError: If model hasn't been trained
        """
        if self.topic_model is None:
            raise RuntimeError("Model must be trained before getting topics")

        return self.topic_model.get_topics()

    def get_topic_info(self) -> pd.DataFrame:
        """
        Get comprehensive topic information.

        Returns:
            DataFrame with topic information

        Raises:
            RuntimeError: If model hasn't been trained
        """
        if self.topic_model is None:
            raise RuntimeError("Model must be trained before getting topic info")

        return self.topic_model.get_topic_info()

    def get_representative_docs(self, topic_id: int) -> List[str]:
        """
        Get representative documents for a topic.

        Args:
            topic_id: Topic ID

        Returns:
            List of representative document texts

        Raises:
            RuntimeError: If model hasn't been trained
        """
        if self.topic_model is None:
            raise RuntimeError(
                "Model must be trained before getting representative docs"
            )

        return self.topic_model.get_representative_docs(topic_id)

    def get_quality_metrics(self) -> Dict[str, Any]:
        """
        Get topic quality metrics.

        Returns:
            Dictionary of quality metrics and training information
        """
        metrics = self.quality_metrics.copy()
        metrics.update(
            {
                "training_time": self.training_time,
                "meets_thresholds": self._training_results.get(
                    "meets_thresholds", False
                ),
                "warnings": self._training_results.get("warnings", []),
                "random_seed": self.random_seed,
            }
        )
        return metrics

    def save_model(self, filepath: Union[str, Path]) -> None:
        """
        Save the trained model to disk.

        Args:
            filepath: Path where to save the model

        Raises:
            RuntimeError: If model hasn't been trained
        """
        if self.topic_model is None:
            raise RuntimeError("Model must be trained before saving")

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(filepath, "wb") as f:
                pickle.dump(self, f)
            logger.info(f"Model saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            raise

    @classmethod
    def load_model(cls, filepath: Union[str, Path]) -> "BERTopicWrapper":
        """
        Load a trained model from disk.

        Args:
            filepath: Path to the saved model

        Returns:
            Loaded BERTopicWrapper instance

        Raises:
            FileNotFoundError: If model file doesn't exist
            RuntimeError: If loading fails
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Model file not found: {filepath}")

        try:
            with open(filepath, "rb") as f:
                instance = pickle.load(f)
            logger.info(f"Model loaded from {filepath}")
            return instance
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise RuntimeError(f"Model loading failed: {e}")

    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration.

        Returns:
            Configuration dictionary
        """
        return self.config.copy()

    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration parameters.

        Args:
            updates: Dictionary of configuration updates

        Raises:
            ValueError: If updates are invalid
        """
        # Create new config with updates
        new_config = self._deep_merge(self.config, updates)

        # Validate new config
        old_config = self.config
        self.config = new_config
        try:
            self._validate_config()
        except ValueError as e:
            self.config = old_config
            raise ValueError(f"Invalid configuration update: {e}")

        logger.info("Configuration updated successfully")

    def __repr__(self) -> str:
        """String representation of the wrapper."""
        status = "trained" if self.topic_model is not None else "untrained"
        return f"BERTopicWrapper(status={status}, random_seed={self.random_seed})"
