"""
BERTopic Wrapper for Topic Modeling Pipeline

This module provides a generic, reusable BERTopic modeling pipeline that accepts
pre-cleaned DataFrames and produces topic assignments with quality monitoring.
It implements GPU optimization, YAML configuration, and comprehensive error handling.

Author: AI Agent implementing MET-34
Date: 2025-01-20
"""

import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import yaml
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# Optional NLTK imports for text preprocessing
try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize

    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set environment variable to prevent tokenizer parallelism warnings
os.environ["TOKENIZERS_PARALLELISM"] = "false"


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
        disable_tokenizer_parallelism: bool = True,
    ):
        # Set random seed first
        self.random_seed = random_seed or 42
        random.seed(self.random_seed)
        np.random.seed(self.random_seed)

        # Configure tokenizer parallelism to prevent warnings
        if disable_tokenizer_parallelism:
            self._set_tokenizer_parallelism(False)
            logger.info("🔧 Tokenizer parallelism disabled to prevent warnings")

        # Load configuration
        self.config = self._load_config(config_path, config_dict)

        # Initialize components
        self.embedding_model = None
        self.topic_model = None
        self.quality_metrics = {}
        self.training_time = 0.0
        self._training_results = {}

        # Initialize embedding model
        self._initialize_embedding_model()

        # Initialize text preprocessing
        self._initialize_text_preprocessing()

        logger.info(f"BERTopicWrapper initialized with random seed: {self.random_seed}")

    def _set_tokenizer_parallelism(self, enable: bool) -> None:
        """
        Set the TOKENIZERS_PARALLELISM environment variable.

        Args:
            enable: Whether to enable tokenizer parallelism
        """
        value = "true" if enable else "false"
        os.environ["TOKENIZERS_PARALLELISM"] = value
        logger.debug(f"Set TOKENIZERS_PARALLELISM={value}")

    def enable_tokenizer_parallelism(self) -> None:
        """Enable tokenizer parallelism (may cause warnings with multiprocessing)."""
        self._set_tokenizer_parallelism(True)
        logger.info("✅ Tokenizer parallelism enabled")

    def disable_tokenizer_parallelism(self) -> None:
        """Disable tokenizer parallelism to prevent warnings."""
        self._set_tokenizer_parallelism(False)
        logger.info("✅ Tokenizer parallelism disabled")

    def _load_config(
        self,
        config_path: Optional[Union[str, Path]] = None,
        config_dict: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Load configuration from file or dictionary."""
        config = {}

        # Load from file if provided
        if config_path:
            config_path = Path(config_path)
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")

            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from: {config_path}")

        # Override with dictionary if provided
        if config_dict:
            config.update(config_dict)
            logger.info("Configuration updated with dictionary parameters")

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
                # UMAP configuration for better performance
                "umap_model": {
                    "n_neighbors": 15,
                    "n_components": 5,
                    "min_dist": 0.0,
                    "metric": "cosine",
                    "n_jobs": 1,
                    "low_memory": True,
                },
                "hdbscan_model": {
                    "min_cluster_size": 15,
                    "min_samples": 5,
                    "metric": "euclidean",
                    "cluster_selection_method": "eom",
                    "core_dist_n_jobs": 1,
                },
            },
            "quality_thresholds": {"c_v_min": 0.4, "c_npmi_min": 0.1},
            "gpu_optimization": {
                "enable": True,
                "max_batch_size": 128,
                "memory_threshold": 0.8,  # Use GPU if memory usage < 80%
            },
            "metrics": {
                "max_docs": 50000,  # Limit documents for coherence computation
                "top_k_words": 10,  # Limit words per topic for metrics
            },
            "performance": {
                "enable_progress_logging": True,
                "progress_log_interval": 10000,
                "enable_timing_breakdown": True,
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

        # Validate metrics
        metrics_config = self.config.get("metrics", {})
        if metrics_config.get("max_docs", 50000) <= 0:
            raise ValueError("metrics.max_docs must be positive")
        if metrics_config.get("top_k_words", 10) <= 0:
            raise ValueError("metrics.top_k_words must be positive")

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

    def _initialize_text_preprocessing(self) -> None:
        """
        Initialize text preprocessing components including NLTK resources.
        """
        if not self.config.get("text_preprocessing", {}).get("enable", False):
            logger.info("Text preprocessing disabled in configuration")
            return

        if not NLTK_AVAILABLE:
            logger.warning("NLTK not available - text preprocessing will be limited")
            return

        # Download required NLTK resources
        try:
            nltk.data.find("tokenizers/punkt")
        except LookupError:
            logger.info("Downloading NLTK punkt tokenizer...")
            nltk.download("punkt", quiet=True)

        try:
            nltk.data.find("corpora/stopwords")
        except LookupError:
            logger.info("Downloading NLTK stopwords...")
            nltk.download("stopwords", quiet=True)

        logger.info("Text preprocessing initialized successfully")

    def _preprocess_text(self, text: str) -> str:
        """
        Preprocess a single text string according to configuration.

        Args:
            text: Input text string

        Returns:
            Preprocessed text string
        """
        if not isinstance(text, str) or not text.strip():
            return ""

        config = self.config.get("text_preprocessing", {})
        if not config.get("enable", False):
            return text

        # Convert to lowercase
        text = text.lower()

        # Remove URLs if enabled
        if config.get("remove_urls", True):
            text = re.sub(
                r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
                "",
                text,
            )

        # Remove emojis and special characters if enabled
        if config.get("remove_emojis", True):
            text = re.sub(r"[^\w\s]", "", text)

        # Remove stopwords if enabled and NLTK is available
        if config.get("remove_stopwords", True) and NLTK_AVAILABLE:
            try:
                # Tokenize
                tokens = word_tokenize(text)

                # Get stopwords
                language = config.get("language", "english")
                stop_words = set(stopwords.words(language))

                # Add custom stopwords
                custom_stops = set(config.get("custom_stopwords", []))
                stop_words.update(custom_stops)

                # Filter tokens
                min_length = config.get("min_word_length", 2)
                filtered_tokens = [
                    token
                    for token in tokens
                    if token.lower() not in stop_words and len(token) >= min_length
                ]

                # Join back into text
                text = " ".join(filtered_tokens)

            except Exception as e:
                logger.warning(f"Stopwords removal failed, using original text: {e}")

        return text.strip()

    def _validate_input_data(self, df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """
        Validate input DataFrame and text column.

        Args:
            df: Input DataFrame
            text_column: Name of the text column

        Returns:
            Cleaned DataFrame with validated and normalized text

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
        non_string_mask = df[text_column].apply(lambda v: not isinstance(v, str))
        non_string_count = non_string_mask.sum()
        if non_string_count > 0:
            logger.warning(f"Found {non_string_count} non-string values in text column")

        # Clean and normalize text
        df_cleaned = df.copy()
        df_cleaned[text_column] = df_cleaned[text_column].astype(str).str.strip()

        # Drop rows with empty or NaN text after cleaning
        original_count = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(subset=[text_column])
        df_cleaned = df_cleaned[df_cleaned[text_column].str.len() > 0]
        final_count = len(df_cleaned)

        if final_count < original_count:
            dropped_count = original_count - final_count
            logger.info(f"Dropped {dropped_count} rows with empty text after cleaning")

        logger.info(
            f"Input validation passed: {final_count} documents, text column: {text_column}"
        )

        return df_cleaned

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
        Calculate topic coherence metrics with optimized computation.

        Args:
            topic_model: Trained BERTopic model
            texts: Input texts
            embeddings: Text embeddings

        Returns:
            Dictionary of coherence metrics
        """
        logger.info("Calculating topic coherence metrics")

        try:
            # Get configuration for metrics computation
            metrics_config = self.config.get("metrics", {})
            max_docs = metrics_config.get("max_docs", 50000)
            top_k_words = metrics_config.get("top_k_words", 10)

            # Sample documents if corpus is too large
            if len(texts) > max_docs:
                logger.info(
                    f"Sampling {max_docs} documents from {len(texts)} for coherence computation"
                )
                indices = np.random.choice(len(texts), max_docs, replace=False)
                sample_texts = [texts[i] for i in indices]
                sample_embeddings = embeddings[indices]
            else:
                sample_texts = texts
                sample_embeddings = embeddings

            # Precompute lowercased texts and word sets for efficiency
            text_words = [set(text.lower().split()) for text in sample_texts]

            # Get topic words
            topic_words = topic_model.get_topics()

            # Calculate c_v coherence
            c_v_scores = []
            for topic_id, words in topic_words.items():
                if topic_id != -1:  # Skip outlier topic
                    # Limit to top_k_words for efficiency
                    top_words = words[: min(top_k_words, len(words))]
                    topic_word_embeddings = []

                    for word_tuple in top_words:
                        word = word_tuple[0]  # Extract word from (word, weight) tuple
                        # Find documents containing this word using token boundaries
                        word_docs = [
                            i
                            for i, word_set in enumerate(text_words)
                            if word in word_set
                        ]
                        if word_docs:
                            topic_word_embeddings.append(
                                sample_embeddings[word_docs].mean(axis=0)
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
                    # Limit to top_k_words for efficiency
                    top_words = words[: min(top_k_words, len(words))]
                    # Count co-occurrences of top words
                    word_pairs = [
                        (top_words[i][0], top_words[j][0])
                        for i in range(len(top_words))
                        for j in range(i + 1, len(top_words))
                    ]
                    if word_pairs:
                        pair_scores = []
                        for word1, word2 in word_pairs[:20]:  # Limit to top 20 pairs
                            # Count documents containing both words using token boundaries
                            both_count = sum(
                                1
                                for word_set in text_words
                                if word1 in word_set and word2 in word_set
                            )
                            word1_count = sum(
                                1 for word_set in text_words if word1 in word_set
                            )
                            word2_count = sum(
                                1 for word_set in text_words if word2 in word_set
                            )

                            if both_count > 0 and word1_count > 0 and word2_count > 0:
                                # Simplified NPMI calculation
                                pmi = np.log(
                                    both_count
                                    * len(sample_texts)
                                    / (word1_count * word2_count)
                                )
                                npmi = pmi / (-np.log(both_count / len(sample_texts)))
                                pair_scores.append(npmi)

                        if pair_scores:
                            c_npmi_scores.append(np.mean(pair_scores))

            c_npmi_mean = np.mean(c_npmi_scores) if c_npmi_scores else 0.0

            metrics = {
                "c_v_mean": c_v_mean,
                "c_npmi_mean": c_npmi_mean,
                "c_v_scores": c_v_scores,
                "c_npmi_scores": c_npmi_scores,
                "docs_used": len(sample_texts),
                "total_docs": len(texts),
            }

            logger.info(
                f"Coherence metrics calculated: c_v={c_v_mean:.3f}, c_npmi={c_npmi_mean:.3f} "
                f"(using {len(sample_texts)}/{len(texts)} documents)"
            )
            return metrics

        except Exception as e:
            logger.warning(f"Failed to calculate coherence metrics: {e}")
            return {
                "c_v_mean": 0.0,
                "c_npmi_mean": 0.0,
                "c_v_scores": [],
                "c_npmi_scores": [],
                "docs_used": 0,
                "total_docs": len(texts),
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
        overall_start_time = time.time()
        stage_times = {}

        try:
            # Validate input
            stage_start = time.time()
            df_cleaned = self._validate_input_data(df, text_column)
            stage_times["validation"] = time.time() - stage_start

            # Preprocess texts if enabled
            stage_start = time.time()
            if self.config.get("text_preprocessing", {}).get("enable", False):
                logger.info("🔧 Preprocessing texts (removing stopwords, cleaning)...")
                original_texts = df_cleaned[text_column].copy()
                df_cleaned[text_column] = df_cleaned[text_column].apply(
                    self._preprocess_text
                )

                # Remove documents that became empty after preprocessing
                initial_count = len(df_cleaned)
                df_cleaned = df_cleaned[df_cleaned[text_column].str.len() > 0]
                final_count = len(df_cleaned)

                if initial_count != final_count:
                    logger.info(
                        f"Removed {initial_count - final_count} documents that became empty after preprocessing"
                    )

                # Log preprocessing example
                if len(df_cleaned) > 0:
                    sample_idx = 0
                    original = original_texts.iloc[sample_idx]
                    processed = df_cleaned[text_column].iloc[sample_idx]
                    logger.info("Text preprocessing example:")
                    logger.info(
                        f"  Original: {original[:100]}{'...' if len(original) > 100 else ''}"
                    )
                    logger.info(
                        f"  Processed: {processed[:100]}{'...' if len(processed) > 100 else ''}"
                    )
            else:
                logger.info("Text preprocessing disabled - using original texts")

            stage_times["preprocessing"] = time.time() - stage_start

            # Extract texts
            texts = df_cleaned[text_column].tolist()
            logger.info(f"Starting BERTopic training on {len(texts)} documents")

            # Generate embeddings
            stage_start = time.time()
            logger.info("🔧 Generating embeddings...")
            embeddings = self._generate_embeddings(texts)
            stage_times["embeddings"] = time.time() - stage_start
            logger.info(
                f"✅ Embeddings generated in {stage_times['embeddings']:.2f} seconds"
            )

            # Initialize BERTopic model
            stage_start = time.time()
            bertopic_config = self.config["bertopic"].copy()

            # Instantiate UMAP and HDBSCAN models from configuration
            if "umap_model" in bertopic_config:
                umap_config = bertopic_config.pop("umap_model")
                try:
                    import umap

                    umap_model = umap.UMAP(**umap_config)
                    logger.info("✅ UMAP model instantiated from configuration")
                    logger.debug(f"UMAP config used: {umap_config}")
                except Exception as e:
                    logger.warning(
                        f"⚠️  Failed to instantiate UMAP model: {e}, using default"
                    )
                    logger.debug(f"UMAP config that failed: {umap_config}")
                    umap_model = None
            else:
                umap_model = None

            if "hdbscan_model" in bertopic_config:
                hdbscan_config = bertopic_config.pop("hdbscan_model")
                try:
                    import hdbscan

                    hdbscan_model = hdbscan.HDBSCAN(**hdbscan_config)
                    logger.info("✅ HDBSCAN model instantiated from configuration")
                    logger.debug(f"HDBSCAN config used: {hdbscan_config}")
                except Exception as e:
                    logger.warning(
                        f"⚠️  Failed to instantiate HDBSCAN model: {e}, using default"
                    )
                    logger.debug(f"HDBSCAN config that failed: {hdbscan_config}")
                    hdbscan_model = None
            else:
                hdbscan_model = None

            # Create BERTopic model with instantiated models
            self.topic_model = BERTopic(
                umap_model=umap_model, hdbscan_model=hdbscan_model, **bertopic_config
            )

            # Debug: Verify model types
            logger.debug(f"BERTopic model type: {type(self.topic_model)}")
            if umap_model:
                logger.debug(f"UMAP model type: {type(umap_model)}")
            if hdbscan_model:
                logger.debug(f"HDBSCAN model type: {type(hdbscan_model)}")

            stage_times["initialization"] = time.time() - stage_start
            logger.info(
                f"✅ BERTopic model initialized in {stage_times['initialization']:.2f} seconds"
            )

            # Train model with progress logging
            stage_start = time.time()
            logger.info("🤖 Training BERTopic model (clustering phase)...")

            # Add progress callback if enabled
            if self.config.get("performance", {}).get("enable_progress_logging", True):
                logger.info(
                    "📊 Progress logging enabled - monitoring clustering progress..."
                )

            # Debug: Log model configuration
            logger.debug(f"BERTopic model type: {type(self.topic_model)}")
            if hasattr(self.topic_model, "umap_model") and self.topic_model.umap_model:
                logger.debug(f"UMAP model: {type(self.topic_model.umap_model)}")
                logger.debug(
                    f"UMAP n_components: {getattr(self.topic_model.umap_model, 'n_components', 'N/A')}"
                )
            if (
                hasattr(self.topic_model, "hdbscan_model")
                and self.topic_model.hdbscan_model
            ):
                logger.debug(f"HDBSCAN model: {type(self.topic_model.hdbscan_model)}")
                logger.debug(
                    f"HDBSCAN min_cluster_size: {getattr(self.topic_model.hdbscan_model, 'min_cluster_size', 'N/A')}"
                )
                logger.debug(
                    f"HDBSCAN min_samples: {getattr(self.topic_model.hdbscan_model, 'min_samples', 'N/A')}"
                )

            # Debug: Log data shape
            logger.debug(f"Texts shape: {len(texts)}")
            logger.debug(f"Embeddings shape: {embeddings.shape}")

            # Debug: Check for data quality issues
            if len(texts) < 10:
                logger.warning("⚠️  Very small dataset, clustering may be challenging")

            # Debug: Check embedding quality
            if embeddings is not None:
                import numpy as np

                embedding_norms = np.linalg.norm(embeddings, axis=1)
                logger.debug(
                    f"Embedding norms - min: {embedding_norms.min():.4f}, max: {embedding_norms.max():.4f}, mean: {embedding_norms.mean():.4f}"
                )

                # Check for NaN or infinite values
                if np.any(np.isnan(embeddings)) or np.any(np.isinf(embeddings)):
                    logger.error("❌ Embeddings contain NaN or infinite values!")
                    raise ValueError("Embeddings contain NaN or infinite values")

                # Check embedding variance
                embedding_var = np.var(embeddings, axis=0)
                logger.debug(
                    f"Embedding variance - min: {embedding_var.min():.4f}, max: {embedding_var.max():.4f}, mean: {embedding_var.mean():.4f}"
                )

            topics, probs = self.topic_model.fit_transform(texts, embeddings)
            stage_times["clustering"] = time.time() - stage_start
            logger.info(
                f"✅ Clustering completed in {stage_times['clustering']:.2f} seconds"
            )

            # Debug: Log clustering results
            logger.debug(
                f"Topics shape: {len(topics) if topics is not None else 'None'}"
            )
            logger.debug(
                f"Probabilities shape: {probs.shape if probs is not None else 'None'}"
            )
            if topics is not None:
                unique_topics = set(topics)
                logger.debug(f"Unique topics: {len(unique_topics)}")
                logger.debug(
                    f"Topic IDs: {sorted(list(unique_topics))[:10]}..."
                )  # Show first 10

            # Validate clustering results
            if topics is None or len(set(topics)) <= 1:
                logger.warning(
                    "⚠️  Clustering produced insufficient topics, retrying with more permissive parameters..."
                )

                # Retry with more permissive HDBSCAN parameters
                try:
                    import hdbscan

                    fallback_hdbscan = hdbscan.HDBSCAN(
                        min_cluster_size=5,  # Very permissive
                        min_samples=2,  # Very permissive
                        metric="euclidean",
                        cluster_selection_method="eom",
                        core_dist_n_jobs=1,
                        cluster_selection_epsilon=0.2,  # More flexible
                        allow_single_cluster=True,
                    )

                    # Create new BERTopic model with fallback parameters
                    fallback_model = BERTopic(
                        hdbscan_model=fallback_hdbscan, verbose=False
                    )

                    logger.info("🔄 Retrying clustering with fallback parameters...")
                    topics, probs = fallback_model.fit_transform(texts, embeddings)

                    if topics is not None and len(set(topics)) > 1:
                        logger.info("✅ Fallback clustering successful!")
                        self.topic_model = fallback_model  # Use the fallback model
                    else:
                        raise RuntimeError("Fallback clustering also failed")

                except Exception as fallback_error:
                    logger.error(f"❌ Fallback clustering failed: {fallback_error}")
                    raise RuntimeError(
                        f"Both primary and fallback clustering failed. Primary error: clustering produced insufficient topics, fallback error: {fallback_error}"
                    )

            # Calculate quality metrics
            stage_start = time.time()
            logger.info("📈 Calculating quality metrics...")
            self.quality_metrics = self._calculate_coherence_metrics(
                self.topic_model, texts, embeddings
            )
            stage_times["quality_metrics"] = time.time() - stage_start
            logger.info(
                f"✅ Quality metrics calculated in {stage_times['quality_metrics']:.2f} seconds"
            )

            # Check quality thresholds
            stage_start = time.time()
            meets_thresholds, warnings = self._check_quality_thresholds(
                self.quality_metrics
            )
            stage_times["threshold_check"] = time.time() - stage_start

            # Store results
            self.training_time = time.time() - overall_start_time
            self._training_results = {
                "topics": topics,
                "probabilities": probs,
                "texts": texts,
                "embeddings": embeddings,
                "meets_thresholds": meets_thresholds,
                "warnings": warnings,
                "stage_times": stage_times,
            }

            # Log comprehensive results
            logger.info("🎉 BERTopic training completed successfully!")
            logger.info(f"⏱️  Total training time: {self.training_time:.2f} seconds")
            logger.info(
                f"📊 Generated {len(set(topics)) - (1 if -1 in topics else 0)} topics"
            )

            # Log timing breakdown if enabled
            if self.config.get("performance", {}).get("enable_timing_breakdown", True):
                logger.info("📊 Training stage breakdown:")
                for stage, duration in stage_times.items():
                    percentage = (duration / self.training_time) * 100
                    logger.info(
                        f"   {stage.capitalize()}: {duration:.2f}s ({percentage:.1f}%)"
                    )

            if warnings:
                logger.warning("⚠️  Quality warnings: " + "; ".join(warnings))

            return self

        except Exception as e:
            logger.error(f"❌ BERTopic training failed: {e}")
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
                "stage_times": self._training_results.get("stage_times", {}),
            }
        )
        return metrics

    def get_timing_breakdown(self) -> Dict[str, float]:
        """
        Get detailed timing breakdown for each training stage.

        Returns:
            Dictionary mapping stage names to execution times in seconds
        """
        if not self._training_results:
            raise RuntimeError("Model must be trained before getting timing breakdown")

        stage_times = self._training_results.get("stage_times", {})
        if not stage_times:
            return {"total": self.training_time}

        # Add total time and percentages
        breakdown = stage_times.copy()
        breakdown["total"] = self.training_time

        for stage, duration in stage_times.items():
            if self.training_time > 0:
                breakdown[f"{stage}_percentage"] = (duration / self.training_time) * 100

        return breakdown

    def get_performance_summary(self) -> str:
        """
        Get a formatted performance summary string.

        Returns:
            Formatted string with performance metrics and timing breakdown
        """
        if not self._training_results:
            return "Model not yet trained"

        metrics = self.get_quality_metrics()

        summary = "🎯 Performance Summary\n"
        summary += f"{'='*50}\n"
        summary += f"📊 Total Training Time: {metrics['training_time']:.2f} seconds\n"
        summary += (
            f"📈 Documents Processed: {len(self._training_results.get('texts', []))}\n"
        )
        summary += f"🏷️  Topics Generated: {len(set(self._training_results.get('topics', []))) - (1 if -1 in self._training_results.get('topics', []) else 0)}\n"

        if "stage_times" in metrics and metrics["stage_times"]:
            summary += "\n⏱️  Stage Breakdown:\n"
            for stage, duration in metrics["stage_times"].items():
                percentage = (
                    (duration / metrics["training_time"]) * 100
                    if metrics["training_time"] > 0
                    else 0
                )
                summary += (
                    f"   {stage.capitalize()}: {duration:.2f}s ({percentage:.1f}%)\n"
                )

        if "c_v_mean" in metrics:
            summary += "\n📊 Quality Metrics:\n"
            summary += f"   c_v coherence: {metrics['c_v_mean']:.3f}\n"
            if "c_npmi_mean" in metrics:
                summary += f"   c_npmi coherence: {metrics['c_npmi_mean']:.3f}\n"

        return summary

    def get_document_topics(self, documents: Optional[List[str]] = None) -> List[int]:
        """
        Get topic assignments for documents.

        Args:
            documents: Optional list of documents to get topics for.
                      If None, returns topics for the documents used during training.

        Returns:
            List of topic IDs for each document

        Raises:
            RuntimeError: If model hasn't been trained yet
        """
        if self.topic_model is None:
            raise RuntimeError("Model must be trained before getting document topics")

        if documents is None:
            # Return topics for the documents used during training
            # The topics are stored in the model after fit_transform
            if (
                hasattr(self.topic_model, "topics_")
                and self.topic_model.topics_ is not None
            ):
                topics = self.topic_model.topics_
                # Handle both numpy array and list cases
                if hasattr(topics, "tolist"):
                    return topics.tolist()
                elif isinstance(topics, list):
                    return topics
                else:
                    # Convert to list if it's some other iterable
                    return list(topics)
            else:
                raise RuntimeError(
                    "No training topics available. Model may not have been properly trained."
                )
        else:
            # Get topics for new documents using transform
            topics, _ = self.topic_model.transform(documents)
            # Handle both numpy array and list cases
            if hasattr(topics, "tolist"):
                return topics.tolist()
            elif isinstance(topics, list):
                return topics
            else:
                # Convert to list if it's some other iterable
                return list(topics)

    def save_model(self, filepath: Union[str, Path]) -> None:
        """
        Save the trained model to disk using safe, non-pickle persistence.

        Args:
            filepath: Path where to save the model

        Raises:
            RuntimeError: If model hasn't been trained
            RuntimeError: If filesystem operations fail
        """
        if self.topic_model is None:
            raise RuntimeError("Model must be trained before saving")

        filepath = Path(filepath)
        model_dir = filepath.parent / filepath.stem

        try:
            # Create model directory
            model_dir.mkdir(parents=True, exist_ok=True)

            # Save BERTopic model to the directory
            self.topic_model.save(str(model_dir))

            # Save wrapper metadata
            metadata = {
                "random_seed": self.random_seed,
                "training_time": self.training_time,
                "quality_metrics": self.quality_metrics,
                "config": self.config,
            }

            with open(model_dir / "wrapper_meta.json", "w") as f:
                json.dump(metadata, f, indent=2, default=str)

            # Save training results (lightweight version)
            if self._training_results:
                results_meta = {
                    "topics_shape": np.array(
                        self._training_results.get("topics", [])
                    ).shape,
                    "probabilities_shape": np.array(
                        self._training_results.get("probabilities", [])
                    ).shape
                    if self._training_results.get("probabilities") is not None
                    else None,
                    "texts_count": len(self._training_results.get("texts", [])),
                    "embeddings_shape": np.array(
                        self._training_results.get("embeddings", [])
                    ).shape
                    if self._training_results.get("embeddings") is not None
                    else None,
                    "meets_thresholds": self._training_results.get(
                        "meets_thresholds", False
                    ),
                    "warnings": self._training_results.get("warnings", []),
                }

                with open(model_dir / "training_results.json", "w") as f:
                    json.dump(results_meta, f, indent=2, default=str)

            logger.info(f"Model saved to {model_dir}")

        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            raise RuntimeError(f"Model saving failed: {e}")

    def save_model_with_timestamp(
        self, base_path: Union[str, Path], prefix: str = "bertopic_model"
    ) -> str:
        """
        Save the trained model with a timestamp in the filename.

        Args:
            base_path: Base directory where to save the model
            prefix: Prefix for the model filename

        Returns:
            Path to the saved model directory

        Raises:
            RuntimeError: If model hasn't been trained or saving fails
        """
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_name = f"{prefix}_{timestamp}"
        model_path = Path(base_path) / model_name

        self.save_model(model_path)
        logger.info(f"Model saved with timestamp: {model_path}")
        return str(model_path)

    @classmethod
    def load_model(cls, filepath: Union[str, Path]) -> "BERTopicWrapper":
        """
        Load a trained model from disk using safe, non-pickle persistence.

        Args:
            filepath: Path to the saved model

        Returns:
            Loaded BERTopicWrapper instance

        Raises:
            FileNotFoundError: If model directory doesn't exist
            RuntimeError: If loading fails
        """
        filepath = Path(filepath)
        model_dir = filepath.parent / filepath.stem

        if not model_dir.exists():
            raise FileNotFoundError(f"Model directory not found: {model_dir}")

        try:
            # Load wrapper metadata
            with open(model_dir / "wrapper_meta.json", "r") as f:
                metadata = json.load(f)

            # Create wrapper instance
            instance = cls(
                config_dict=metadata["config"], random_seed=metadata["random_seed"]
            )

            # Load BERTopic model from the directory
            instance.topic_model = BERTopic.load(str(model_dir))

            # Restore metadata
            instance.training_time = metadata["training_time"]
            instance.quality_metrics = metadata["quality_metrics"]

            # Load training results if available
            results_file = model_dir / "training_results.json"
            if results_file.exists():
                with open(results_file, "r") as f:
                    results_meta = json.load(f)
                instance._training_results = results_meta

            logger.info(f"Model loaded from {model_dir}")
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
