# Feed-Level Topic Modeling Analysis

This analysis performs BERTopic topic modeling on Bluesky feed content to understand topic distributions across experimental conditions and time periods.

## 🎯 **What This Does**

1. **Loads feed data** from production environment using shared data loading functions
2. **Trains BERTopic model** using the existing BERTopicWrapper (MET-34)
3. **Performs stratified analysis** across conditions and time periods
4. **Exports results** to CSV and JSON files
5. **Generates visualizations** for topic distributions and evolution

## 📁 **Project Structure**

The project is organized into logical folders for better maintainability:

```
calculate_feed_topic_models_2025_08_18/
├── README.md                    # This documentation
├── main.py                      # Main entry point with train/inference/full modes
├── load/                        # Data loading components
│   └── load_data.py            # DataLoader class with sampling logic
├── train/                       # Training components
│   ├── train.py                # Training script for hybrid approach
│   ├── run_training_local.sh   # Local training script for quick testing
│   └── submit_topic_modeling_training.sh  # SLURM script for training
├── inference/                   # Inference components
│   ├── inference.py            # Inference script for trained models
│   └── submit_topic_modeling_inference.sh # SLURM script for inference
└── helper/                      # Helper utilities and tools
    ├── topic_analysis_helpers.py    # Analysis helper functions
    ├── visualize_results.py         # Visualization generation
    ├── use_saved_model.py           # Model usage utilities
    ├── run_topic_modeling.py        # Alternative runner script
    ├── load_testing.py              # Performance testing
    ├── LOAD_TESTING_README.md       # Load testing documentation
    └── submit_topic_modeling_analysis.sh  # Original full analysis SLURM script
```

## 🚀 **Usage**

### **Training and Inference Workflow (Recommended for Large Datasets)**

For datasets with 1M+ documents, use the hybrid approach that separates training and inference:

#### **1. Training Phase (Run Once)**
```bash
# Train model on representative sample (500 posts per day)
python main.py --run-mode train --mode prod --sample-per-day 500

# Or use standalone training script
python train.py --mode prod --sample-per-day 500 --output-dir ./trained_models
```

#### **2. Inference Phase (Run Multiple Times)**
```bash
# Run inference on full dataset using trained model
python main.py --run-mode infer \
    --model-path ./trained_models/feed_topic_model_20250119_143022 \
    --metadata-path ./trained_models/model_metadata_feed_topic_model_20250119_143022.json \
    --mode prod

# Or use standalone inference script
python inference.py \
    --model-path ./trained_models/feed_topic_model_20250119_143022 \
    --metadata-path ./trained_models/model_metadata_feed_topic_model_20250119_143022.json \
    --mode full --data-mode prod
```

### **Local Training (Quick Testing)**

For quick local testing and development:

```bash
# Run local training with hardcoded parameters
./train/run_training_local.sh
```

This script automatically:
- Uses local mode data
- Samples 500 documents per day
- Saves to `./trained_models/local/`
- Activates the `bluesky_research` conda environment
- Provides clear success/failure feedback

### **Complete Analysis (Original Behavior)**
```bash
# Full analysis: training + inference in one run (for smaller datasets)
python main.py --run-mode full --mode prod
```

### **SLURM Job Submission**

#### **Training Job (Hybrid Approach)**
```bash
# Basic training
sbatch train/submit_topic_modeling_training.sh

# Training with custom parameters
sbatch train/submit_topic_modeling_training.sh --mode prod --sample-per-day 500 --output-dir ./my_models/prod

# Training with fallback configuration
sbatch train/submit_topic_modeling_training.sh --force-fallback
```

#### **Inference Job (Hybrid Approach)**
```bash
# Inference (requires trained model)
sbatch inference/submit_topic_modeling_inference.sh --model-path ./trained_models/prod/feed_topic_model_20250119_143022 --metadata-path ./trained_models/prod/model_metadata_feed_topic_model_20250119_143022.json

# Inference with custom output directory
sbatch inference/submit_topic_modeling_inference.sh --model-path ./trained_models/prod/feed_topic_model_20250119_143022 --metadata-path ./trained_models/prod/model_metadata_feed_topic_model_20250119_143022.json --output-dir ./my_results
```

#### **Full Analysis (Original Approach)**
```bash
sbatch helper/submit_topic_modeling_analysis.sh
```

### **Generate Visualizations**
```bash
python helper/visualize_results.py
```

### **Load Testing**
For performance testing across different dataset sizes, see the comprehensive load testing documentation:
- **📋 [Load Testing Guide](helper/LOAD_TESTING_README.md)** - Complete guide for performance testing
- **🧪 Run Load Tests**: `python helper/load_testing.py --sample-sizes 1000 10000 100000`
- **⚡ Quick Test**: `python main.py --sample-size 10000 --mode local`

## 🔧 **Command-Line Arguments**

### **Main Script (`main.py`)**
```bash
python main.py [OPTIONS]

Options:
  --run-mode {train,infer,full}    Run mode (default: full)
  --model-path PATH                Path to trained model (required for infer mode)
  --metadata-path PATH             Path to model metadata (required for infer mode)
  --sample-per-day INT             Documents per day for training (default: 500)
  --force-fallback                 Force conservative configuration
  --sample-size INT                Sample size for testing
  --mode {local,prod}              Data loading mode (default: prod)
```

### **Training Script (`train/train.py`)**
```bash
python train/train.py [OPTIONS]

Options:
  --mode {local,prod}              Data loading mode (default: prod)
  --sample-per-day INT             Documents per day for training (default: 500)
  --output-dir PATH                Directory to save trained model
  --force-fallback                 Force conservative configuration
```

### **Inference Script (`inference/inference.py`)**
```bash
python inference/inference.py [OPTIONS]

Options:
  --model-path PATH                Path to trained model directory (required)
  --metadata-path PATH             Path to model metadata JSON file (required)
  --mode {full,file}               Inference mode (default: full)
  --documents-file PATH            Path to CSV file with documents (for file mode)
  --data-mode {local,prod}         Data loading mode for full mode (default: prod)
  --output-dir PATH                Directory to save inference results
```

## 📁 **Output Files**

### **Training Output (trained_models/ directory)**
```
trained_models/
├── local/                          # Local mode training results
│   └── feed_topic_model_{timestamp}/
│       ├── bertopic_model/         # BERTopic model files
│       ├── wrapper_meta.json       # Wrapper metadata
│       └── training_results.json   # Training results
└── prod/                           # Production mode training results
    └── feed_topic_model_{timestamp}/
        ├── bertopic_model/         # BERTopic model files
        ├── wrapper_meta.json       # Wrapper metadata
        └── training_results.json   # Training results
```

### **Analysis Output (results/ directory)**
```
results/
├── local/                          # Local mode analysis results
│   └── {timestamp}/
│       ├── topics_{timestamp}.csv
│       ├── quality_metrics_{timestamp}.json
│       ├── topic_assignments_{timestamp}.csv
│       ├── stratified_topic_analysis_{timestamp}.csv
│       ├── topic_evolution_{timestamp}.csv
│       └── summary_{timestamp}.json
└── prod/                           # Production mode analysis results
    └── {timestamp}/
        ├── topics_{timestamp}.csv
        ├── quality_metrics_{timestamp}.json
        ├── topic_assignments_{timestamp}.csv
        ├── stratified_topic_analysis_{timestamp}.csv
        ├── topic_evolution_{timestamp}.csv
        └── summary_{timestamp}.json
```

### **Visualization Files**
1. **`topic_distribution_by_size.png`** - Bar chart of topic sizes
2. **`topic_evolution_over_time.png`** - Time series of topic proportions
3. **`topic_distribution_by_condition.png`** - Comparison across experimental conditions
4. **`topic_model_quality_metrics.png`** - Quality metrics visualization

## 🔧 **How It Works**

### **Training Mode (`train.py` or `--run-mode train`)**
1. **Data Loading**: Loads full dataset using shared functions
2. **Representative Sampling**: Creates stratified sample (500 posts per day by default)
3. **Model Training**: Trains BERTopic model on representative sample
4. **Model Saving**: Saves trained model and metadata for later inference

### **Inference Mode (`inference.py` or `--run-mode infer`)**
1. **Model Loading**: Loads pre-trained model and metadata
2. **Data Loading**: Loads full dataset or new documents
3. **Topic Assignment**: Assigns topics to documents using trained model
4. **Stratified Analysis**: Performs analysis across conditions and time periods
5. **Results Export**: Exports all results in standardized format

### **Full Mode (`--run-mode full` - Original Behavior)**
1. **Data Loading**: Loads users and partition dates using shared functions
2. **Model Training**: Trains BERTopic model on full dataset
3. **Topic Assignment**: Computes topic assignments for all documents
4. **Stratified Analysis**: Performs analysis across conditions and time periods
5. **Results Export**: Exports topic information, quality metrics, and analysis results

### **Key Components**
- **Shared Data Loading**: Uses proven shared functions for data access
- **BERTopicWrapper**: Leverages existing implementation
- **Stratified Analysis**: Uses shared analysis functions for consistency
- **Standardized Output**: Timestamped files with consistent naming patterns

## 🔄 **Integration with Existing System**

### **Shared Components Used:**
- `services.calculate_analytics.shared.data_loading.users.load_user_data()`
- `services.calculate_analytics.shared.data_loading.feeds.map_users_to_posts_used_in_feeds()`
- `services.calculate_analytics.shared.data_loading.posts.load_preprocessed_posts_by_uris()`
- `services.calculate_analytics.shared.constants.STUDY_START_DATE`, `STUDY_END_DATE`

### **Standard Patterns Followed:**
- **File Organization**: `main.py`, `submit_*.sh`, `visualize_results.py`, `README.md`
- **Error Handling**: Standard try/catch patterns with detailed logging
- **Output Management**: Timestamped files in `results/` directory
- **SLURM Integration**: Standard job submission script
