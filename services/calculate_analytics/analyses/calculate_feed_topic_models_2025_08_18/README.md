# Feed-Level Topic Modeling Analysis

This analysis performs BERTopic topic modeling on Bluesky feed content to understand topic distributions across experimental conditions and time periods.

## 🎯 **What This Does**

1. **Loads feed data** from production environment using shared data loading functions
2. **Trains BERTopic model** using the existing BERTopicWrapper (MET-34) with hybrid approach
3. **Performs stratified analysis** across conditions and time periods
4. **Exports results** to CSV and JSON files with structured metadata
5. **Generates visualizations** for topic distributions and evolution

## ✨ **Key Features**

- **Hybrid Training/Inference Approach**: Separate training on representative samples and inference on full datasets
- **Automatic Sampling**: Built-in stratified sampling (500 posts per day) during training
- **Model Persistence**: Structured model saving with metadata and topic exports
- **OpenAI Topic Name Generation**: Automatic generation of meaningful topic names using GPT-4o-mini
- **Compatibility Fixes**: Resolved BERTopic/SentenceTransformer interface issues
- **Local Testing**: Easy local training and inference scripts for development
- **Structured Output**: Consistent directory structure for both training and inference results
- **Performance Optimized**: Batch processing and memory-efficient data loading
- **Data Export**: Export sociopolitical posts data for efficient reuse across runs
- **Sliced UMAP Visualizations**: Create targeted visualizations by condition, date range, or combinations
- **Production-Only Features**: Export and visualization features only available in prod mode (no local labels/feed data)

## 📁 **Project Structure**

The project is organized into logical folders for better maintainability:

```
calculate_feed_topic_models_2025_08_18/
├── README.md                    # This documentation
├── main.py                      # Main entry point with train/inference/full modes
├── load/                        # Data loading components
│   └── load_data.py            # DataLoader class with sampling logic and export functionality
├── train/                       # Training components
│   ├── train.py                # Training script for hybrid approach
│   ├── run_training_local.sh   # Local training script for quick testing
│   └── submit_topic_modeling_training.sh  # SLURM script for training
├── inference/                   # Inference components
│   ├── inference.py            # Inference script for trained models
│   └── submit_topic_modeling_inference.sh # SLURM script for inference
├── visualization/               # Visualization components (prod mode only)
│   ├── create_umap_visualization.py      # Original UMAP visualization
│   └── sliced_umap_visualization.py     # Sliced UMAP visualization for exported data
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

There are two distinct workflows depending on your data environment:

## 🏠 **Local Workflow** (Development & Testing)

For local development and testing with sample data:

### **Local Training**
```bash
# Quick local training with hardcoded parameters
./train/run_training_local.sh

# Or manual local training
python main.py --run-mode train --mode local --sample-per-day 500
```

### **Local Inference**
```bash
# Quick local inference with automatic model detection
python inference/run_inference_local.py

# Or manual local inference
python main.py --run-mode infer \
    --model-path ./train/trained_models/local/20250119_143022/model \
    --metadata-path ./train/trained_models/local/20250119_143022/metadata/model_metadata.json \
    --mode local
```

### **Local Visualizations**
```bash
# Generate traditional visualizations
python helper/visualize_results.py
```

**Note**: Local mode does not have access to sociopolitical labels or feed data, so export functionality and sliced UMAP visualizations are not available.

---

## 🏭 **Production Workflow** (Full Analysis)

For production datasets with sociopolitical labels and feed data:

### **1. Export Sociopolitical Posts Data (Run Once)**
```bash
# Export sociopolitical posts data for efficient reuse
python -c "
from load.load_data import DataLoader
dataloader = DataLoader(mode='prod', run_mode='inference')
export_path = dataloader.export_sociopolitical_posts_data(
    include_by_date_condition=True
)
print(f'Data exported to: {export_path}')
"
```

This creates a timestamped directory with:
- `documents_df.parquet`: Deduplicated documents for training/inference
- `uri_doc_map.parquet`: URI to doc_id mapping for reconstruction
- `date_condition_uris_map.json`: Slicing structure for analysis
- `metadata.json`: Study configuration and export info
- `by_date_condition/`: Organized by date/condition folders

### **2. Training Phase (Run Once)**
```bash
# Train model on representative sample (500 posts per day)
python main.py --run-mode train --mode prod --sample-per-day 500

# Or use standalone training script
python train/train.py --mode prod --sample-per-day 500 --output-dir ./trained_models
```

### **3. Inference Phase (Run Multiple Times)**

**Option A: Use Exported Data (Recommended - Faster)**
```bash
# Run inference on exported data for efficiency
python -c "
from inference.inference import run_inference_on_exported_data
doc_topic_assignments, bertopic = run_inference_on_exported_data(
    model_path='./train/trained_models/prod/20250119_143022/model',
    metadata_path='./train/trained_models/prod/20250119_143022/metadata/model_metadata.json',
    exported_data_path='./sociopolitical_posts_used_in_feeds/20250119_143022'
)
print('Inference completed successfully!')
"
```

**Option B: Load Data Fresh (Legacy)**
```bash
# Run inference on full dataset using trained model
python main.py --run-mode infer \
    --model-path ./train/trained_models/prod/20250119_143022/model \
    --metadata-path ./train/trained_models/prod/20250119_143022/metadata/model_metadata.json \
    --mode prod

# Or use standalone inference script
python inference/inference.py \
    --model-path ./train/trained_models/prod/20250119_143022/model \
    --metadata-path ./train/trained_models/prod/20250119_143022/metadata/model_metadata.json \
    --mode full --data-mode prod
```

### **4. Sliced UMAP Visualizations (Production Only)**
```bash
# Create visualizations for specific conditions and date ranges
python -c "
from visualization.sliced_umap_visualization import SlicedUMAPVisualizer
visualizer = SlicedUMAPVisualizer(
    model_path='./train/trained_models/prod/20250119_143022/model',
    metadata_path='./train/trained_models/prod/20250119_143022/metadata/model_metadata.json',
    exported_data_path='./sociopolitical_posts_used_in_feeds/20250119_143022'
)
visualizer.load_exported_data()
visualizer.load_model_and_assignments()

# Create visualization for control condition, specific week
fig, ax = visualizer.create_sliced_visualization(
    condition='control',
    date_range=['2024-01-15', '2024-01-21'],
    output_path='./visualizations/control_week1_umap.png'
)
"
```

---

## 🔄 **Legacy Workflows**

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
sbatch inference/submit_topic_modeling_inference.sh --model-path ./train/trained_models/prod/20250119_143022/model --metadata-path ./train/trained_models/prod/20250119_143022/metadata/model_metadata.json

# Inference with custom output directory
sbatch inference/submit_topic_modeling_inference.sh --model-path ./train/trained_models/prod/20250119_143022/model --metadata-path ./train/trained_models/prod/20250119_143022/metadata/model_metadata.json --output-dir ./my_results
```

#### **Full Analysis (Original Approach)**
```bash
sbatch helper/submit_topic_modeling_analysis.sh
```

### **Generate Visualizations**

#### **Traditional Visualizations**
```bash
python helper/visualize_results.py
```

#### **Sliced UMAP Visualizations (Production Only)**
```bash
# Create multiple slice visualizations
python -c "
from visualization.sliced_umap_visualization import SlicedUMAPVisualizer
visualizer = SlicedUMAPVisualizer(
    model_path='./train/trained_models/prod/20250119_143022/model',
    metadata_path='./train/trained_models/prod/20250119_143022/metadata/model_metadata.json',
    exported_data_path='./sociopolitical_posts_used_in_feeds/20250119_143022'
)
visualizer.load_exported_data()
visualizer.load_model_and_assignments()

# Define visualization slices
slice_configs = [
    {'condition': 'control', 'date_range': None, 'title_suffix': 'Control Only'},
    {'condition': 'treatment', 'date_range': None, 'title_suffix': 'Treatment Only'},
    {'condition': None, 'date_range': ['2024-01-15', '2024-01-21'], 'title_suffix': 'Week 1'},
    {'condition': 'control', 'date_range': ['2024-01-15', '2024-01-21'], 'title_suffix': 'Control Week 1'},
]

# Create all visualizations
visualizer.create_multiple_slice_visualizations(
    slice_configs=slice_configs,
    output_dir='./visualizations/slices'
)
"
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

### **Training Output (train/trained_models/ directory)**
```
train/trained_models/
├── local/                          # Local mode training results
│   └── {timestamp}/                # Timestamped model directory
│       ├── metadata/
│       │   └── model_metadata.json # Training metadata (samples, topics, coherence, etc.)
│       ├── model/                  # BERTopic model directory
│       │   ├── bertopic_model      # Main BERTopic model file
│       │   ├── wrapper_meta.json   # BERTopicWrapper metadata
│       │   └── training_results.json # Training results
│       ├── topics.csv              # Topic information export (with generated names)
│       └── topic_names.csv         # OpenAI-generated topic names
└── prod/                           # Production mode training results
    └── {timestamp}/                # Timestamped model directory
        ├── metadata/
        │   └── model_metadata.json # Training metadata
        ├── model/                  # BERTopic model directory
        │   ├── bertopic_model      # Main BERTopic model file
        │   ├── wrapper_meta.json   # BERTopicWrapper metadata
        │   └── training_results.json # Training results
        ├── topics.csv              # Topic information export (with generated names)
        └── topic_names.csv         # OpenAI-generated topic names
```

### **Inference Output (results/ directory)**
```
results/
├── local/                          # Local mode inference results
│   └── {timestamp}/
│       ├── metadata/
│       │   └── inference_metadata.json # Inference metadata (samples, time, topics, etc.)
│       ├── topic_distribution.csv      # Document count per topic
│       ├── topics_{timestamp}.csv      # Topic information (if stratified analysis)
│       ├── quality_metrics_{timestamp}.json # Quality metrics (if stratified analysis)
│       ├── topic_assignments_{timestamp}.csv # Document-topic assignments (if stratified analysis)
│       ├── stratified_topic_analysis_{timestamp}.csv # Stratified analysis results (if available)
│       └── topic_evolution_{timestamp}.csv # Topic evolution over time (if available)
└── prod/                           # Production mode inference results
    └── {timestamp}/
        ├── metadata/
        │   └── inference_metadata.json # Inference metadata
        ├── topic_distribution.csv      # Document count per topic
        ├── topics_{timestamp}.csv      # Topic information (if stratified analysis)
        ├── quality_metrics_{timestamp}.json # Quality metrics (if stratified analysis)
        ├── topic_assignments_{timestamp}.csv # Document-topic assignments (if stratified analysis)
        ├── stratified_topic_analysis_{timestamp}.csv # Stratified analysis results (if available)
        └── topic_evolution_{timestamp}.csv # Topic evolution over time (if available)
```

### **Visualization Files**
1. **`topic_distribution_by_size.png`** - Bar chart of topic sizes
2. **`topic_evolution_over_time.png`** - Time series of topic proportions
3. **`topic_distribution_by_condition.png`** - Comparison across experimental conditions
4. **`topic_model_quality_metrics.png`** - Quality metrics visualization

## 🔧 **How It Works**

### **Training Mode (`train/train.py` or `--run-mode train`)**
1. **Data Loading**: Loads full dataset using shared functions with built-in sampling
2. **Representative Sampling**: Automatically samples 500 posts per day during data loading
3. **Model Training**: Trains BERTopic model on representative sample using BERTopicWrapper
4. **OpenAI Topic Name Generation**: Generates meaningful topic names using GPT-4o-mini
5. **Model Saving**: Saves trained model in structured format:
   - `train/trained_models/{mode}/{timestamp}/model/` - BERTopic model files
   - `train/trained_models/{mode}/{timestamp}/metadata/model_metadata.json` - Training metadata
   - `train/trained_models/{mode}/{timestamp}/topics.csv` - Topic information export (with generated names)
   - `train/trained_models/{mode}/{timestamp}/topic_names.csv` - OpenAI-generated topic names

### **Inference Mode (`inference/inference.py` or `--run-mode infer`)**
1. **Model Loading**: Loads pre-trained BERTopic model with embedding adapter for compatibility
2. **Data Loading**: Loads full dataset using DataLoader with inference mode (no sampling)
3. **Topic Assignment**: Assigns topics to documents using trained model with batch processing
4. **Stratified Analysis**: Performs analysis across conditions and time periods (if metadata available)
5. **Results Export**: Exports results in structured format:
   - `results/{mode}/{timestamp}/metadata/inference_metadata.json` - Inference metadata
   - `results/{mode}/{timestamp}/topic_distribution.csv` - Document count per topic
   - Additional analysis files if stratified analysis is performed

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

## 🤖 **OpenAI Integration**

The system includes automatic topic name generation using OpenAI's GPT-4o-mini model:

### **Features:**
- **Automatic Generation**: Topic names are generated during training using the top keywords from each topic
- **Meaningful Categories**: Generates descriptive names like "Sports and Athletics", "Left-leaning Politics", "Pet Humor and Cuteness"
- **Fallback Handling**: Falls back to generic names (e.g., "Topic 0") if OpenAI API is unavailable
- **Cost Efficient**: Uses GPT-4o-mini for cost-effective generation
- **Configurable**: Can specify different OpenAI models via the BERTopicWrapper method

### **Implementation:**
- **BERTopicWrapper Method**: `generate_topic_names()` method integrated into the BERTopicWrapper class
- **Generic OpenAI Module**: `ml_tooling/llm/openai.py` provides the `run_query()` function for general OpenAI API usage
- **API Key Management**: Uses `OPENAI_API_KEY` from `lib/helper.py`
- **Error Handling**: Graceful fallback to generic names if API calls fail

### **Output Files:**
- **`topic_names.csv`**: Contains `topic_id` and `generated_name` columns
- **Updated `topics.csv`**: Includes both original and generated topic names

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
