# Bluesky Research Infrastructure

A comprehensive data platform and research infrastructure for analyzing social media content from Bluesky. This project provides end-to-end capabilities for real-time data ingestion, ML-powered content analysis, personalized feed generation, and research tooling for social media researchers and data scientists.

## Project Overview

The Bluesky Research Infrastructure is designed to support large-scale social media research by providing:

- **Real-time Data Pipeline**: Handles ~8.1M events/day from Bluesky's firehose stream
- **ML-Powered Analysis**: Multiple classifiers for content toxicity, political orientation, and sentiment analysis
- **Research Tools**: Web interface for exploring and exporting post data with advanced filtering
- **Feed Generation**: Personalized content recommendation algorithms
- **Production API**: FastAPI service for powering custom Bluesky feeds

### Key Stakeholders

- **Social Media Researchers**: Primary users analyzing social media trends and patterns
- **Data Scientists**: Users performing large-scale content analysis and ML experiments
- **Feed Developers**: Teams building custom Bluesky feed algorithms
- **Academic Institutions**: Northwestern University and research collaborators

## Architecture Overview

The system is built as a modular, scalable data platform with the following core components:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Bluesky       │───▶│  Data Pipeline   │───▶│  Research       │
│   Firehose      │    │  & Processing    │    │  Interface      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  Feed Generation │
                       │  & API Services  │
                       └──────────────────┘
```

### Data Flow

1. **Ingestion**: Real-time stream processing from Bluesky's firehose
2. **Processing**: ML inference, content classification, and data enrichment
3. **Storage**: Parquet-based data lake with efficient querying capabilities
4. **Analysis**: Research interface and API for data exploration
5. **Generation**: Personalized feed algorithms and recommendation systems

## Core Components

### 🔄 Orchestration Pipeline (`orchestration/`)

**Purpose**: Workflow orchestration using Prefect and SLURM for HPC clusters

The orchestration layer manages six main pipelines:

1. **Sync Pipeline** - Real-time data ingestion from Bluesky firehose
2. **Integrations Sync** - Popular posts from external feeds (every 2 hours)
3. **Data Pipeline** - Preprocessing and ML inference (every 2 hours)
4. **Recommendation Pipeline** - Personalized feed generation (every 4 hours)
5. **Compaction Pipeline** - Data optimization and snapshots (twice daily)
6. **Analytics Pipeline** - User activity analysis (daily)

**Key Features**:
- SLURM integration for HPC cluster execution
- Structured logging and error handling
- Email notifications for job failures
- Parallel processing with dependency management

### 🏗️ Pipeline Components (`pipelines/`)

**Purpose**: Individual pipeline stages that can be orchestrated together

**Core Processing Pipelines**:
- `sync_post_records/` - Firehose and popular posts ingestion
- `preprocess_raw_data/` - Data cleaning, spam filtering, bot detection
- `classify_records/` - ML inference for toxicity, politics, sentiment
- `consolidate_enrichment_integrations/` - Merge all ML results
- `rank_score_feeds/` - Generate personalized recommendations

**Utility Pipelines**:
- `add_users_to_study/` - Research participant management
- `generate_vector_embeddings/` - Text embeddings for similarity search
- `backfill_records_coordination/` - Historical data processing

### ⚙️ Microservices (`services/`)

**Purpose**: Containerized services that perform the actual data processing work

**Core Services**:
- `sync/stream/` - Bluesky firehose stream connector
- `sync/jetstream/` - Data persistence from stream
- `preprocess_raw_data/` - Content filtering and classification
- `ml_inference/` - Machine learning model inference
- `rank_score_feeds/` - Feed ranking algorithms
- `compact_all_services/` - Data compaction and optimization

### 🤖 ML Tooling (`ml_tooling/`)

**Purpose**: Machine learning models and inference utilities

**Key Components**:
- `perspective_api/` - Google Perspective API for toxicity detection
- `llm/` - Large language model integrations (OpenAI, Google AI)
- `valence_classifier/` - Sentiment and emotional valence analysis
- `ime/` - Individualized Moral Equivalence scoring

### 🌐 Feed API (`feed_api/`)

**Purpose**: Production FastAPI service for serving custom Bluesky feeds

**Features**:
- Integration with Bluesky's feed generation protocol
- Real-time feed serving with caching
- Currently deployed on EC2 with CloudWatch logging
- Designed for extension with custom algorithms

### 🔍 Research Interface (`bluesky_database/frontend/`)

**Purpose**: Next.js web application for exploring and analyzing post data

**Features**:
- Advanced search with text, hashtag, and username filtering
- Date range filtering and exact match toggles
- CSV export for research analysis
- Coming soon: ML-powered filters (Political, Outrage, Toxicity)
- Responsive design with accessibility compliance

**Tech Stack**:
- Next.js 14 with TypeScript
- Tailwind CSS for styling
- React Hook Form for validation
- Deployed on Vercel

### 📚 Shared Libraries (`lib/`)

**Purpose**: Common utilities, database connections, and shared functionality

**Key Modules**:
- `db/` - Database utilities and connections
- `aws/` - AWS service integrations
- `telemetry/` - Observability and monitoring
- `constants.py` - Project-wide constants
- `helper.py` - Common utility functions

## Technology Stack

### Backend Infrastructure
- **Python 3.12** - Primary programming language
- **Prefect 2.19.6** - Workflow orchestration
- **SLURM** - HPC cluster job scheduling
- **Docker** - Service containerization
- **Hetzner** - Cloud infrastructure hosting

### Data Storage & Processing
- **Parquet** - Columnar data storage format
- **DuckDB** - Analytical query engine
- **Redis** - Real-time data buffering
- **SQLite** - Local database for Prefect logs
- **Pandas & PyArrow** - Data manipulation

### Machine Learning
- **Transformers 4.44.2** - HuggingFace model library
- **PyTorch 2.4.0** - Deep learning framework
- **OpenAI API** - Large language model inference
- **Google Perspective API** - Content moderation
- **scikit-learn 1.5.1** - Traditional ML algorithms

### Web & API
- **FastAPI 0.111.0** - API framework
- **Next.js 14** - React frontend framework
- **TypeScript** - Frontend type safety
- **Tailwind CSS v3** - UI styling

### Development & CI/CD
- **GitHub Actions** - Continuous integration
- **Ruff 0.7.0** - Python linting and formatting
- **pytest** - Testing framework
- **pre-commit** - Git hooks for code quality

## Project Setup

### Prerequisites

- **Python 3.10+** (3.10, 3.11, 3.12 supported)
- **uv** (preferred) or **conda** for package management
- **Node.js 18+** (for frontend development)
- **Docker** (for service deployment)
- **Git** with pre-commit hooks

### Quick Setup (Recommended)

Use our automated setup script for the fastest installation:

```bash
# Clone the repository
git clone https://github.com/METResearchGroup/bluesky-research.git
cd bluesky-research

# Quick setup with uv and Python 3.10 (default)
./scripts/setup_environment.sh

# Or with custom options
./scripts/setup_environment.sh --python 3.12 --env-name my-bluesky-env

# Or use conda instead of uv
./scripts/setup_environment.sh --conda --python 3.11
```

The setup script will:
- ✅ Install/validate package manager (uv or conda)
- ✅ Create virtual environment with specified Python version
- ✅ Install all project dependencies (core, dev, ML tooling)
- ✅ Set up pre-commit hooks
- ✅ Install project in editable mode
- ✅ Run validation tests to ensure everything works

**Setup Script Options:**
- `-p, --python VERSION` - Python version (3.10, 3.11, 3.12)
- `-c, --conda` - Use conda instead of uv
- `-e, --env-name NAME` - Custom environment name
- `-h, --help` - Show detailed help

### Manual Installation with uv

If you prefer manual setup:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/METResearchGroup/bluesky-research.git
   cd bluesky-research
   ```

2. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Create and activate virtual environment**:
   ```bash
   uv venv --python 3.10  # or 3.11, 3.12
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

4. **Install project dependencies**:
   ```bash
   # Core dependencies
   uv pip install -r requirements.txt
   
   # Development dependencies
   uv pip install -r dev_requirements.txt
   
   # ML tooling dependencies
   uv pip install -r ml_tooling/requirements.txt
   
   # Install project in editable mode
   uv pip install -e .
   ```

5. **Set up pre-commit hooks**:
   ```bash
   pre-commit install
   ```

### Manual Installation with Conda

1. **Create conda environment**:
   ```bash
   conda create -n bluesky-research python=3.10  # or 3.11, 3.12
   conda activate bluesky-research
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install -r dev_requirements.txt
   pip install -r ml_tooling/requirements.txt
   pip install -e .
   ```

### Frontend Setup

For the research interface frontend:

```bash
cd bluesky_database/frontend
npm install
npm run dev  # Development server at http://localhost:3000
```

### Environment Configuration

1. **Copy environment template**:
   ```bash
   cp .env.example .env
   ```

2. **Configure required environment variables**:
   - `GOOGLE_API_KEY` - For Perspective API
   - `OPENAI_API_KEY` - For LLM inference
   - `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` - For S3 storage
   - `MONGODB_URI` - For document storage (if used)

### Dependency Management

The project uses a consolidated requirements system:

- **`requirements.in`** - Core project dependencies with standardized versions
- **`dev_requirements.in`** - Development and testing dependencies
- **`ml_tooling/requirements.in`** - Machine learning and AI dependencies
- **Individual `requirements.in`** files in each module for specific dependencies

All package versions are standardized across the project to ensure consistency. The setup script automatically handles all dependency installation and ensures compatibility.

## Development Workflow

### Running Tests

The project uses pytest with comprehensive test coverage:

```bash
# Run all tests
pytest

# Run specific test modules
pytest lib/tests
pytest ml_tooling/tests
pytest services/*/tests

# Run with coverage
pytest --cov=lib --cov=ml_tooling
```

### Code Quality

All code must pass linting and formatting checks:

```bash
# Run linter (will fix auto-fixable issues)
ruff check .

# Format code
ruff format .

# Pre-commit hooks (runs automatically on commit)
pre-commit run --all-files
```

### Local Development

1. **Start core services** (Redis, database):
   ```bash
   docker-compose up -d redis
   ```

2. **Run individual pipeline components**:
   ```bash
   cd pipelines/preprocess_raw_data
   python handler.py
   ```

3. **Start frontend development server**:
   ```bash
   cd bluesky_database/frontend
   npm run dev
   ```

4. **Test API endpoints**:
   ```bash
   cd feed_api
   python app.py
   ```

### Orchestration Development

For HPC/SLURM development:

1. **Test individual pipelines locally**:
   ```bash
   cd orchestration
   python sync_pipeline.py --local
   ```

2. **Deploy to HPC cluster**:
   ```bash
   ./submit_sync_pipeline_job.sh
   ```

3. **Monitor job status**:
   ```bash
   squeue -u $USER
   ```

## Deployment

### Production Deployment

The system is deployed across multiple environments:

1. **HPC Cluster (Quest/SLURM)**:
   - Main data processing pipelines
   - Orchestration and scheduling
   - ML model inference

2. **Hetzner Cloud**:
   - Storage infrastructure
   - Redis buffering services
   - Docker container hosting

3. **AWS EC2**:
   - Feed API production service
   - CloudWatch logging and monitoring

4. **Vercel**:
   - Research interface frontend
   - Static asset serving

### Docker Deployment

Individual services can be deployed via Docker:

```bash
# Build service container
docker build -f Dockerfiles/preprocess_raw_data.Dockerfile -t preprocess_raw_data .

# Run with orchestration
docker-compose up -d
```

### CI/CD Pipeline

GitHub Actions automatically:

1. **Run Tests**: Python tests, TypeScript compilation, linting
2. **Build Containers**: Docker images for all services
3. **Lint Dockerfiles**: Hadolint validation
4. **Deploy Frontend**: Automatic Vercel deployment on merge

The CI pipeline is defined in `.github/workflows/python-ci.yml` and supports Python 3.10, 3.11, and 3.12.

## Usage Examples

### Research Data Analysis

```python
# Query posts from the research database
from lib.db import get_posts_by_criteria

posts = get_posts_by_criteria(
    text_query="climate change",
    date_range=("2024-01-01", "2024-12-31"),
    classification_filters={"toxicity": "low"}
)

# Export to CSV for analysis
posts.to_csv("climate_posts_2024.csv")
```

### Custom Feed Development

```python
# Create a custom ranking algorithm
from services.rank_score_feeds import BaseFeedRanker

class CustomRanker(BaseFeedRanker):
    def score_post(self, post):
        # Custom scoring logic
        return post.engagement_score * post.recency_factor
```

### ML Model Integration

```python
# Add a new content classifier
from ml_tooling.inference_helpers import BaseClassifier

class CustomClassifier(BaseClassifier):
    def classify(self, text):
        # Your ML model inference
        return {"category": "news", "confidence": 0.85}
```

## API Documentation

### Feed API Endpoints

- `GET /feed/{feed_name}` - Get personalized feed for user
- `GET /health` - Service health check
- `POST /webhook` - Bluesky feed updates

### Research API Endpoints (Planned)

- `GET /api/posts/search` - Search posts with filters
- `GET /api/posts/export` - Export filtered posts as CSV
- `GET /api/analytics/trends` - Get trending topics and hashtags

## Monitoring & Observability

### Logging

- **Structured logs** in `/projects/p32375/bluesky-research/lib/log/`
- **CloudWatch integration** for production services
- **Email alerts** for job failures on HPC cluster

### Metrics

- **Data throughput**: ~8.1M events/day processing capacity
- **Query performance**: Sub-30 second response for 1-day queries
- **Uptime**: 99.9% availability target for API services

### Health Checks

All services expose health check endpoints for monitoring:

```bash
curl http://localhost:8000/health
```

## Research Applications

This infrastructure supports various research applications:

### Social Media Content Analysis
- Large-scale sentiment analysis across political topics
- Misinformation and toxicity pattern detection
- Community formation and interaction studies

### Algorithm Fairness Research
- Feed recommendation bias analysis
- Content diversity measurement
- User experience impact studies

### Behavioral Science
- User engagement pattern analysis
- Information consumption habits
- Social influence network effects

## Contributing

### Getting Started

1. **Fork the repository** and create a feature branch
2. **Set up development environment** following the setup instructions
3. **Make your changes** following the coding standards
4. **Write tests** for new functionality
5. **Submit a pull request** with a clear description

### Coding Standards

- **Python**: Follow PEP 8, use type hints, write docstrings
- **TypeScript**: Use strict mode, follow React best practices
- **Git**: Use conventional commit messages
- **Testing**: Maintain >90% code coverage for new features

### Project Structure Guidelines

- **`lib/`** - Shared utilities and common functionality
- **`services/`** - Independent microservices
- **`pipelines/`** - Orchestratable pipeline components
- **`orchestration/`** - Workflow definitions
- **`ml_tooling/`** - Machine learning models and utilities

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support & Community

### Getting Help

- **GitHub Issues**: Report bugs and request features
- **Discussions**: Ask questions and share ideas
- **Documentation**: Comprehensive guides in each component directory

### Academic Use

If you use this infrastructure in academic research, please cite:

```bibtex
@software{bluesky_research_infrastructure,
  title={Bluesky Research Infrastructure},
  author={Torres, Mark and Northwestern University MET Research Group},
  year={2024},
  url={https://github.com/METResearchGroup/bluesky-research}
}
```

### Contact

- **Primary Maintainer**: Mark Torres (markptorres1@gmail.com)
- **Institution**: Northwestern University MET Research Group
- **Project Page**: [GitHub Repository](https://github.com/METResearchGroup/bluesky-research)
