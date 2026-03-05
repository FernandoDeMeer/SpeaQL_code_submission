# DB Knowledge Graph Explorer (DB_KG_EXP)

An interactive knowledge exploration tool that enables intuitive navigation and extraction of meaningful insights from relational databases through knowledge graph visualization and LLM-powered analysis.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/streamlit-dashboard-red.svg)](https://streamlit.io/)
[![SQLAlchemy](https://img.shields.io/badge/database-SQLAlchemy-green.svg)](https://www.sqlalchemy.org/)

## Overview

DB_KG_EXP combines GUI interfaces, relational knowledge graph visualization, and LLM-powered chatbots to transform database exploration from complex SQL queries into an intuitive, visual experience. The tool automatically builds knowledge graphs from relational database structures and enables natural language querying of your data.

## Quick Start

### Prerequisites
- Python 3.12 or higher
- SQLite database (lightweight, no installation required)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd db_kg_exp
   ```

2. **Setup environment (recommended):**
   ```bash
   chmod +x runme.sh
   ./runme.sh
   ```

   Or manually:
   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

3. **Run the Streamlit application:**
   ```bash
   streamlit run streamlit/sca_streamlit_app.py
   ```

### Reproducing the paper experiments

Follow the steps below from the project root (`db_kg_exp/`).

#### 1) Prepare environment

```bash
chmod +x runme.sh
./runme.sh
source .venv/bin/activate
```

Set your LLM key (required by `run_experiment.py`):

```bash
export OPENAI_API_KEY="<your-api-key>"
```

#### 2) Download SEC dataset

Download:

```text
https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/2024q1_notes.zip
```

Place the archive in `data/` and extract it so files like `sub.tsv`, `num.tsv`, and `notes-metadata.json` are under `data/2024q1_notes/`.

#### 3) Build the SQLite database

```bash
python scripts/experiment/build_db_from_source_files.py
```

Expected output database:

`data/2024q1_notes/2024q1_notes.sqlite`

#### 4) Generate SQL ground truth file

```bash
python scripts/experiment/run_sql_queries.py
```

Expected output file:

`data/queries/ground_truth.jsonl`

#### 5) Run the LLM experiment

```bash
python scripts/experiment/run_experiment.py
```

Outputs are written to:

- `data/logs/experiment_<timestamp>.jsonl`
- `data/logs/results_<timestamp>.jsonl`

Optional debug runs:

```bash
# Run a single question
python scripts/experiment/run_experiment.py --question-ids q039

# Run a single question with a specific model
python scripts/experiment/run_experiment.py --question-ids q039 --model gpt-4.1

# Run multiple specific questions
python scripts/experiment/run_experiment.py --question-ids q001,q002,q003
```

If you prefer temporary in-code debugging, uncomment the example override lines in `scripts/experiment/run_experiment.py` under `main()` and re-comment them afterward.

#### 6) Evaluate results

```bash
python scripts/experiment/evaluation.py
```

Before running evaluation, update `APP_PATH` in `scripts/experiment/evaluation.py` to point to the generated results file (for example, `data/logs/results_<timestamp>.jsonl`).

### Testing with Sample Data

The project includes a Netflix dataset for testing:

- **Sample Database:** `tests/netflixdb-mysql.zip` contains a sample database based on the Netflix Engagement Report
- **Database Format:** SQLite database for easy setup and testing
- **Source:** [Netflix DB GitHub Repository](https://github.com/lerocha/netflixdb)

⚠️ **Important:** Check `tests/README_tests.md` for database setup instructions before running the code.

## Key Features

### 🔍 **Intelligent Record Selection**
- Search and select initial database records using keyword/attribute value filters
- Intuitive interface for exploring large tables

### 🕸️ **Automated Knowledge Graph Generation**
- Automatically builds relationship graphs using foreign key constraints
- Interactive visualization of entity relationships and attributes
- Real-time graph updates based on user selections

### 🤖 **LLM-Powered Analysis**
- Natural language querying of your database through chat interface
- Context-aware responses based on knowledge graph data
- Two-step processing: node extraction and intelligent summarization

### 🔌 **Database Agnostic**
- Built on SQLAlchemy for broad database compatibility
- Plug-and-play support for various relational databases
- Easy configuration for custom database connections

### 📊 **Interactive Dashboard**
- Modern Streamlit-based user interface
- Real-time graph visualization with vis.js
- Export capabilities for analysis results (JSON/text)

## Project Architecture

```
db_kg_exp/
├── dbkgexp/                    # Core application modules
│   ├── rdb_explorer.py         # Recursive foreign key graph builder
│   ├── rel_node.py             # Data model for graph nodes
│   ├── llm_handler.py          # LLM integration and processing
├── streamlit/                  # Web dashboard interface
│   ├── sca_streamlit_app.py    # Main Streamlit application
│   ├── streamlit_llm_handler.py # LLM chat interface
│   └── sca_gui_db_utils.py     # Database utilities for GUI
├── tests/                      # Test suite and sample data
│   ├── test_db.py              # Database connection tests
│   ├── llm_test.py             # LLM functionality tests
│   ├── netflixdb-mysql/        # Sample Netflix dataset
│   └── README_tests.md         # Testing documentation
├── runme.sh                    # Quick setup script
├── setup.cfg                   # Package dependencies
├── pyproject.toml              # Modern Python project configuration
└── logging.yaml                # Logging configuration
```

### Core Components

#### **Database Layer**
- **`rdb_explorer.py`**: Heart of the system - recursively builds knowledge graphs from database foreign key relationships using depth-first traversal
- **`rel_node.py`**: Defines the data structure for graph nodes, relationships, and foreign key constraints
- **`sca_gui_db_utils.py`**: Database abstraction layer with SQLAlchemy integration for multiple database backends

#### **LLM Integration Layer**
- **`llm_handler.py`**: Manages LLM integration using DSPy framework for structured outputs and authenticity guardrails
- **`streamlit_llm_handler.py`**: Streamlit-specific LLM interface with chat functionality and session management

#### **Presentation Layer**
- **Graph Visualization**: Real-time interactive graphs using vis.js with dynamic node highlighting
- **Chat Interface**: Conversational AI interface for natural language database querying

### Data Flow Architecture

1. **Initialization**: User selects initial database record through search interface
2. **Graph Building**: `RDBExplorer` recursively builds knowledge graph using foreign key relationships
3. **Visualization**: Interactive graph rendered using vis.js with real-time updates
4. **LLM Processing**: User queries processed through DSPy pipeline with node extraction and summarization
5. **Authenticity Guardrail (Validation)**: LLM outputs validated against actual database contents to prevent hallucinations
6. **Response Formatting**: Results formatted and displayed with validated/non-matching node highlighting

## LLM Integration

Enhance your database exploration with AI-powered natural language querying capabilities.

### How It Works

1. **🔍 Node Extraction**: The LLM analyzes the knowledge graph to identify nodes and attributes relevant to your queries
2. **📝 Intelligent Summarization**: Creates comprehensive, context-aware summaries based on extracted data
3. **💬 Interactive Chat**: Real-time conversation interface within the exploration dashboard
4. **📤 Export Options**: Download analysis results as JSON or structured text summaries

### Capabilities

- **Context-Aware Responses**: Answers are generated based only on the current knowledge graph data
- **Graph Highlighting**: Relevant nodes are automatically highlighted during LLM interactions
- **Multi-Modal Output**: Support for both textual summaries and structured data exports
- **Query Memory**: Maintains conversation context for follow-up questions

### Example Use Case

```
User: "Show me all movies released in 2023 with high ratings"
AI: Analyzes the knowledge graph, identifies relevant movie nodes,
    and provides detailed information about 2023 releases with ratings data.

```

### Configuration

The LLM integration supports various providers and can be configured through environment variables or the configuration module. See the setup documentation for API key configuration and model selection options.

## Development & Debugging

### Development Setup

For development work, install with development dependencies:

```bash
pip install -e ".[dev]"
```

This includes additional tools like:
- `pytest` for testing
- `black` for code formatting
- `fastapi` and `uvicorn` for API development
- Enhanced visualization libraries

### Debugging the Streamlit Application

To debug the Streamlit dashboard in VS Code, add this configuration to your `.vscode/launch.json`:

```json
{
    "name": "debug streamlit",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/.venv/bin/streamlit",
    "python": "${workspaceFolder}/.venv/bin/python",
    "console": "integratedTerminal",
    "args": [
        "run",
        "streamlit/sca_streamlit_app.py"
    ]
}
```

This allows you to set breakpoints and debug the dashboard with live user inputs.

## Dependencies

Key dependencies include:

- **Core**: `SQLAlchemy`, `networkx`, `pyvis`, `streamlit`
- **LLM**: `openai`, `dspy`, `pydantic`
- **Data Processing**: `numpy`, `pandas`, `tqdm`
- **Database**: `sqlite3` (built-in), `pymysql`, `mysql-connector-python` (optional)
- **Utilities**: `python-dotenv`, `PyYAML`, `aiofiles`

See `setup.cfg` for the complete dependency list and version requirements.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Merge Request

## Support

- 📖 **Documentation**: Check the `tests/README_tests.md` for database setup
- 🐛 **Issues**: Report bugs through GitHub Issues
- 💡 **Features**: Request new features through GitHub Issues
- 📧 **Contact**: fernando.demeer@supsi.ch

## Acknowledgments

- Netflix dataset sourced from [lerocha/netflixdb](https://github.com/lerocha/netflixdb)
- Built with [Streamlit](https://streamlit.io/) for the web interface
- Graph visualization powered by [vis.js](https://visjs.org/)
