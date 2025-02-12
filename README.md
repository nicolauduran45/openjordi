# OpenJoRDI 🐉🔬🫰👴🏻

OpenJordi is an open-source platform that consolidates and standardizes grant data from multiple sources into a centralized repository using DuckDB. It integrates LLM-based processing for data cleaning and ontology alignment, providing structured access via an API.

## Key Features
- **Data Collection**: Extracts grant data from diverse portals.
- **Data Processing**: Uses LLM to clean, structure, and align data with ontology.
- **Storage**: Stores structured data in DuckDB.
- **API Access**: Provides an API for querying grant information.
- **Standardization**: Implements metadata alignment based on a defined ontology.

## Project Structure
```
openjordi/
├── data/                   # Raw and processed data storage
├── notebooks/              # Jupyter notebooks for exploration
├── scripts/                # Scripts for data collection and processing
│   ├── fetch_data.py       # Script to fetch grant data from sources
│   ├── clean_data.py       # LLM-based data cleaning and alignment
│   ├── load_data.py        # Load data into DuckDB
├── api/                    # API implementation
│   ├── app.py              # FastAPI-based API server
├── ontology/               # Metadata definitions and ontology mappings
├── config/                 # Configuration files
├── tests/                  # Unit and integration tests
├── requirements.txt        # Dependencies list
├── README.md               # Project documentation
├── LICENSE                 # License file
└── .gitignore              # Git ignore rules
```

## Installation
### Prerequisites
- Python 3.9+
- DuckDB
- FastAPI
- Requests (for fetching data)
- HuggingFace and Together.AI (for LLM processing)

### Setup
```bash
git clone https://github.com/yourusername/openjordi.git
cd openjordi
pip install -r requirements.txt
```

## Usage
### 1. Fetch Data
```bash
python scripts/fetch_data.py
```
### 2. Process & Clean Data
```bash
python scripts/clean_data.py
```
### 3. Load Data into DuckDB
```bash
python scripts/load_data.py
```
### 4. Start API
```bash
uvicorn api.app:app --reload
```

## API Endpoints
| Method | Endpoint | Description |
|--------|---------|-------------|
| GET | `/grants` | Retrieve all grants |
| GET | `/grants/{id}` | Retrieve grant by ID |
| POST | `/grants` | Add new grant |
| GET | `/search` | Search grants based on filters |

## Contributing
We welcome contributions! Please check `CONTRIBUTING.md` for details.

## Environemnt

Create the environment from `environment.yml`
```bash
conda env create -f environment.yml --name openjordi
```

Export the environment, when we install need dependencies
```bash
conda env export --name openjordi > environemnt.yml
```

## License

This project is licensed under [Apache Licence](https://github.com/sirisacademic/erc-classifiers/blob/main/LICENSE).

