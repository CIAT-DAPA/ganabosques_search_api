#🐮 Ganabosques Search API
![GitHub release (latest by](https://img.shields.io/github/v/release/CIAT-DAPA/ganabosques_search_api)
![GitHub tag (latest by](https://img.shields.io/github/v/tag/CIAT-DAPA/ganabosques_search_api)

##📌 Description
Ganabosques Search API is a backend service designed to support search
and retrieval operations within the Ganabosques platform.
This API is built using FastAPI and follows a modular architecture.

##🏗️ Project Structure
    ganabosques_search_api/
    │
    ├── src/
    │   ├── main.py
    │   ├── auth/
    │   ├── routes/
    │   ├── services/
    │   ├── models/
    │   └── requirements.txt
    │
    ├── tests/
    ├── .github/workflows/
    └── README.md

##⚙️ Requirements
Python 3.10+
pip

##🚀 Installation
```bash
    git clone https://github.com/CIAT-DAPA/ganabosques_search_api.git
    cd ganabosques_search_api
    python -m venv env
```

Activate:
Linux/Mac:
```bash
    source env/bin/activate
```

Windows:
```bash
    env\Scripts\activate
```

Install dependencies:
```bash
    pip install -r src/requirements.txt
```

##▶️ Running the API
```bash
    uvicorn src.main:app --reload
```

Docs: - http://127.0.0.1:8000/docs - http://127.0.0.1:8000/redoc

##🧪 Testing
```bash
    PYTHONPATH=. pytest tests/
```
or
```bash
    python -m unittest discover tests
```
