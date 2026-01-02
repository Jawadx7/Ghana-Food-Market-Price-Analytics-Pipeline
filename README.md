# Ghana Food Market Price Analytics Pipeline ✅

Lightweight ETL pipeline to extract, transform and (eventually) load WFP market price data for Ghana. The project focuses on cleaning and standardizing food price data (per-kg prices), detecting outliers, and preparing the dataset for downstream analysis and analytics.

---

## Table of Contents

- 📌 About
- ⚙️ Features
- 🗂️ Repository Structure
- 📥 Data
- ⚙️ Installation
- 🚀 Usage
- 🛠️ Development Notes
- ✅ Tests
- ✍️ Contributing
- 📄 License

---

## 📌 About

This repo implements an ETL pipeline for WFP Ghana food price data (CSV). The pipeline reads raw CSV(s), runs a set of data cleaning and transformation steps, detects outliers and provides an enriched dataset ready for analytics.

## ⚙️ Features

- Extraction from CSV using `pandas` (handles HXL style header row)
- Robust transformation steps:
  - Remove HXL header row
  - Standardize column names
  - Parse dates reliably
  - Normalize categories and text fields
  - Convert numeric and coordinate types
  - Normalize units to per-kg and compute `price_per_kg`
  - Detect outliers using z-score method
- Structured logging (writes component logs to `logs/`)
- Modular code organized under `src/`

## 🗂️ Repository Structure

Key files and folders:

- `src/pipeline.py` — main entry point that orchestrates extraction, transform, enrichment
- `src/extract/csv_extractor.py` — reads CSV into a DataFrame
- `src/transform/clean_prices.py` — transformation helpers (remove HXL, clean types, normalize units, outlier detection)
- `src/transform/transform_prices.py` — orchestrator for transformation steps
- `src/transform/enrich_prices.py` — enrichment placeholder (add more features here)
- `src/config/logger_config.py` — sets up file and console logging
- `data/raw/wfp_food_prices_gha.csv` — example raw dataset (included)
- `requirements.txt` — Python dependencies

---

## 📥 Data

Source data should be a CSV similar to `data/raw/wfp_food_prices_gha.csv`. The file included in `data/raw/` contains a HXL header row (starting with `#...`) followed by rows with fields such as `date, admin1, market, commodity, unit, price, usdprice`.

---

## ⚙️ Installation

Recommended: Python 3.9+ (or newer). Create a virtual environment and install dependencies:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install --upgrade pip; pip install -r requirements.txt
```

Create a `.env` file in the repo root with at least:

```env
RAW_PRICES_PATH=data/raw/wfp_food_prices_gha.csv
```

---

## 🚀 Usage

Run the pipeline from the project root:

```powershell
python -m src.pipeline
# or
python src\pipeline.py
```

What happens:

- The pipeline reads the CSV pointed to by `RAW_PRICES_PATH` (from `.env`).
- The transformation pipeline runs (cleaning, parsing, unit normalization, outlier detection).
- Enrichment runs (currently a minimal placeholder).
- Logs are written to `logs/` (see `pipeline.log`, `extract.log`, `transform.log`).

The `pipeline.py` currently prints a head of the enriched DataFrame as a simple proof of concept.

---

## 🛠️ Development Notes

- Logging: Use `setup_logger(name, LogType)` (see `src/config/logger_config.py`) — logs are saved under `logs/`.
- Add more enrichment steps in `src/transform/enrich_prices.py`.
- Add loading logic under `src/load/` to push processed data to a database, parquet, or analytics store.
- Keep transforms small and unit-testable: `clean_prices.py` contains many small functions that can be tested independently.

Suggested improvements / TODOs:

1. Implement full enrichment features (e.g., add temporal features, regional aggregations)
2. Implement a `load` module to persist outputs
3. Add unit tests for each transformation helper
4. Add CI (GitHub Actions) for linting, tests and packaging

---

## ✅ Tests

Run tests using `pytest`:

```powershell
pytest
```

Currently there are no tests in `tests/`. Add tests for

- `extract_prices()`
- each function in `src/transform/clean_prices.py`
- `transform_prices()` orchestration

---

## ✍️ Contributing

Contributions are welcome. Please:

1. Fork the repository
2. Create a feature branch
3. Open a PR with a clear description and tests for the change

---

## 📄 License

No license specified in this repository. If you intend to open-source this project, please add a `LICENSE` file (e.g., MIT, Apache 2.0) to make the terms clear.

---

Maintainer: **Jawadx7**
