# ClaimFlow-Medallion: Insurance PDF Claim Parser & Reconciler

ClaimFlow-Medallion is a data engineering and web dashboard project demonstrating a document parsing and policy reconciliation pipeline using a **Medallion Data Architecture (Bronze ➔ Silver ➔ Gold)**. 

It contains both a **production-ready Azure Databricks Spark Structured Streaming notebook** using Unity Catalog and a **local Streamlit web application** for interactive testing and presentation.

---

## Architecture Overview

```
                        +----------------------------+
                        |  Incoming Claims PDFs      |
                        |  (Azure Blob / Local Ingest)|
                        +--------------+-------------+
                                       |
                                       v
   [BRONZE]   +----------------------------------------------+
   Raw Ingest |  Extracts raw text from PDF binaries via UDF |
              |  Writes to: bronze_claims Delta Table        |
              +------------------------+---------------------+
                                       |
                                       v
   [SILVER]   +----------------------------------------------+
   Cleanse &  |  Parses fields using SQL Regex extraction     |
   Structure  |  Calculates SHA-256 Surrogate Key            |
              |  Writes to: silver_claims Delta Table        |
              +------------------------+---------------------+
                                       |
                                       v  <-- [Stream-Static Join] on reference_no
   [GOLD]     +------------------------+---------------------+   +-----------------------+
   Reconcile  |  Checks for duplicates using Surrogate Key   |   | reference_policies   |
   & Map      |  Validates name matches, coverage limits, etc|   | Master Delta Database |
              |  Writes to: gold_claims_report               |   +-----------------------+
              +------------------------+---------------------+
                                       |
                                       v  (If duplicate detected)
                        +--------------+-------------+
                        |  Quarantine Folder (Review) |
                        |  (PDF file routed here)     |
                        +----------------------------+
```

---

## File Structure

```
/
├── data/
│   ├── bronze/                           # Local raw parsed JSONs (Streamlit)
│   ├── silver/                           # Local cleaned JSONs (Streamlit)
│   ├── gold/                             # Local reconciled reports (Streamlit)
│   ├── review/                           # Quarantined duplicate PDF files
│   └── reference_policies.json           # Reference policy database
├── samples/                              # Pre-generated sample PDF files
├── app/                                  # Common Python module for local processing
│   ├── __init__.py
│   ├── parser.py                         # PDF text & regex extraction logic
│   ├── pipeline.py                       # Local Medallion pipeline simulator
│   └── database.py                       # Policy database CRUD helpers
├── insurance_claims_medallion.ipynb      # Databricks Spark Structured Streaming Notebook
├── app.py                                # Streamlit Web Dashboard
├── generate_samples.py                   # Script to create sample PDFs for testing
└── requirements.txt                      # Dependencies (streamlit, pypdf, reportlab, etc.)
```

---

## Streamlit Local Application Setup

### 1. Install Dependencies
Ensure you have Python 3.11+ installed. Install the required libraries:
```bash
pip install -r requirements.txt
```

### 2. Generate Sample Claim Forms
Run the sample generation script. This will populate the `samples/` directory with test PDF files representing active, expired, invalid, and limit-exceeded claim forms:
```bash
python generate_samples.py
```

### 3. Run the Dashboard
Start the Streamlit application:
```bash
streamlit run app.py
```
Open your browser and navigate to `http://localhost:8501`.

### Local Testing Scenarios:
1. **Valid Claim**: Upload `claim_john_smith_valid.pdf` (Status: **APPROVED**).
2. **Coverage Limit Exceeded**: Upload `claim_sarah_connor_over_limit.pdf` (Status: **REJECTED - LIMIT EXCEEDED**).
3. **Invalid Reference**: Upload `claim_invalid_reference.pdf` (Status: **REJECTED - POLICY NOT FOUND**).
4. **Claimant Mismatch**: Upload `claim_name_mismatch.pdf` (Status: **REJECTED - CLAIMANT MISMATCH**).
5. **Duplicate Quarantine**: Upload `claim_john_smith_valid.pdf` a second time. The dashboard will flag the status as **FLAGGED - DUPLICATE (UNDER REVIEW)**, and quarantine the PDF inside the `data/review/` directory.

---

## Azure Databricks Streaming Integration

The notebook `insurance_claims_medallion.ipynb` is designed to run in Azure Databricks targeting the Australian East Development Workspace catalog `adb-core-data-dev-aue`.

### How to Deploy in Databricks:
1. **Import Notebook**: 
   * Open your Databricks workspace.
   * Go to **Workspace ➔ Users ➔ Your User**, right-click and select **Import**.
   * Upload the `insurance_claims_medallion.ipynb` file.
2. **Attach Cluster**: Attach the notebook to a cluster running Databricks Runtime 13.3 LTS or higher.
3. **Run Setup**:
   * Run the environment cell to verify the catalog catalog `adb-core-data-dev-aue` and initialize schema `insurance_claims`.
   * Run the master policy cell to load reference records.
4. **Start Streaming Queries**:
   * Run the Bronze, Silver, and Gold streaming cells.
   * Spark Structured Streaming will monitor DBFS `/tmp/claims_streaming/raw/` (or your configured ADLS Gen2 path `abfss://...`) and ingest files on-the-fly.
5. **Audit Duplicates**:
   * Run the SQL verification cells to view the final mapped output schema.
   * Periodically trigger the `quarantine_duplicate_files()` Python routine to quarantine duplicate PDFs inside DBFS `/tmp/claims_streaming/review/`.
