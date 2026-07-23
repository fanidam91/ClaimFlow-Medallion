# Databricks notebook source
# MAGIC %md
# MAGIC # ClaimFlow-Medallion: PDF Claims Parser & Reconciler
# MAGIC ### End-to-End Spark Structured Streaming Pipeline with Medallion Architecture (Bronze -> Silver -> Gold)
# MAGIC 
# MAGIC This Databricks Notebook implements a real-time ingestion pipeline for processing insurance claim forms submitted as PDF files. 
# MAGIC The architecture follows the **Medallion Data Design Pattern** under the **Unity Catalog** on Azure Databricks:
# MAGIC 
# MAGIC 1. **Bronze Layer**: Streams binary PDF files from Azure Blob Storage (`abfss://`), extracts raw text via a PySpark UDF using `pypdf`, and appends the metadata and raw text to `bronze_claims`.
# MAGIC 2. **Silver Layer**: Streams raw records, extracts structured fields (`reference_no`, `claimant_name`, `claim_amount`, `claim_date`) using Spark SQL regex functions, and calculates a SHA-256 **Surrogate Key**.
# MAGIC 3. **Gold Layer**: Joins the Silver stream with a static **Reference Policies** table in a **Stream-Static Join**, checks for duplicate submissions using the surrogate key, applies business reconciliation rules, and logs outcomes in `gold_claims_report`.
# MAGIC 4. **Quarantine Routine**: Detects duplicates and moves the duplicate source PDFs to a separate review folder (`review/`) for audit.

# COMMAND ----------
# MAGIC %pip install pypdf reportlab

# COMMAND ----------
import io
import os
import time
from pypdf import PdfReader
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Catalog, Schema and Storage Configurations
# MAGIC We configure the three-level namespace under the Australian East Dev Catalog: `` `adb-core-data-dev-aue` ``.

# COMMAND ----------
CATALOG_NAME = "`adb-core-data-dev-aue`"
SCHEMA_NAME = "insurance_claims"

# Production Azure Blob Storage Path Configuration (ABFSS)
# In production, replace the DBFS path with your mounted ADLS Gen2 path, e.g.:
# RAW_PATH = "abfss://claims-raw@stcoredatadevaue.dfs.core.windows.net/submissions"
# REVIEW_PATH = "abfss://claims-review@stcoredatadevaue.dfs.core.windows.net/review"

BASE_STORAGE_PATH = "dbfs:/tmp/claims_streaming"
RAW_PATH = f"{BASE_STORAGE_PATH}/raw"
REVIEW_PATH = f"{BASE_STORAGE_PATH}/review"

CHECKPOINT_BRONZE = f"{BASE_STORAGE_PATH}/_checkpoints/bronze"
CHECKPOINT_SILVER = f"{BASE_STORAGE_PATH}/_checkpoints/silver"
CHECKPOINT_GOLD = f"{BASE_STORAGE_PATH}/_checkpoints/gold"

# Initialize DBFS directories
dbutils.fs.mkdirs(RAW_PATH)
dbutils.fs.mkdirs(REVIEW_PATH)

# Ensure target Unity Catalog Schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
print(f"Database schema initialized under: {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Initialize static Reference Policies Table
# MAGIC Creates the active policies list that we will match incoming claims against.

# COMMAND ----------
policies_data = [
    ("POL-101", "John Smith", "Auto Insurance", "Active", 5000.0, "2025-01-01"),
    ("POL-102", "Sarah Connor", "Home Insurance", "Active", 10000.0, "2024-06-15"),
    ("POL-103", "Alice Johnson", "Health Insurance", "Expired", 25000.0, "2023-01-01"),
    ("POL-104", "Tony Stark", "Commercial Liability", "Active", 500000.0, "2025-01-01")
]

policies_schema = StructType([
    StructField("reference_no", StringType(), True),
    StructField("policyholder", StringType(), True),
    StructField("policy_type", StringType(), True),
    StructField("policy_status", StringType(), True),
    StructField("coverage_limit", DoubleType(), True),
    StructField("effective_date", StringType(), True)
])

df_policies = spark.createDataFrame(policies_data, schema=policies_schema)
df_policies.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.reference_policies")
print("Master policy database written to Delta Table successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Utility Function to generate Sample Claim PDFs
# MAGIC This helps simulate file drops into Blob storage / DBFS path.

# COMMAND ----------
def generate_sample_pdf_on_dbfs(filename, ref_no, claimant, date, amount, reason):
    """Generates a structured test PDF claim document and copies it to the streaming raw folder."""
    local_path = f"/tmp/{filename}"
    c = canvas.Canvas(local_path, pagesize=letter)
    width, height = letter
    
    # Header
    c.setFillColorRGB(0.1, 0.2, 0.45)
    c.rect(0, height - 100, width, 100, fill=True, stroke=False)
    c.setFillColorRGB(1.0, 1.0, 1.0)
    c.setFont("Helvetica-Bold", 20)
    c.drawString(50, height - 60, "SECURE GUARD INSURANCE")
    c.setFont("Helvetica", 10)
    c.drawString(50, height - 80, "Official Claim Request Document (Databricks Stream Test)")
    
    # Content
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, height - 130, "Claim Submission Details")
    c.setStrokeColorRGB(0.8, 0.8, 0.8)
    c.line(50, height - 140, width - 50, height - 140)
    
    # Form fields
    y = height - 170
    fields = [
        ("Reference Number:", ref_no),
        ("Claimant Name:", claimant),
        ("Claim Date:", date),
        ("Claim Amount:", amount),
        ("Reason for Claim:", reason)
    ]
    for label, val in fields:
        c.setFont("Helvetica-Bold", 10)
        c.drawString(50, y, label)
        c.setFont("Helvetica", 10)
        c.drawString(220, y, str(val))
        y -= 25
        
    c.line(50, y - 5, width - 50, y - 5)
    c.save()
    
    # Copy local file to DBFS raw streaming path
    dbutils.fs.cp(f"file:{local_path}", f"{RAW_PATH}/{filename}")
    os.remove(local_path)
    print(f"Spawned claim PDF: {filename} -> {RAW_PATH}/{filename}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Bronze Streaming Ingestion Layer
# MAGIC Loads PDF documents as raw binary streams, extracts text via UDF, and writes to `bronze_claims` table.

# COMMAND ----------
# Define PDF Text Extractor UDF
@udf("string")
def extract_pdf_text(content):
    try:
        reader = PdfReader(io.BytesIO(content))
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text
    except Exception as e:
        return f"Parsing Error: {str(e)}"

# Streaming binary file ingestion source
df_binary_stream = (spark.readStream
                    .format("binaryFile")
                    .option("pathGlobFilter", "*.pdf")
                    .load(RAW_PATH))

# Ingest to Bronze Table
query_bronze = (df_binary_stream
                .withColumn("raw_text", extract_pdf_text(col("content")))
                .drop("content")  # Remove raw byte column to save storage space
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_BRONZE)
                .toTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_claims"))

print("Bronze Streaming Query initialized and running.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Silver Streaming Cleansing & Structuring Layer
# MAGIC Reads from the Bronze table, extracts key variables using regex patterns, standardizes types, and hashes variables to generate a **Surrogate Key**.

# COMMAND ----------
df_bronze_stream = spark.readStream.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_claims")

df_silver_parsed = df_bronze_stream.select(
    col("path"),
    col("modificationTime").alias("ingested_time"),
    regexp_extract(col("raw_text"), r"(?i)Reference\s*(?:No|Number)?\s*:\s*([A-Z0-9-]+)", 1).alias("reference_no"),
    regexp_extract(col("raw_text"), r"(?i)Claimant\s*(?:Name)?\s*:\s*([A-Za-z \t\.\'\-]+)", 1).alias("claimant_name"),
    regexp_extract(col("raw_text"), r"(?i)Claim\s*Date\s*:\s*([\d-]+)", 1).alias("claim_date"),
    regexp_extract(col("raw_text"), r"(?i)Claim\s*Amount\s*:\s*\$?([\d,.]+)", 1).alias("claim_amount"),
    regexp_extract(col("raw_text"), r"(?i)Reason\s*(?:for\s*Claim)?\s*:\s*([^\n]+)", 1).alias("claim_reason")
).withColumn(
    "claimant_name", initcap(trim(col("claimant_name")))
).withColumn(
    "reference_no", upper(trim(col("reference_no")))
).withColumn(
    "claim_amount", regexp_replace(col("claim_amount"), ",", "").cast("double")
).withColumn(
    # Generate SHA-256 Surrogate Key based on parsed parameters
    "surrogate_key",
    sha2(concat_ws("||", 
                   coalesce(col("reference_no"), lit("")), 
                   coalesce(col("claimant_name"), lit("")), 
                   coalesce(col("claim_date"), lit("")), 
                   coalesce(col("claim_amount"), lit(""))), 256)
)

query_silver = (df_silver_parsed
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_SILVER)
                .toTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_claims"))

print("Silver Streaming Query initialized and running.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Gold Streaming Reconciliation & Verification Layer
# MAGIC Before running this stream, we ensure the Gold target table exists. This allows us to perform self-lookup joins to detect duplicate submissions stream-by-stream.

# COMMAND ----------
# Initialize empty Gold table if it doesn't exist to prevent catalog schema lookup errors
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.gold_claims_report (
  surrogate_key STRING,
  reference_no STRING,
  claimant_name STRING,
  claim_amount DOUBLE,
  claim_date STRING,
  claim_reason STRING,
  policyholder_db STRING,
  policy_type_db STRING,
  policy_status_db STRING,
  coverage_limit_db DOUBLE,
  is_policy_found BOOLEAN,
  is_name_matched BOOLEAN,
  is_policy_active BOOLEAN,
  is_limit_exceeded BOOLEAN,
  amount_difference DOUBLE,
  claim_status STRING,
  rejection_reason STRING,
  processed_time TIMESTAMP,
  path STRING,
  is_duplicate BOOLEAN
) USING DELTA
""")

# COMMAND ----------
df_silver_stream = spark.readStream.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_claims")
df_policies_static = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.reference_policies")

# Read Gold table statically to join and look up already processed keys (Duplicates)
df_gold_static = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.gold_claims_report")

df_joined_stream = df_silver_stream.join(
    df_policies_static,
    on="reference_no",
    how="left"
).join(
    df_gold_static.select("surrogate_key").withColumn("is_duplicate_flag", lit(True)),
    on="surrogate_key",
    how="left"
)

df_gold_reconciled = df_joined_stream.select(
    col("surrogate_key"),
    col("reference_no"),
    col("claimant_name"),
    col("claim_amount"),
    col("claim_date"),
    col("claim_reason"),
    col("policyholder").alias("policyholder_db"),
    col("policy_type").alias("policy_type_db"),
    col("policy_status").alias("policy_status_db"),
    col("coverage_limit").alias("coverage_limit_db"),
    
    col("policyholder").isNotNull().alias("is_policy_found"),
    (lower(trim(col("claimant_name"))) == lower(trim(col("policyholder"))).alias("is_name_matched")),
    (col("policy_status") == "Active").alias("is_policy_active"),
    (col("claim_amount") > col("coverage_limit")).alias("is_limit_exceeded"),
    (col("claim_amount") - col("coverage_limit")).alias("amount_difference"),
    
    # Validation rule output
    when(col("is_duplicate_flag") == True, "FLAGGED - DUPLICATE (UNDER REVIEW)")
    .when(col("policyholder").isNull(), "REJECTED - POLICY NOT FOUND")
    .when(col("policy_status") != "Active", "REJECTED - POLICY EXPIRED")
    .when(lower(trim(col("claimant_name"))) != lower(trim(col("policyholder"))), "REJECTED - CLAIMANT MISMATCH")
    .when(col("claim_amount") > col("coverage_limit"), "REJECTED - COVERAGE EXCEEDED")
    .otherwise("APPROVED").alias("claim_status"),
    
    when(col("is_duplicate_flag") == True, "Duplicate claim with identical details detected.")
    .when(col("policyholder").isNull(), "Reference policy number not found in reference database.")
    .when(col("policy_status") != "Active", "Target policy is inactive or expired.")
    .when(lower(trim(col("claimant_name"))) != lower(trim(col("policyholder"))), "Claimant name does not match policy owner.")
    .when(col("claim_amount") > col("coverage_limit"), "Claim amount exceeds the policy coverage limit.")
    .otherwise("All validations passed. Policy matches and coverage approved.").alias("rejection_reason"),
    
    current_timestamp().alias("processed_time"),
    col("path"),
    coalesce(col("is_duplicate_flag"), lit(False)).alias("is_duplicate")
)

query_gold = (df_gold_reconciled
              .writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", CHECKPOINT_GOLD)
              .toTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.gold_claims_report"))

print("Gold Streaming Query initialized and running.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 7. Duplicate File Quarantine Script
# MAGIC A utility script that runs on a schedule to move duplicate source PDF files out of the stream path to prevent processing loops and clear raw directory.

# COMMAND ----------
def quarantine_duplicate_files():
    """Audits Gold table for duplicate claims and quarantine moves their PDFs from RAW to REVIEW directory."""
    duplicates = spark.sql(f"""
        SELECT DISTINCT path 
        FROM {CATALOG_NAME}.{SCHEMA_NAME}.gold_claims_report 
        WHERE claim_status = 'FLAGGED - DUPLICATE (UNDER REVIEW)'
    """).collect()
    
    for row in duplicates:
        source_path = row['path']
        filename = os.path.basename(source_path)
        dest_path = f"{REVIEW_PATH}/DUPLICATE_{filename}"
        
        try:
            if dbutils.fs.ls(source_path):
                print(f"Quarantining duplicate blob file: {source_path} -> {dest_path}")
                dbutils.fs.mv(source_path, dest_path)
        except Exception:
            # Handled if file was already moved
            pass

quarantine_duplicate_files()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 8. Verification & Auditing SQL Queries

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Summary metric query
# MAGIC SELECT claim_status, count(*) as count 
# MAGIC FROM `adb-core-data-dev-aue`.insurance_claims.gold_claims_report 
# MAGIC GROUP BY claim_status

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Reconciled dashboard query mapping claimant and policy details side-by-side
# MAGIC SELECT 
# MAGIC   surrogate_key,
# MAGIC   reference_no,
# MAGIC   claimant_name,
# MAGIC   policyholder_db,
# MAGIC   claim_amount,
# MAGIC   coverage_limit_db,
# MAGIC   claim_status,
# MAGIC   rejection_reason,
# MAGIC   is_duplicate,
# MAGIC   path
# MAGIC FROM `adb-core-data-dev-aue`.insurance_claims.gold_claims_report
# MAGIC ORDER BY processed_time DESC

# COMMAND ----------
# MAGIC %md
# MAGIC ### 9. Streaming Ingestion Test Run
# MAGIC We drop mock files to see the Structured Stream process them in real-time.

# COMMAND ----------
# Ingest a valid claim for John Smith (POL-101)
generate_sample_pdf_on_dbfs("claim_john_smith_valid.pdf", "POL-101", "John Smith", "2026-05-12", "$1,200.00", "Windshield rock strike damage and glass replacement")

time.sleep(5)

# Ingest an invalid claim for Bruce Wayne (POL-999)
generate_sample_pdf_on_dbfs("claim_invalid_reference.pdf", "POL-999", "Bruce Wayne", "2026-06-10", "$5,000.00", "Bumper replacement and minor bodywork scratching")

time.sleep(5)

# Trigger a duplicate of John Smith to verify surrogate key duplicate flag and quarantine
generate_sample_pdf_on_dbfs("claim_john_smith_duplicate.pdf", "POL-101", "John Smith", "2026-05-12", "$1,200.00", "Windshield rock strike damage and glass replacement")

time.sleep(5)
quarantine_duplicate_files()
