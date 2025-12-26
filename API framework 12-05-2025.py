# Databricks notebook source
# %pip install aiohttp

# COMMAND ----------

dbutils.widgets.text("config_date_params_overwrite", "0")
dbutils.widgets.text("datetime_start", "2025-11-01T00:00:00.000Z")
dbutils.widgets.text("datetime_end", "2025-11-02T23:59:59.999Z")
dbutils.widgets.text("SourceFileName", "conversations.json")
dbutils.widgets.text("SourceFilePath", "/dbfs/mnt/radaruc/bronze/genesys/genesys/analytics/conversations/config/")
dbutils.widgets.text("TargetFileName", "conversations_2025-11-03_000000.json")
dbutils.widgets.text("TargetFilePath", "bronze/genesys/genesys/analytics/conversations/delta_date=2025-11-03/")
dbutils.widgets.text("backfill_from_historical", "0")  # 0=false, 1=true
dbutils.widgets.text("backfill_table_name", "")  # Empty = all tables, or specific table name

# COMMAND ----------


# schema_json = spark.table("silver.genesys_conversation_emails").schema.json()
# print(schema_json)

# COMMAND ----------

# ========================================
# CELL 1: API Configuration JSON
# ========================================


import json
# '/dbfs/mnt/radaruc/bronze/genesys/genesys/analytics/queue/config/Genesys_queue.json'
SourceFilePath = dbutils.widgets.get("SourceFilePath")
SourceFileName = dbutils.widgets.get("SourceFileName")
TargetFilePath = dbutils.widgets.get("TargetFilePath")
TargetFileName = dbutils.widgets.get("TargetFileName")
config_path = SourceFilePath + SourceFileName

config_date_params_overwrite = int(dbutils.widgets.get("config_date_params_overwrite"))
datetime_start = dbutils.widgets.get("datetime_start")
datetime_end = dbutils.widgets.get("datetime_end")




with open(config_path, 'r') as f:
    API_CONFIG = json.load(f)

print(json.dumps(API_CONFIG, indent=2))
print(f'config_path: {config_path}')
print("✓ API Configuration loaded")
print(f"  API URL: {API_CONFIG['API_URL']}")
print(f"  Main table: {API_CONFIG['main_table_name']}")


global start_date, end_date
if config_date_params_overwrite: #check which date parameter to use
    start_date = datetime_start.strip()
    end_date = datetime_end.strip()
else:
    start_date = API_CONFIG['START_DATE'].strip()
    end_date = API_CONFIG['END_DATE'].strip()

print(f"  Date range: {start_date} to {end_date}")
print(f"  Page size: {API_CONFIG['PAGE_SIZE']}")
print(f"  Nested tables: {len(API_CONFIG['nested_tables'])}")

# COMMAND ----------

# Validate unnested_column is a list
for nested in API_CONFIG.get("nested_tables", []):
    if not isinstance(nested.get("unnested_column"), list):
        print(f"⚠️  WARNING: 'unnested_column' should be a list for table '{nested['unnested_to_table']}'")

# COMMAND ----------


# ========================================
# CELL 2: Imports
# ========================================

import requests
import asyncio
import aiohttp
import json
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import time
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StringType
from asyncio import Semaphore
print("✓ Imports loaded")

# ========================================
# RATE LIMIT SETTINGS (safe for Genesys)
# ========================================
MAX_CONCURRENT = 8                     # never more than 8 requests at the same time
REQUEST_SEMAPHORE = Semaphore(MAX_CONCURRENT)
SLEEP_BETWEEN_CHUNKS = 1.5             # small pause between 7-day chunks

# COMMAND ----------


# ========================================
# CELL 3: Core Framework Functions
# ========================================

def split_date_range_by_days(start_date_str, end_date_str, interval_days):
    """Split a date range into chunks of specified days"""
    start = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    end = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    chunks = []
    current = start
    
    while current <= end:
        next_chunk = current + timedelta(days=interval_days)
        chunk_end = min(next_chunk - timedelta(seconds=1), end)
        
        chunks.append({
            'start': current.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'end': chunk_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        })
        
        current = next_chunk
    
    return chunks


# get_bearer_token
def get_bearer_token(token_url, client_id, client_secret):
    """Get OAuth bearer token"""
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    response = requests.post(token_url, data=token_data)
    response.raise_for_status()
    token = response.json()["access_token"]
    print(f"✓ Bearer token obtained")
    return token


#  Helper function to substitute placeholders in payload template
def build_query_payload_from_template(payload_template, start_date, end_date, page_size, page_number):
    """
    Build query payload from template by replacing placeholders
    Supports nested dict structures
    """
    # Convert template to JSON string, replace placeholders, convert back
    payload_str = json.dumps(payload_template)
    payload_str = payload_str.replace("{start_date}", start_date)
    payload_str = payload_str.replace("{end_date}", end_date)
    payload_str = payload_str.replace('"{page_size}"', str(page_size))  # Remove quotes for integers
    payload_str = payload_str.replace('"{page_number}"', str(page_number))
    
    return json.loads(payload_str)


# Make API calss to get total pages
def get_total_pages(token, api_url, http_method, start_date, end_date, payload_template, page_size, pages_key, data_key):
    """Make initial request to determine total pages"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    if http_method.upper() == "POST":
        payload = build_query_payload_from_template(payload_template, start_date, end_date, page_size, 1)
        response = requests.post(api_url, headers=headers, json=payload)
    else:  # GET
        # Build query parameters for GET (page_size and page_number in URL)
        params = {"pageSize": page_size, "pageNumber": 1}
        response = requests.get(api_url, headers=headers, params=params)
    
    response.raise_for_status()
    
    data = response.json()
    total_hits = data.get(pages_key, 0)
    total_pages = (total_hits + page_size - 1) // page_size
    
    print(f"✓ Total records: {total_hits}")
    print(f"✓ Total pages: {total_pages} (pageSize={page_size})")
    
    return total_pages, data.get(data_key, [])


# fucntion to get data from invidual page
async def fetch_page(session, token, api_url, http_method, start_date, end_date, page_number, 
                    payload_template, page_size, data_key, max_retries, retry_delay):
    """Fetch a single page with proper 429 handling and concurrency limit"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    async with REQUEST_SEMAPHORE:  # ← concurrency traffic light
        for attempt in range(1, max_retries + 1):
            try:
                if http_method.upper() == "POST":
                    payload = build_query_payload_from_template(payload_template, start_date, end_date, page_size, page_number)
                    async with session.post(api_url, headers=headers, json=payload, timeout=90) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", retry_delay * 2))
                            print(f"  ⚠ 429 Too Many Requests – waiting {retry_after}s (page {page_number})")
                            await asyncio.sleep(retry_after + 2)
                            continue
                            
                        resp.raise_for_status()
                        data = await resp.json()
                        records = data.get(data_key, [])
                        print(f"  → Page {page_number} OK ({len(records)} records)")
                        return records
                        
                else:  # GET
                    params = {"pageSize": page_size, "pageNumber": page_number}
                    async with session.get(api_url, headers=headers, params=params, timeout=90) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", retry_delay * 2))
                            print(f"  ⚠ 429 Too Many Requests – waiting {retry_after}s (page {page_number})")
                            await asyncio.sleep(retry_after + 2)
                            continue
                            
                        resp.raise_for_status()
                        data = await resp.json()
                        records = data.get(data_key, [])
                        print(f"  → Page {page_number} OK ({len(records)} records)")
                        return records
                    
            except Exception as e:
                print(f"  ⚠ Attempt {attempt}/{max_retries} failed (page {page_number}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
        
        print(f"  ✗ Page {page_number} failed after {max_retries} attempts")
    
    return []

# Make API calls to get all pages concurrently
async def fetch_all_pages_async(token, api_url, http_method, start_date, end_date, start_page, end_page,
                                payload_template, page_size, data_key, max_retries, retry_delay):
    """Fetch multiple pages asynchronously"""
    all_data = []
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_page(session, token, api_url, http_method, start_date, end_date, page, 
                      payload_template, page_size, data_key, max_retries, retry_delay) 
            for page in range(start_page, end_page + 1)
        ]
        results = await asyncio.gather(*tasks)
        
        for data in results:
            all_data.extend(data)
    
    return all_data

def save_raw_json_to_blob(data, TargetFileName, base_path):
    """
    Save raw API response data to JSON file in blob storage
    IMPORTANT: Uses /dbfs/ prefix for mounted Azure Blob Storage
    """
    from datetime import datetime
    import os
    
    # Generate timestamp for filename
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    TargetFileName = TargetFileName.replace(".json", "")
    filename = f"{TargetFileName}.jsonl"
    base_path = '/dbfs/mnt/radaruc/' + base_path
    full_path = os.path.join(base_path, filename)
    
    # Ensure directory exists
    os.makedirs(base_path, exist_ok=True)
    
    # Save JSON file
    with open(full_path, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + "\n")
    
    file_size_mb = os.path.getsize(full_path) / (1024*1024)
    
    # full_path_to_read = full_path.replace('/dbfs', '')

    print(f"\n{'='*60}")
    print(f"✓ RAW JSON BACKUP SAVED TO AZURE BLOB")
    print(f"{'='*60}")
    print(f"  File: {filename}")
    print(f"  Azure Path: {full_path}")  # Show mount path to read
    print(f"  Records: {len(data)}")
    print(f"  Size: {file_size_mb:.2f} MB")
    print(f"{'='*60}\n")
    
    return full_path

print("✓ API call functions defined")




# COMMAND ----------


# ========================================
# CELL 4: Nested Table Processing Functions
# ========================================



def get_schema_columns(schema_dict):
    """Extract column names from schema dictionary"""
    return list(schema_dict.keys())


# CHANGED: Simplified to only add missing columns as null strings (bronze layer)
def enforce_schema_columns(df, schema_dict):
    """
    Enforce schema columns on dataframe - add missing columns as null
    Bronze layer: All columns stored as strings, no type conversion
    """
    schema_cols = get_schema_columns(schema_dict)
    existing_cols = df.columns
    
    # Find missing columns
    missing_cols = [c for c in schema_cols if c not in existing_cols]
    
    if missing_cols:
        print(f"  ⚠ Adding {len(missing_cols)} missing columns as null")
    
    # Add missing columns as null strings (bronze layer - no type conversion)
    for col_name in missing_cols:
        print(f"      ⚠ Adding missing column: '{col_name}' as null")
        df = df.withColumn(col_name, lit(None).cast(StringType()))
    
    # Select columns in schema order
    df = df.select(*schema_cols)
    
    return df




print("✓ Nested table processing functions defined")


# COMMAND ----------


# ========================================
# CELL 5: collect api data and save to blob
# ========================================

# Extract variables from config at the start, pass to functions
async def ingest_api_data(config):
    """
    Main orchestration function for API ingestion
    Extracts variables from config and passes them to functions
    
    Args:
        config: API configuration dictionary containing all settings
    
    Returns:
        Dictionary of all created dataframes
    """
    # Extract variables from config
    api_url = config["API_URL"]
    token_url = config["TOKEN_URL"]
    http_method = config["HTTP_METHOD"]
    client_id = config["CLIENT_ID"]
    client_secret = config["CLIENT_SECRET"]
    # start_date = config["START_DATE"]
    # end_date = config["END_DATE"]
    retrieval_interval = config["RETRIEVAL_INTERVAL"]
    max_retries = config["MAX_RETRIES"]
    retry_delay = config["RETRY_DELAY"]
    page_size = config["PAGE_SIZE"]
    payload_template = config["REQUEST_PAYLOAD"]
    pages_key = config["pages"]
    data_key = config["data"]
    main_table_name = config["main_table_name"]
    data_schema = config["data_schema"]
    
    print(f"\n{'='*60}")
    print(f"STARTING API INGESTION")
    print(f"{'='*60}")
    print(f"Main table: {main_table_name}")
    print(f"API URL: {api_url}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Interval: {retrieval_interval} days")
    print(f"Page size: {page_size}")
    print(f"{'='*60}\n")


    # Global token cache (survives across cells in the same notebook session)
    if 'GENESYS_TOKEN' not in globals():
        GENESYS_TOKEN = None
        TOKEN_EXPIRES_AT = 0  # Unix timestamp when it dies

    current_time = time.time()

    # Reuse token if still valid (leave 60s safety margin)
    if GENESYS_TOKEN is None or current_time > TOKEN_EXPIRES_AT - 60:
        print("🔑 Fetching new Genesys OAuth token...")
        # Get authentication token
        token = get_bearer_token(token_url, client_id, client_secret)
        GENESYS_TOKEN = token
        TOKEN_EXPIRES_AT = current_time + 3500  # ~58 minutes safety
        print(f"✓ New token cached (expires in ~58 minutes)")
    else:
        token = GENESYS_TOKEN
        print(f"♻ Reusing cached token (valid for another {(TOKEN_EXPIRES_AT - current_time)/60:.1f} minutes)")
    
    
    #smart analyzing if data need to be split into chunks and fetch data accordingly
    all_data = []
    # Check if date chunking is needed (only for POST with date range)
    use_date_chunks = http_method.upper() == "POST" and start_date and end_date
    
    if use_date_chunks:
        # POST method with date range - split into chunks
        date_chunks = split_date_range_by_days(start_date, end_date, retrieval_interval)
        print(f"✓ Date range split into {len(date_chunks)} chunks\n")
        
        # Fetch data for each chunk
        for i, chunk in enumerate(date_chunks, 1):
            print(f"\n{'='*60}")
            print(f"Chunk {i}/{len(date_chunks)}: {chunk['start'][:10]} to {chunk['end'][:10]}")
            print(f"{'='*60}")
            
            # Get total pages for this chunk
            total_pages, first_page_data = get_total_pages(
                token, api_url, http_method, chunk['start'], chunk['end'], 
                payload_template, page_size, pages_key, data_key
            )
            
            chunk_data = first_page_data.copy()
            
            # Fetch remaining pages if needed
            if total_pages > 1:
                print(f"Fetching pages 2-{total_pages} asynchronously...")
                remaining_data = await fetch_all_pages_async(
                    token, api_url, http_method, chunk['start'], chunk['end'], 2, total_pages,
                    payload_template, page_size, data_key, max_retries, retry_delay
                )
                chunk_data.extend(remaining_data)
            
            all_data.extend(chunk_data)
            print(f"✓ Chunk {i} complete: {len(chunk_data)} records collected")
    else:
        # GET method or POST without date chunking - single fetch
        print(f"✓ No date chunking needed (GET method or no date range)\n")
        
        # Get total pages
        total_pages, first_page_data = get_total_pages(
            token, api_url, http_method, None, None, 
            payload_template, page_size, pages_key, data_key
        )
        
        all_data = first_page_data.copy()
        
        # Fetch remaining pages if needed
        if total_pages > 1:
            print(f"Fetching pages 2-{total_pages} asynchronously...")
            remaining_data = await fetch_all_pages_async(
                token, api_url, http_method, None, None, 2, total_pages,
                payload_template, page_size, data_key, max_retries, retry_delay
            )
            all_data.extend(remaining_data)
    
    print(f"\n{'='*60}")
    print(f"✓ ALL CHUNKS COMPLETE")
    print(f"✓ Total records collected: {len(all_data)}")
    print(f"{'='*60}\n")


    # ========================================
    #  Save raw JSON backup to Blob BEFORE DataFrame processing
    # ========================================
    # "/dbfs/mnt/radaruc/bronze/genesys/genesys/analytics/queue/delta_date=1900-01-01/"
    print("Saving raw JSON backup to Azure Blob Storage...")
    json_backup_path = save_raw_json_to_blob(
        data=all_data,
        TargetFileName=TargetFileName,
        base_path=TargetFilePath
    )
    
    return json_backup_path
print("✓ API ingestion function defined")

# COMMAND ----------

    
# ========================================
# CELL 5: collect api data and save to blob
# ========================================

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType

def process_api_data(read_path, config):
    """
    Bronze Layer: Create ONE raw table with JSON strings
    - Table name from config: raw_table_name
    - Columns: raw_json, load_timestamp, source_file
    - NO parsing, NO unnesting - just raw storage
    """
    raw_table_name = config.get("raw_table_name", "raw_data")
    
    print(f"\n{'='*60}")
    print(f"BRONZE LAYER: Creating raw table")
    print(f"{'='*60}")
    print(f"Raw table: {raw_table_name}")
    print(f"Reading from: {read_path}")
    
    # Remove /dbfs prefix for Spark
    read_path = read_path.replace('/dbfs', '')
    
    # Read JSONL file as text (each line = one JSON object)
    df_text = spark.read.text(read_path)
    
    # Create Bronze raw table with metadata
    df_bronze = df_text.select(
        col("value").alias("raw_json"),
        current_timestamp().alias("load_timestamp"),
        lit(read_path).alias("source_file")
    )
    
    row_count = df_bronze.count()
    
    print(f"✅ Bronze raw table created: {row_count} rows")
    print(f"   Schema: [raw_json: STRING, load_timestamp: TIMESTAMP, source_file: STRING]")
    
    # Return only ONE table for Bronze
    result_tables = {raw_table_name: df_bronze}
    
    print(f"\n✅ Bronze layer ready")
    print(f"   Table: {raw_table_name}")
    print(f"   Rows: {row_count}")
    print(f"   Note: Main/nested tables will be created in Silver layer")
    print(f"{'='*60}\n")
    
    return result_tables


# COMMAND ----------

# ========================================
# CELL 6.1: Execute Ingestion (Conditional - skip if backfill mode)
# ========================================

backfill_mode = int(dbutils.widgets.get("backfill_from_historical"))

if backfill_mode:
    print("="*60)
    print("BACKFILL MODE ENABLED - Skipping API ingestion")
    print("="*60)
    print("Will rebuild Silver tables from existing Bronze data")
    json_backup_path = None  # Not needed in backfill mode
else:
    print("Starting API ingestion framework...")
    json_backup_path = await ingest_api_data(config=API_CONFIG)
    
    print(f"\n{'='*60}")
    print(f"INGESTION COMPLETE")
    print(f"{'='*60}")

# COMMAND ----------

# ========================================
# CELL 6.2: Execute Bronze Processing (Conditional - skip if backfill mode)
# ========================================

backfill_mode = int(dbutils.widgets.get("backfill_from_historical"))

if backfill_mode:
    print("="*60)
    print("BACKFILL MODE - Skipping Bronze processing")
    print("="*60)
    result_tables = None  # Not needed in backfill mode
else:
    print("Starting Bronze layer processing...")
    result_tables = process_api_data(read_path=json_backup_path, config=API_CONFIG)
    
    print(f"Tables created: {len(result_tables)}")
    for table_name, df in result_tables.items():
        print(f"  • {table_name}: {df.count()} rows")
    print(f"{'='*60}\n")

# COMMAND ----------


# ========================================
# CELL 7: Display Results
# ========================================


# Display bronze table
if not backfill_mode:
    raw_table_name = API_CONFIG["raw_table_name"]
    print(f"Main Table: {raw_table_name}")
    print(f"Schema:")
    df_bronze = result_tables[raw_table_name]
    df_bronze.printSchema()
    print(f"\nSample data:")
    display(df_bronze.limit(10))


# COMMAND ----------


# # ========================================
# # CELL 8: Display Nested Tables
# # ========================================

# # Display each nested table
# for nested_config in API_CONFIG.get("nested_tables", []):
#     table_name = nested_config["unnested_to_table"]
    
#     if table_name in result_tables:
#         print(f"\n{'='*60}")
#         print(f"Nested Table: {table_name}")
#         print(f"{'='*60}")
#         print(f"Rows: {result_tables[table_name].count()}")
#         print(f"\nSchema:")
#         result_tables[table_name].printSchema()
#         print(f"\nSample data:")
#         display(result_tables[table_name].limit(10))

# COMMAND ----------

# ========================================
# CELL 9: Save to Delta Tables 
# ========================================

def save_bronze_to_delta(df_bronze, table_name, bronze_schema, mode="append"):
    """
    Save Bronze raw table to Delta
    Mode: 'append' for incremental loads (default)
          'overwrite' for full refresh
    """
    print(f"\n{'='*60}")
    print(f"SAVING BRONZE RAW TABLE")
    print(f"{'='*60}")
    
    table_full_name = f"{bronze_schema}.{table_name}"
    
    print(f"\nTable: {table_full_name}")
    print(f"  Mode: {mode}")
    print(f"  Rows to add: {df_bronze.count()}")
    
    # Check if table exists to show total count
    table_exists = spark.catalog.tableExists(table_full_name)
    
    if table_exists:
        existing_count = spark.table(table_full_name).count()
        print(f"  Existing rows: {existing_count}")
    
    # Save to Delta
    df_bronze.write.format("delta") \
        .mode(mode) \
        .option("mergeSchema", "false") \
        .saveAsTable(table_full_name)
    
    # Verify final count
    final_count = spark.table(table_full_name).count()
    print(f"  ✅ Final total rows: {final_count}")
    
    print(f"\n{'='*60}")
    print(f"✅ BRONZE RAW TABLE SAVED")
    print(f"{'='*60}\n")


# COMMAND ----------

# ===================================================================
# EXECUTE BRONZE APPEND (Conditional - skip if backfill mode)
# ===================================================================

backfill_mode = int(dbutils.widgets.get("backfill_from_historical"))

if backfill_mode:
    print("="*60)
    print("BACKFILL MODE - Skipping Bronze save")
    print("="*60)
else:
    bronze_schema = API_CONFIG['bronze_schema']
    raw_table_name = API_CONFIG['raw_table_name']
    save_bronze_to_delta(df_bronze, raw_table_name, bronze_schema, mode="append")

# COMMAND ----------

# ========================================
# CELL 11: SILVER LAYER – Smart Type Casting + Delta MERGE (Recommended)
# ========================================

from pyspark.sql.functions import from_json, col, lit, explode, to_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType, BooleanType,
    LongType, DoubleType, DecimalType, ArrayType
)
from delta.tables import DeltaTable


def enforce_schema_columns_silver(df, schema_dict, complex_as_json=True):
    """
    Enforce schema columns on DataFrame:
    - Add missing columns as NULL (with warning)
    - Warn about extra columns not in config
    - Cast scalar types to proper types
    - Convert complex types (array/struct) to JSON strings
    
    Args:
        df: DataFrame with parsed JSON
        schema_dict: data_schema from config
        complex_as_json: If True, convert array/struct to JSON strings (default)
    
    Returns:
        DataFrame with enforced schema
    """
    schema_cols = list(schema_dict.keys())
    existing_cols = df.columns
    
    # Identify missing and extra columns
    missing_cols = [c for c in schema_cols if c not in existing_cols]
    extra_cols = [c for c in existing_cols if c not in schema_cols]
    
    # Print warnings
    if missing_cols:
        print(f"\n⚠️  WARNING: {len(missing_cols)} column(s) missing from API response:")
        for col_name in missing_cols:
            print(f"     ✚ Adding '{col_name}' as NULL ({schema_dict[col_name]})")
    
    if extra_cols:
        print(f"\n⚠️  INFO: {len(extra_cols)} extra column(s) in API response (not in config):")
        for col_name in extra_cols[:10]:  # Show first 10
            print(f"     • {col_name}")
        if len(extra_cols) > 10:
            print(f"     ... and {len(extra_cols) - 10} more")
    
    # Build select expressions
    select_exprs = []
    
    for col_name, config_type in schema_dict.items():
        config_type_lower = config_type.strip().lower()
        
        # Handle missing columns - add as NULL with appropriate type
        if col_name not in existing_cols:
            if any(indicator in config_type_lower for indicator in ["array", "struct"]):
                # Complex type - add as NULL STRING (for JSON)
                select_exprs.append(lit(None).cast(StringType()).alias(col_name))
            elif "timestamp" in config_type_lower or "datetime" in config_type_lower:
                select_exprs.append(lit(None).cast(TimestampType()).alias(col_name))
            elif "date" in config_type_lower:
                select_exprs.append(lit(None).cast(DateType()).alias(col_name))
            elif "boolean" in config_type_lower:
                select_exprs.append(lit(None).cast(BooleanType()).alias(col_name))
            elif config_type_lower in ["int", "long"]:
                select_exprs.append(lit(None).cast(LongType()).alias(col_name))
            elif "double" in config_type_lower or "float" in config_type_lower:
                select_exprs.append(lit(None).cast(DoubleType()).alias(col_name))
            elif config_type_lower == "currency":
                select_exprs.append(lit(None).cast(DecimalType(20, 2)).alias(col_name))
            else:
                select_exprs.append(lit(None).cast(StringType()).alias(col_name))
            continue
        
        # Column exists in data - process based on type
        actual_type = df.schema[col_name].dataType
        
        # Check if it's a complex type that should become JSON string
        if complex_as_json and any(indicator in config_type_lower for indicator in ["array", "struct"]):
            if isinstance(actual_type, (ArrayType, StructType)):
                # Convert complex type to JSON string
                select_exprs.append(to_json(col(col_name)).alias(col_name))
                print(f"  📦 '{col_name}' → JSON string (config: {config_type})")
            else:
                # Already a string (shouldn't happen but handle gracefully)
                select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
        else:
            # Scalar type - cast to proper type
            if "timestamp" in config_type_lower or "datetime" in config_type_lower:
                select_exprs.append(col(col_name).cast(TimestampType()).alias(col_name))
            elif "date" in config_type_lower:
                select_exprs.append(col(col_name).cast(DateType()).alias(col_name))
            elif "boolean" in config_type_lower:
                select_exprs.append(col(col_name).cast(BooleanType()).alias(col_name))
            elif config_type_lower in ["int", "long"]:
                select_exprs.append(col(col_name).cast(LongType()).alias(col_name))
            elif "double" in config_type_lower or "float" in config_type_lower:
                select_exprs.append(col(col_name).cast(DoubleType()).alias(col_name))
            elif config_type_lower == "currency":
                select_exprs.append(col(col_name).cast(DecimalType(20, 2)).alias(col_name))
            else:
                select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
    
    # Select columns in config order
    df_final = df.select(*select_exprs)
    
    return df_final


def process_nested_table_from_bronze(df_bronze_parsed, nested_config):
    """
    Process nested table INDEPENDENTLY from already-parsed Bronze DataFrame
    - Each explosion level carries ONLY parent keys + column to explode
    - No unnecessary columns carried through
    - Supports parent_key as dict with types: {"key1": "string", "key2": "int"}
    
    Args:
        df_bronze_parsed: Already-parsed DataFrame from Bronze
        nested_config: Nested table configuration
    
    Returns:
        DataFrame with unnested data (includes all parent keys)
    """
    child_table_name = nested_config["unnested_to_table"]
    unnested_columns = nested_config["unnested_column"]  # LIST
    parent_keys_config = nested_config["parent_key"]  # Dict with types: {"conversationId": "string"}
    nested_schema_dict = nested_config["data_schema"]
    
    # 🆕 Extract parent keys and their types from dict
    if isinstance(parent_keys_config, dict):
        parent_keys = list(parent_keys_config.keys())
        parent_key_types = parent_keys_config  # {"conversationId": "string", "participantId": "string"}
    else:
        print(f"  ❌ ERROR: parent_key must be a dictionary with types, got: {type(parent_keys_config)}")
        return None
    
    print(f"\n{'='*60}")
    print(f"Processing nested table: {child_table_name}")
    print(f"  Explosion path: {' → '.join(unnested_columns)}")
    print(f"  Parent keys: {parent_keys}")
    print(f"  Parent key types: {parent_key_types}")
    print(f"{'='*60}")
    
    # CRITICAL: Start fresh from parsed Bronze
    df_current = df_bronze_parsed
    
    print(f"  📖 Starting with fresh DataFrame: {df_current.count()} records")
    
    # Track discovered parent keys at each level
    discovered_parent_keys = []
    
    # Step 1: Explode through multiple levels
    for level, column_to_explode in enumerate(unnested_columns, 1):
        print(f"\n  🔄 Level {level}: Exploding '{column_to_explode}'")
        
        # Check if column exists
        if column_to_explode not in df_current.columns:
            print(f"     ⚠️  Column '{column_to_explode}' not found, skipping table")
            return None
        
        # CRITICAL: Check if any parent keys already exist BEFORE first explosion
        if level == 1:
            for pk in parent_keys:
                if pk in df_current.columns and pk not in discovered_parent_keys:
                    discovered_parent_keys.append(pk)
                    print(f"     ✓ Parent key '{pk}' exists in source DataFrame")
        
        col_type = df_current.schema[column_to_explode].dataType
        sample_df = df_current.select(column_to_explode).filter(col(column_to_explode).isNotNull()).limit(10000)
        
        if sample_df.count() == 0:
            print(f"     ⚠️  No data in '{column_to_explode}' to explode")
            return None
        
        if isinstance(col_type, StringType):
            # JSON string - parse and explode
            print(f"     🔧 Parsing JSON string (using 10k sample for accuracy)...")
            inferred_schema = spark.read.json(sample_df.rdd.map(lambda row: row[0])).schema
            
            if isinstance(inferred_schema, StructType):
                array_schema = ArrayType(inferred_schema)
            else:
                array_schema = inferred_schema
            
            # Carry forward parent keys that exist in df_current
            keys_to_keep = [pk for pk in parent_keys if pk in df_current.columns]
            
            select_exprs = [col(pk) for pk in keys_to_keep]
            select_exprs.append(from_json(col(column_to_explode), array_schema).alias(f"{column_to_explode}_parsed"))
            
            df_with_array = df_current.select(*select_exprs)
            
            explode_exprs = [col(pk) for pk in keys_to_keep]
            explode_exprs.append(explode(col(f"{column_to_explode}_parsed")).alias(f"{column_to_explode}_exploded"))
            
            df_current = df_with_array.select(*explode_exprs)
            
        elif isinstance(col_type, ArrayType):
            # Already array - just explode
            print(f"     ✓ Already array type, exploding...")
            
            # Carry forward parent keys that exist in df_current
            keys_to_keep = [pk for pk in parent_keys if pk in df_current.columns]
            
            select_exprs = [col(pk) for pk in keys_to_keep]
            select_exprs.append(explode(col(column_to_explode)).alias(f"{column_to_explode}_exploded"))
            
            df_current = df_current.select(*select_exprs)
        else:
            print(f"     ❌ Unexpected type: {col_type}")
            return None
        
        # Flatten exploded struct
        exploded_col = f"{column_to_explode}_exploded"
        if exploded_col in df_current.columns:
            exploded_type = df_current.schema[exploded_col].dataType
            
            if isinstance(exploded_type, StructType):
                struct_fields = exploded_type.fields
                
                # Discover new parent keys from this struct
                for pk in parent_keys:
                    if pk in [f.name for f in struct_fields] and pk not in discovered_parent_keys:
                        discovered_parent_keys.append(pk)
                        print(f"     ✓ Found parent key '{pk}' at level {level}")
                
                # Build select: existing parent keys + flattened struct
                select_exprs = []
                
                # Keep parent keys from before this explosion
                for pk in discovered_parent_keys:
                    if pk in df_current.columns and pk != exploded_col:
                        select_exprs.append(col(pk))
                
                # Add flattened struct fields
                for field in struct_fields:
                    select_exprs.append(col(f"{exploded_col}.{field.name}").alias(field.name))
                
                df_current = df_current.select(*select_exprs)
                print(f"     ✓ Flattened {len(struct_fields)} fields from struct")
        
        current_count = df_current.count()
        current_cols = len(df_current.columns)
        print(f"     ✓ After explosion: {current_count} rows, {current_cols} columns")
    
    print(f"\n  ✅ Completed all {len(unnested_columns)} explosion levels")
    print(f"  ✅ Discovered parent keys: {discovered_parent_keys}")
    
    # Step 2: Verify all parent keys exist
    missing_parent_keys = [k for k in parent_keys if k not in df_current.columns]
    if missing_parent_keys:
        print(f"  ⚠️  WARNING: Parent keys not found in data: {missing_parent_keys}")
    
    # Step 3: Enforce schema on final result
    print(f"  🔧 Enforcing final schema...")
    
    # 🆕 Create extended schema: ALL parent keys (from config) + child columns
    extended_schema_dict = {}
    
    # Add all parent keys with their types FROM CONFIG (not from DataFrame)
    for pk, pk_type in parent_key_types.items():
        extended_schema_dict[pk] = pk_type
        if pk in df_current.columns:
            print(f"     ✓ Parent key '{pk}' exists, will cast to: {pk_type}")
        else:
            print(f"     ⚠️  Parent key '{pk}' missing, will add as NULL ({pk_type})")
    
    # Add child columns
    extended_schema_dict.update(nested_schema_dict)
    
    df_final = enforce_schema_columns_silver(df_current, extended_schema_dict, complex_as_json=True)
    
    final_count = df_final.count()
    print(f"  ✅ Final result: {len(df_final.columns)} columns, {final_count} rows")
    print(f"  ✅ Includes {len(parent_keys)} parent key(s): {parent_keys}")
    
    return df_final


def silver_transform_from_bronze(df_bronze, config: dict, target_tables: list = None):
    """
    Transform Bronze raw JSON to Silver structured tables
    
    Args:
        df_bronze: Bronze DataFrame with raw_json column
        config: API configuration
        target_tables: List of specific table names to process (None = process ALL tables)
    
    Returns:
        Dictionary of Silver DataFrames (only requested tables)
    """
    print(f"\n{'='*80}")
    print(f"SILVER LAYER TRANSFORMATION")
    print(f"{'='*80}")
    
    # 🆕 Show what tables will be processed
    if target_tables is None:
        print(f"🎯 Mode: Processing ALL tables")
    else:
        print(f"🎯 Mode: Processing ONLY specific tables: {target_tables}")
    
    bronze_count = df_bronze.count()
    print(f"✅ Bronze DF loaded: {bronze_count} rows")
    
    main_schema_dict = config["data_schema"]
    main_table_name = config["main_table_name"]
    
    print(f"\n🔧 Parsing raw JSON...")
    
    # Sample 10,000 rows for accurate schema inference (NO parallelize!)
    sample_df = df_bronze.select("raw_json").limit(10000)
    
    # Infer schema directly from DataFrame (NO parallelize!)
    inferred_schema = spark.read.json(sample_df.rdd.map(lambda row: row[0])).schema
    
    print(f"  ✅ Schema inferred from 10k sample rows")
    
    # Parse all raw_json using inferred schema
    df_main = df_bronze.select(
        from_json(col("raw_json"), inferred_schema).alias("parsed_data")
    ).select("parsed_data.*")
    
    records_count = df_main.count()
    print(f"  ✅ Parsed {records_count} conversation records")
    
    # Store result tables - only add main table if needed
    result_tables = {}
    
    # 🆕 Check if we should process main table
    if target_tables is None or main_table_name in target_tables:
        print(f"\n🔧 Enforcing schema on main table: {main_table_name}")
        df_main_silver = enforce_schema_columns_silver(df_main, main_schema_dict, complex_as_json=True)
        
        print(f"\n✅ Main table schema:")
        df_main_silver.printSchema()
        
        result_tables[main_table_name] = df_main_silver
        print(f"✅ Main table '{main_table_name}' processed")
    else:
        print(f"\n⏭️  Skipping main table '{main_table_name}' (not in target list)")
    
    # Process nested tables (ALL independently from Bronze)
    nested_tables_config = config.get("nested_tables", [])

    if nested_tables_config:
        print(f"\n{'='*80}")
        print(f"PROCESSING NESTED TABLES ({len(nested_tables_config)} tables)")
        print(f"{'='*80}")
        
        # Parse Bronze ONCE for ALL nested tables
        print(f"\n📖 Creating reusable parsed DataFrame from Bronze...")
        df_bronze_parsed = df_bronze.select(
            from_json(col("raw_json"), inferred_schema).alias("parsed_data")
        ).select("parsed_data.*")
        
        parsed_count = df_bronze_parsed.count()
        print(f"✅ Bronze parsed: {parsed_count} records")
        print(f"   This will be reused for nested table processing\n")
        
        # Process each nested table INDEPENDENTLY from Bronze
        for i, nested_config in enumerate(nested_tables_config, 1):
            child_table_name = nested_config["unnested_to_table"]
            
            # 🆕 Check if this table should be processed
            if target_tables is not None and child_table_name not in target_tables:
                print(f"\n[{i}/{len(nested_tables_config)}] ⏭️  Skipping: {child_table_name} (not in target list)")
                continue
            
            print(f"\n[{i}/{len(nested_tables_config)}] Processing: {child_table_name}")
            print(f"  🔗 Explosion path: {' → '.join(nested_config['unnested_column'])}")
            
            # Each table processes independently from Bronze
            df_nested = process_nested_table_from_bronze(df_bronze_parsed, nested_config)
            
            if df_nested is not None:
                result_tables[child_table_name] = df_nested
                nested_count = df_nested.count()
                print(f"  ✅ Created: {nested_count} rows")
            else:
                print(f"  ⚠️  Skipped (no data or column not found)")
        
        # 🆕 Show summary of what was actually processed
        processed_nested = [t for t in result_tables if t != main_table_name]
        if processed_nested:
            print(f"\n✅ Processed {len(processed_nested)} nested tables: {processed_nested}")
        else:
            print(f"\n⏭️  No nested tables were processed")
    
    print(f"\n{'='*80}")
    print(f"✅ SILVER TRANSFORMATION COMPLETE")
    print(f"  Total tables created: {len(result_tables)}")
    for table_name, df in result_tables.items():
        print(f"    • {table_name}: {df.count()} rows, {len(df.columns)} cols")
    print(f"{'='*80}\n")
    
    return result_tables


def silver_upsert_from_bronze(config: dict):
    """
    Complete Silver layer upsert:
    1. Transform Bronze raw → Silver DataFrames
    2. Merge into Silver Delta tables
    
    Supports backfill mode: rebuild tables from Bronze with date filtering
    """
    print(f"\n{'='*80}")
    print(f"SILVER LAYER UPSERT")
    print(f"{'='*80}")
    
    # Check if backfill mode
    backfill_mode = int(dbutils.widgets.get("backfill_from_historical"))
    backfill_table_name_raw = dbutils.widgets.get("backfill_table_name").strip()
    
    # 🆕 Parse backfill_table_name into list (supports "table1,table2,table3" or "table1, table2, table3")
    if backfill_table_name_raw:
        backfill_target_tables = [t.strip() for t in backfill_table_name_raw.split(",") if t.strip()]
        print(f"📋 Backfill target tables: {backfill_target_tables}")
    else:
        backfill_target_tables = None  # None = process ALL tables
    
    bronze_schema = config["bronze_schema"]
    silver_schema = config["silver_schema"]
    raw_table_name = config["raw_table_name"]
    main_table_name = config["main_table_name"]
    main_table_key = config["main_table_key"]
    
    bronze_table_full = f"{bronze_schema}.{raw_table_name}"
    
    # ========================================
    # Get or Read Bronze DataFrame
    # ========================================
    
    if backfill_mode:
        print(f"\n🔄 BACKFILL MODE ENABLED")
        datetime_start = dbutils.widgets.get("datetime_start").strip()
        datetime_end = dbutils.widgets.get("datetime_end").strip()
        print(f"  Date range: {datetime_start} to {datetime_end}")
        
        if backfill_target_tables:
            print(f"  Target tables: {backfill_target_tables}")
        else:
            print(f"  Target: ALL tables")
        
        # Read Bronze table (result_tables is None in backfill mode)
        print(f"\n  📖 Reading Bronze table: {bronze_table_full}")
        df_bronze = spark.table(bronze_table_full)
        
        # Filter by date if load_timestamp exists
        if "load_timestamp" in df_bronze.columns:
            df_bronze = df_bronze.filter(
                (col("load_timestamp") >= datetime_start) & 
                (col("load_timestamp") <= datetime_end)
            )
            print(f"  ✓ Filtered Bronze: {df_bronze.count()} rows in date range\n")
        else:
            print(f"  ⚠️  No load_timestamp column, using all Bronze data\n")
    else:
        print(f"\n🔧 API MODE - Using existing Bronze DataFrame")
        # Reuse df_bronze from result_tables (already in memory)
        # Get the DataFrame from the global scope
        try:
            df_bronze = result_tables[raw_table_name]
            print(f"  ✓ Reusing Bronze DF from result_tables: {df_bronze.count()} rows\n")
        except:
            # Fallback: read from table if result_tables not available
            print(f"  ⚠️  result_tables not found, reading from table")
            df_bronze = spark.table(bronze_table_full)
            print(f"  ✓ Read Bronze table: {df_bronze.count()} rows\n")
    
    # 🆕 Transform Bronze → Silver DataFrames (pass target_tables to process only needed tables)
    if backfill_mode and backfill_target_tables:
        # Backfill mode with specific tables - only process those tables
        print(f"\n🎯 Processing ONLY target tables: {backfill_target_tables}")
        silver_dfs = silver_transform_from_bronze(df_bronze, config, target_tables=backfill_target_tables)
        
        # Verify all requested tables were processed
        missing_tables = [t for t in backfill_target_tables if t not in silver_dfs]
        if missing_tables:
            print(f"\n⚠️  WARNING: These tables were not found/processed: {missing_tables}")
            print(f"   Successfully processed: {list(silver_dfs.keys())}")
    else:
        # Process ALL tables (backfill all OR API mode)
        print(f"\n🔄 Processing ALL tables")
        silver_dfs = silver_transform_from_bronze(df_bronze, config, target_tables=None)


    # ========================================
    # API mode
    # ========================================
    bronze_schema = config["bronze_schema"]
    silver_schema = config["silver_schema"]
    raw_table_name = config["raw_table_name"]
    main_table_name = config["main_table_name"]
    main_table_key = config["main_table_key"]
    
    bronze_table_full = f"{bronze_schema}.{raw_table_name}"
    
    # Transform Bronze → Silver DataFrames
    # silver_dfs = silver_transform_from_bronze(bronze_table_full, config)
    
    # ========================================
    # Upsert Main Table (only if it was processed)
    # ========================================
    if main_table_name in silver_dfs:
        main_silver_table = f"{silver_schema}.{main_table_name}"
        df_main_silver = silver_dfs[main_table_name]
        
        print(f"\n{'='*60}")
        print(f"Upserting Main Table: {main_silver_table}")
        print(f"  Primary key: {main_table_key}")
        print(f"{'='*60}")
    
        table_exists = spark.catalog.tableExists(main_silver_table)
        
        if not table_exists:
            print(f"✨ Creating new Silver table...")
            df_main_silver.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(main_silver_table)
            print(f"✅ Created: {main_silver_table}")
        else:
            print(f"🔄 Merging into existing table...")
            
            temp_view = "temp_main_silver"
            df_main_silver.createOrReplaceTempView(temp_view)
            
            spark.sql(f"""
                MERGE INTO {main_silver_table} AS target
                USING {temp_view} AS source
                ON target.{main_table_key} = source.{main_table_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            print(f"✅ Merged: {main_silver_table}")
        
        final_count = spark.table(main_silver_table).count()
        print(f"   📊 Total rows: {final_count}")

    else:
        print(f"\n⏭️  Skipping main table upsert (not in processed tables)")
        
    # ========================================
    # Upsert Nested Tables
    # ========================================
    nested_tables_config = config.get("nested_tables", [])
    
    if nested_tables_config:
        print(f"\n{'='*60}")
        print(f"UPSERTING NESTED TABLES")
        print(f"{'='*60}")
        
        for i, nested_config in enumerate(nested_tables_config, 1):
            child_table_name = nested_config["unnested_to_table"]
            
            if child_table_name not in silver_dfs:
                print(f"\n[{i}/{len(nested_tables_config)}] ⚠️  Skipping '{child_table_name}' (not in transformed data)")
                continue
            
            df_nested = silver_dfs[child_table_name]
            silver_nested_table = f"{silver_schema}.{child_table_name}"
            
            parent_keys_config = nested_config["parent_key"]  # Dict: {"conversationId": "string"}
            child_keys = nested_config["unnested_to_table_key"]
            
            # 🆕 Extract parent keys list from dict
            if isinstance(parent_keys_config, dict):
                parent_keys = list(parent_keys_config.keys())
            else:
                print(f"  ❌ ERROR: parent_key must be dict, got {type(parent_keys_config)}")
                continue
            
            print(f"\n[{i}/{len(nested_tables_config)}] Processing: {silver_nested_table}")
            print(f"  Parent keys: {parent_keys}")
            print(f"  Parent key types from config: {parent_keys_config}")
            print(f"  Child keys: {child_keys}")
            
            # 🆕 Parent key types already enforced in process_nested_table_from_bronze()
            # No need to cast again here - the DataFrame already has correct types
            print(f"  ✓ Parent key types already enforced during transformation")
            
            nested_exists = spark.catalog.tableExists(silver_nested_table)
            
            if not nested_exists:
                print(f"  ✨ Creating new nested table...")
                df_nested.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(silver_nested_table)
                print(f"  ✅ Created: {silver_nested_table}")
            else:
                print(f"  🔄 Merging into existing table...")
                
                temp_view = f"temp_{child_table_name}"
                df_nested.createOrReplaceTempView(temp_view)
                
                # Build ON clause with ALL parent keys + child keys
                on_conditions = []
                
                # Add all parent keys
                for pk in parent_keys:
                    on_conditions.append(f"target.{pk} = source.{pk}")
                
                # Add child-specific keys
                for key in child_keys:
                    on_conditions.append(f"target.{key} = source.{key}")
                
                on_clause = " AND ".join(on_conditions)
                
                print(f"  ON clause: {on_clause}")  # Show first 100 chars
                display(df_nested.limit(10))

                spark.sql(f"""
                    MERGE INTO {silver_nested_table} AS target
                    USING {temp_view} AS source
                    ON {on_clause}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
                
                print(f"  ✅ Merged: {silver_nested_table}")
            
            final_count = spark.table(silver_nested_table).count()
            print(f"     📊 Total rows: {final_count}")
    
    print(f"\n{'='*80}")
    print(f"✅ SILVER LAYER UPSERT COMPLETE")
    print(f"{'='*80}")
    print(f"📋 Summary:")
    print(f"  • Bronze: 1 raw table with JSON strings (append-only)")
    print(f"  • Silver: {len(silver_dfs)} structured tables")
    print(f"  • Scalar types: Cast to proper types")
    print(f"  • Complex types: Stored as JSON strings")
    print(f"  • Missing columns: Added as NULL with warnings")
    print(f"  • Schema evolution: Fully supported")
    print(f"{'='*80}\n")

# COMMAND ----------

# ===================================================================
# EXECUTE SILVER UPSERT (Run after your bronze processing)
# ===================================================================
silver_upsert_from_bronze(API_CONFIG)

# COMMAND ----------


# ========================================
# CELL 12: Query All Silver Tables
# ========================================

silver_schema = API_CONFIG["silver_schema"]
main_table_name = API_CONFIG["main_table_name"]

print(f"\n{'='*80}")
print(f"QUERYING ALL SILVER TABLES")
print(f"{'='*80}\n")

# Query main table
main_table_full = f"{silver_schema}.{main_table_name}"
print(f"{'='*60}")
print(f"MAIN TABLE: {main_table_full}")
print(f"{'='*60}")

df_main = spark.table(main_table_full)
print(f"Rows: {df_main.count()}")
print(f"Columns: {len(df_main.columns)}")
print(f"\nSchema:")
df_main.printSchema()
print(f"\nSample Data:")
display(df_main.limit(10))

# Query all nested tables
nested_tables_config = API_CONFIG.get("nested_tables", [])

if nested_tables_config:
    for i, nested_config in enumerate(nested_tables_config, 1):
        nested_table_name = nested_config["unnested_to_table"]
        nested_table_full = f"{silver_schema}.{nested_table_name}"
        
        print(f"\n{'='*60}")
        print(f"NESTED TABLE {i}: {nested_table_full}")
        print(f"{'='*60}")
        
        df_nested = spark.table(nested_table_full)
        print(f"Rows: {df_nested.count()}")
        print(f"Columns: {len(df_nested.columns)}")
        # print(f"Parent Table: {nested_config['nested_from_table']}")
        print(f"Unnested Column: {nested_config['unnested_column']}")
        print(f"Parent Key: {nested_config['parent_key']}")
        print(f"Child Keys: {nested_config['unnested_to_table_key']}")
        print(f"\nSchema:")
        df_nested.printSchema()
        print(f"\nSample Data:")
        display(df_nested.limit(10))

print(f"\n{'='*80}")
print(f"✅ ALL SILVER TABLES DISPLAYED")
print(f"{'='*80}")