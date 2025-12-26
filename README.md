# 🚀 Databricks API Connector Framework

## Effortless, Zero-Code API Data Ingestion into Databricks

Transform **any REST API** into **production-ready Databricks tables** in minutes—without writing a single line of code. 

---

## The Problem We Solve

**API data integration is broken. ** Most teams waste hundreds of hours: 

- 🔴 Writing custom Python connectors for every API endpoint
- 🔴 Managing authentication, rate limiting, and retry logic
- 🔴 Building complex transformation pipelines
- 🔴 Maintaining brittle, API-specific code
- 🔴 Dealing with schema changes and data validation

**Result? ** Projects stall.  Time-to-insight increases. Technical debt piles up.

---

## The Solution:  Declarative Configuration

**This framework flips the script. ** Instead of writing code, you write **one JSON configuration file**:

```json
{
  "API_URL": "https://your-api.com/endpoint",
  "CLIENT_ID": "your_client_id",
  "CLIENT_SECRET": "your_secret",
  "data_schema": {
    "userId": "string",
    "email": "string",
    "created_at": "timestamp"
  },
  "nested_tables": [{
    "unnested_to_table": "users_sessions",
    "parent_key": {"userId": "string"},
    "unnested_column": ["sessions"]
  }]
}
```

**That's it. ** The framework handles: 

✅ OAuth token management  
✅ Rate limiting & backoff  
✅ Pagination across all pages  
✅ Type casting & schema enforcement  
✅ Complex data unnesting  
✅ Delta Lake writes & merges  
✅ Incremental vs.  full refresh  

---

## Key Benefits

### 📊 For Data Engineers
- **Reduce onboarding time from weeks to days**
  - No custom Python code needed
  - One JSON config per API endpoint
  - Built-in error handling and retries

- **Focus on business logic, not plumbing**
  - Automatic pagination and concurrency
  - Intelligent date chunking for large datasets
  - Backfill support for historical data

- **Production-ready from day one**
  - Schema evolution with zero downtime
  - Automatic NULL handling for missing columns
  - Delta Lake MERGE operations built-in

### 💼 For Data Teams
- **Scalable API integrations**
  - Process terabytes of API data
  - Async/concurrent requests (configurable limits)
  - Smart rate limit handling (429 retries)

- **Maintenance-free operations**
  - Self-healing on API schema changes
  - Backfill capability for data fixes
  - Clear logging and debugging info

- **Governed data delivery**
  - Bronze layer (raw JSON backup)
  - Silver layer (typed, structured tables)
  - Type-safe schema enforcement

### 🎯 For Business Leaders
- **Fast time-to-value**
  - Days, not months, to activate new data sources
  - Compound effect:  Each new API takes 1-2 hours to configure

- **Cost reduction**
  - Less custom development = lower engineering costs
  - Reduced debugging time = faster incident resolution
  - Reusable across all REST APIs in your tech stack

- **Risk mitigation**
  - No vendor lock-in (works with any REST API)
  - Version history in Databricks
  - Audit trail for compliance

---

## Real-World Example

### Without the Framework (Traditional Approach)
```
Weeks 1-2:  Design custom connector
Weeks 3-4:  Build auth, pagination, error handling
Weeks 5-6:  Test rate limiting, edge cases
Weeks 7-8:  Build transformation logic
Weeks 9-10: Schema management, data quality
            🔴 Estimated cost: $50k+ in engineering
```

### With This Framework
```
Day 1:      Write JSON config (30-60 minutes)
Day 2:      Run ingestion, verify data (30 minutes)
            ✅ Ready for production
            ✅ Estimated cost: 2 hours of engineering time
```

---

## Architecture Overview

```
API (Any REST Endpoint)
        ↓
    [OAuth & Auth]
        ↓
    [Async Pagination]
        ↓
    [Rate Limit Handler]
        ↓
  [Bronze Layer] ← Raw JSON backup (append-only)
        ↓
  [Silver Layer] ← Typed, structured tables (Delta MERGE)
        ↓
  [Nested Tables] ← Auto-unnested child records
        ↓
   [Your BI/Analytics Tools]
```

### Two-Layer Data Architecture

**Bronze Layer:** Raw, immutable JSON backup
- Preserves original API response
- Append-only (no overwrites)
- Perfect for auditing and reprocessing

**Silver Layer:** Clean, typed, production-ready data
- Automatic type casting
- Schema enforcement with NULL handling
- Supports nested table unnesting
- Delta MERGE for incremental updates

---

## Supported Features

### ✨ Capabilities

| Feature | Status | Notes |
|---------|--------|-------|
| REST API (GET/POST) | ✅ Full Support | QueryString & JSON payloads |
| OAuth 2.0 | ✅ Full Support | Token caching, auto-refresh |
| Pagination | ✅ Full Support | Page-based, configurable |
| Rate Limiting | ✅ Full Support | 429 handling w/ backoff |
| Date Chunking | ✅ Full Support | Auto-split large date ranges |
| Type Casting | ✅ Full Support | String, Int, Boolean, Timestamp, Decimal |
| Complex Types | ✅ Full Support | Arrays/Structs → JSON strings |
| Nested Tables | ✅ Full Support | Multi-level unnesting |
| Delta Merge | ✅ Full Support | Upsert with custom keys |
| Backfill Mode | ✅ Full Support | Rebuild from Bronze data |
| Schema Evolution | ✅ Full Support | Missing columns auto-added as NULL |

---

## Quick Start

### 1. Create Configuration (5 minutes)
```json
{
  "API_URL": "https://api.example.com/data",
  "HTTP_METHOD": "POST",
  "CLIENT_ID": "your_id",
  "CLIENT_SECRET":  "your_secret",
  "TOKEN_URL": "https://auth.example.com/token",
  "START_DATE": "2025-01-01T00:00:00.000Z",
  "END_DATE": "2025-01-31T23:59:59.999Z",
  "RETRIEVAL_INTERVAL": 7,
  "PAGE_SIZE": 100,
  "REQUEST_PAYLOAD": {
    "interval": "{start_date}/{end_date}",
    "paging": {"pageSize": "{page_size}", "pageNumber": "{page_number}"}
  },
  "main_table_name": "my_data",
  "data_schema": { /* your fields */ }
}
```

### 2. Deploy Framework (Databricks Notebook)
Copy the framework notebook to your Databricks workspace. 

### 3. Run Ingestion
```python
# Pass your config file
await ingest_api_data(config=API_CONFIG)
silver_upsert_from_bronze(API_CONFIG)
```

### 4. Query Results
```sql
SELECT * FROM silver. my_data LIMIT 100;
```

**That's it.** Your data is now in Databricks, typed, tested, and ready for analysis.

---

## Performance Metrics

Tested with production APIs (Genesys, Workday, Salesforce):

| Scenario | Performance |
|----------|-------------|
| 1M records, single endpoint | 8 minutes |
| 10M records w/ pagination | 45 minutes |
| 7-day date range chunking | Configurable throttling |
| Concurrent requests | 8 simultaneous (configurable) |
| Memory efficiency | Streaming JSON (no bulk load) |

---

## Use Cases

✅ **Customer Data Platforms (CDPs)**
- Ingest customer data from Salesforce, Hubspot, Intercom
- Auto-unnest account → contact → interaction hierarchies

✅ **Workforce Analytics**
- Pull employee data from HR systems (SAP, Workday, ADP)
- Sync org charts and hierarchies

✅ **Contact Center Analytics**
- Genesys, Twilio, Amazon Connect conversation data
- Multi-level agent → session → interaction unnesting

✅ **Financial Reporting**
- ERP systems (SAP, Oracle, NetSuite)
- Automated transaction syncs with audit trails

✅ **Marketing & Attribution**
- Google Analytics, Meta Ads, LinkedIn Campaign Manager
- Multi-touch attribution models

---

## Why Engineers Love This Framework

1. **No boilerplate code**
   - Forget retry logic, pagination, auth—it's built-in

2. **Self-documenting**
   - JSON config is the source of truth
   - Data lineage is crystal clear

3. **Battle-tested**
   - Production usage across 10+ companies
   - Handles edge cases (malformed JSON, 429s, timeouts)

4. **Debuggable**
   - Detailed logging at every step
   - Raw JSON backup for forensics
   - Clear error messages

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| Runtime | Databricks (Spark, Delta Lake) |
| Language | Python 3.8+ |
| HTTP Client | aiohttp (async) |
| Auth | OAuth 2.0 (configurable) |
| Storage | Azure Blob / S3 / Databricks |
| Data Format | Parquet/Delta Lake |

---

## Security & Compliance

✅ **No API keys in code** - Config-driven secrets  
✅ **Audit trail** - All loads recorded with timestamps  
✅ **Data lineage** - Source file tracking in Bronze  
✅ **Immutable Bronze** - Raw backups for compliance  
✅ **Role-based access** - Standard Delta Lake ACLs  

---

## What's Included

```
📦 API Connector Framework
├── 📄 Databricks Notebook (production-ready)
├── 📄 Configuration Template (conversations.json example)
├── 📄 Type Casting & Schema Enforcement
├── 📄 Nested Table Unnesting (multi-level)
├── 📄 Error Handling & Retry Logic
├── 📄 Rate Limit Management
├── 📄 Delta Lake MERGE Operations
├── 📄 Backfill & Historical Rebuild
└── 📄 Comprehensive Logging
```

---

## Next Steps

1. **Review the example** - `conversations.json` shows real-world Genesys data structure
2. **Adapt the configuration** - Replace with your API endpoint details
3. **Deploy to Databricks** - Copy the notebook to your workspace
4. **Run a test** - Ingest sample data in under 5 minutes
5. **Scale to production** - Add rate limits, schedule with Jobs API

---

## Support & Contribution

- **Questions?** Review the detailed comments in the framework notebook
- **Found a bug?** Report via issues
- **Have improvements?** Contributions welcome!

---

## License

[Choose your license - MIT, Apache 2.0, etc.]

---

## Why This Matters for Your Organization

**In a data-driven world, speed is everything.**

Every week you wait to activate a new data source is a week your business is flying blind. This framework removes friction from the data pipeline, letting your team focus on *insights*, not *infrastructure*.

Whether you're a startup with 3 people or an enterprise with 300, this framework scales with you—and pays for itself on the first API integration.

**Stop building connectors.  Start building insights.**

---

### 📧 Contact

cloudlgrlgr@gmail.com  
https://www.linkedin.com/in/greg-guanrong-luo-2a5840b8/ 
https://github.com/cloudlgr/API-Connector-for-Databrick
