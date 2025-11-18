# Snowflake_Jobs_ICB_BERT

Snowflake jobs that generate clean, merchant-partitioned DOM samples for the ICB-BERT training pipeline.  
Each run selects the latest, high-quality ticket data and exports it as CSV to a Snowflake stage, organized by merchant and timestamp.

---

## Jobs

### 1. All Merchants (Top 50 / Merchant)
- Scope: all merchants in `ASSET.AI_OCP_DATASET`.
- Logic:
  - Filter for non-null ground truth and DOM fields.
  - Keep the latest row per ticket (ROW_NUMBER over MERCHANT_ID + TRACKING_TICKET_NUMBER).
  - Take up to 50 most recent samples per merchant.
- Output:
  - `@DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT_ALL/run_<timestamp>/`
  - Partitioned by `MERCHANT_ID` (one folder per merchant).

### 2. Selected Merchants (Top 10 / Merchant)
- Scope: only merchants you pass to the stored procedure.
- Logic:
  - Uses the same filters and latest-per-ticket dedupe as the original query.
  - Takes up to 10 most recent samples per merchant (configurable).
- Output:
  - `@DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT/run_<timestamp>/`
  - Partitioned by `MERCHANT_ID`.

---

## Usage

Run manually:

```sql
CALL SP_ICB_BERT_EXPORT_ALL_MERCHANTS();
CALL SP_ICB_BERT_EXPORT_SELECTED_MERCHANTS('5246,8333,7206,...', 10);
