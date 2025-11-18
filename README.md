# Snowflake_Jobs_ICB_BERT

Snowflake jobs that export the latest, high-quality DOM samples for the ICB-BERT model, partitioned by merchant and timestamp.

## Jobs

- **All merchants**  
  - Top 50 latest rows per merchant  
  - Output: `@DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT_ALL/run_<timestamp>/`

- **Selected merchants**  
  - Top 10 latest rows per provided merchants (configurable)  
  - Output: `@DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT/run_<timestamp>/`

## Usage

```sql
CALL SP_ICB_BERT_EXPORT_ALL_MERCHANTS();
CALL SP_ICB_BERT_EXPORT_SELECTED_MERCHANTS('5246,8333,...', 10);
SHOW TASKS LIKE '%ICB_BERT%';
