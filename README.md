# ❄️ Snowflake Data Sampling & Export

Automated SQL scripts to filter, sample, and export **ICB-BERT OCP Dataset** records to the Data Science stage.

## ⚡ Quick Start
These scripts use **Anonymous Blocks**. Just copy, paste into a Snowflake Worksheet, and run.

* **Option A:** Exports samples for **ALL** merchants.
* **Option B:** Exports samples for a **specific list** of merchants.

> **Note:** Columns are automatically renamed during export:
> `GROUND_TRUTH_NETWORK_ORDER_NUMBER` → `ground_truth_order_id`
> `GROUND_TRUTH_NETWORK_SUBTOTAL` → `ground_truth_subtotal`
> `SANITIZED_DOM` → `sanitized_text`
