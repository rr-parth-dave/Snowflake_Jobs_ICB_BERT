USE DATABASE EBATES_PROD;

EXECUTE IMMEDIATE $$
DECLARE
    -- CONFIGURATION
    v_merchant_list STRING := '5246, 8333, 7206, 8378, 4548, 10086, 4207';
    v_sample_size INT := 50;
    v_temp_table STRING := 'TEMP.TEST_TABLE_JOB_SPECIFIC';

    -- INTERNAL VARS
    v_run_ts STRING;
    v_prep_sql STRING;
    v_copy_sql STRING;
    
BEGIN
    v_run_ts := TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- 1. CREATE TABLE: We apply the renaming (AS) here
    v_prep_sql := '
    CREATE OR REPLACE TABLE ' || v_temp_table || ' AS
    WITH latest_per_ticket_keys AS (
        SELECT
            d."MERCHANT_ID",
            d."TRACKING_TICKET_NUMBER",
            d."UPDATE_TIMESTAMP"
        FROM ASSET.AI_OCP_DATASET d
        WHERE d."MERCHANT_ID" IN (' || v_merchant_list || ')
          AND d."GROUND_TRUTH_NETWORK_ORDER_NUMBER" IS NOT NULL
          AND d."GROUND_TRUTH_NETWORK_SUBTOTAL" IS NOT NULL
          AND d."RAW_DOM" IS NOT NULL
          AND d."UPDATE_TIMESTAMP" IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY d."MERCHANT_ID", d."TRACKING_TICKET_NUMBER"
            ORDER BY d."UPDATE_TIMESTAMP" DESC
        ) = 1
    ),
    top_keys AS (
        SELECT
            k."MERCHANT_ID",
            k."TRACKING_TICKET_NUMBER",
            k."UPDATE_TIMESTAMP"
        FROM latest_per_ticket_keys k
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY k."MERCHANT_ID"
            ORDER BY k."UPDATE_TIMESTAMP" DESC
        ) <= ' || v_sample_size || '
    )
    SELECT
        d."TRACKING_TICKET_NUMBER",
        d."UPDATE_TIMESTAMP",
        d."MERCHANT_ID",
        d."RAW_DOM",
        -- RENAMING HAPPENS HERE:
        d."GROUND_TRUTH_NETWORK_ORDER_NUMBER" AS ground_truth_order_id,
        d."GROUND_TRUTH_NETWORK_SUBTOTAL"     AS ground_truth_subtotal,
        d."SANITIZED_DOM"                     AS sanitized_text
    FROM ASSET.AI_OCP_DATASET d
    JOIN top_keys t
        ON d."MERCHANT_ID" = t."MERCHANT_ID"
        AND d."TRACKING_TICKET_NUMBER" = t."TRACKING_TICKET_NUMBER"
        AND d."UPDATE_TIMESTAMP" = t."UPDATE_TIMESTAMP"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY d."MERCHANT_ID", d."TRACKING_TICKET_NUMBER"
        ORDER BY d."UPDATE_TIMESTAMP" DESC
    ) = 1
    ORDER BY d."MERCHANT_ID", d."UPDATE_TIMESTAMP" DESC
    ';

    EXECUTE IMMEDIATE v_prep_sql;

    -- 2. EXPORT: We select the NEW names here
    v_copy_sql := '
    COPY INTO @DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT/run_LIST_' || v_run_ts || '/
    FROM (
        SELECT 
               TRACKING_TICKET_NUMBER,
               MERCHANT_ID,
               RAW_DOM,
               UPDATE_TIMESTAMP,
               -- Select the new names
               ground_truth_order_id,
               ground_truth_subtotal,
               sanitized_text
        FROM ' || v_temp_table || '
    )
    PARTITION BY (TO_VARCHAR(MERCHANT_ID))
    FILE_FORMAT = (
        TYPE = CSV
        COMPRESSION = NONE
        FIELD_DELIMITER = '',''
        FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
        HEADER = TRUE 
    )
    MAX_FILE_SIZE = 100000000
    ';

    EXECUTE IMMEDIATE v_copy_sql;

    RETURN 'Success: Exported ' || v_sample_size || ' samples for Merchant List (Renamed) to run_LIST_' || v_run_ts;
END;
$$;
