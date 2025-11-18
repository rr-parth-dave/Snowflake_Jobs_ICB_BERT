USE DATABASE EBATES_PROD;

EXECUTE IMMEDIATE $$
DECLARE
    ---------------------------------------------------------
    -- CONFIGURATION SECTION
    ---------------------------------------------------------
    -- 1. How many samples do you want per merchant? (e.g., 50, 100, 300)
    v_sample_size INT := 50;
    
    -- 2. Temp table name (Safe to leave as is)
    v_temp_table STRING := 'TEMP.TEST_TABLE_JOB_ALL';

    ---------------------------------------------------------
    -- INTERNAL VARIABLES
    ---------------------------------------------------------
    v_run_ts STRING;
    v_prep_sql STRING;
    v_copy_sql STRING;
    
BEGIN
    -- Generate Timestamp
    v_run_ts := TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- Build the CREATE TABLE query (No Merchant Filter)
    v_prep_sql := '
    CREATE OR REPLACE TABLE ' || v_temp_table || ' AS
    WITH latest_per_ticket_keys AS (
        SELECT
            d."MERCHANT_ID",
            d."TRACKING_TICKET_NUMBER",
            d."UPDATE_TIMESTAMP"
        FROM ASSET.AI_OCP_DATASET d
        WHERE d."GROUND_TRUTH_NETWORK_ORDER_NUMBER" IS NOT NULL
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
        -- This limits the rows for EVERY merchant found in the data
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY k."MERCHANT_ID"
            ORDER BY k."UPDATE_TIMESTAMP" DESC
        ) <= ' || v_sample_size || '
    )
    SELECT
        d."TRACKING_TICKET_NUMBER",
        d."UPDATE_TIMESTAMP",
        d."MERCHANT_ID",
        d."GROUND_TRUTH_NETWORK_ORDER_NUMBER",
        d."GROUND_TRUTH_NETWORK_SUBTOTAL",
        d."RAW_DOM",
        d."SANITIZED_DOM"
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

    -- Build the COPY INTO statement
    v_copy_sql := '
    COPY INTO @DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT/run_ALL_' || v_run_ts || '/
    FROM (
        SELECT TRACKING_TICKET_NUMBER,
               MERCHANT_ID,
               GROUND_TRUTH_NETWORK_ORDER_NUMBER,
               GROUND_TRUTH_NETWORK_SUBTOTAL,
               RAW_DOM,
               SANITIZED_DOM,
               UPDATE_TIMESTAMP
        FROM ' || v_temp_table || '
    )
    PARTITION BY (TO_VARCHAR(MERCHANT_ID))
    FILE_FORMAT = (
        TYPE = CSV
        COMPRESSION = NONE
        FIELD_DELIMITER = '',''
        FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
    )
    MAX_FILE_SIZE = 100000000
    ';

    EXECUTE IMMEDIATE v_copy_sql;

    RETURN 'Success: Exported ' || v_sample_size || ' samples for ALL available merchants to folder run_ALL_' || v_run_ts;
END;
$$;
