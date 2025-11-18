USE DATABASE EBATES_PROD;

EXECUTE IMMEDIATE $$
DECLARE
    ---------------------------------------------------------
    -- CONFIGURATION SECTION
    ---------------------------------------------------------
    -- 1. Your specific list of Merchant IDs
    v_merchant_list STRING := '5246, 8333, 7206, 8378, 4548, 10086, 4207, 3993, 16788, 10722, 5487, 896, 13779, 2423, 10752, 4646, 12083, 8850, 13349, 3930, 3866, 98, 4227, 9376, 10199, 8084, 10437, 4767, 8303, 10481, 9142, 13505, 10764, 6646, 16450, 3726, 9800, 9728, 8933, 10151, 12028, 3509, 9265, 13499, 9528, 19013, 9388, 8257';
    
    -- 2. How many samples do you want per merchant?
    v_sample_size INT := 50;
    
    -- 3. Temp table name
    v_temp_table STRING := 'TEMP.TEST_TABLE_JOB_SPECIFIC';

    ---------------------------------------------------------
    -- INTERNAL VARIABLES
    ---------------------------------------------------------
    v_run_ts STRING;
    v_prep_sql STRING;
    v_copy_sql STRING;
    
BEGIN
    -- Generate Timestamp
    v_run_ts := TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- Build the CREATE TABLE query (WITH Merchant Filter)
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
    COPY INTO @DA_ASSET.DS_FILE_DROP_STAGE/ICB_BERT/run_LIST_' || v_run_ts || '/
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

    RETURN 'Success: Exported ' || v_sample_size || ' samples for Merchant List to folder run_LIST_' || v_run_ts;
END;
$$;
