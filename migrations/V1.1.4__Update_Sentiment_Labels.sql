-- V1.1.4__Update_Sentiment_Labels.sql
-- Refine news sentiment labels to use a 5-category Fear & Greed scale

-- 1. Update the metadata comment for the table
COMMENT ON COLUMN ANALYTICS.NEWS_SENTIMENT.SENTIMENT_LABEL IS 'Sentiment category: EXTREMELY FEAR, FEAR, NEUTRAL, GREED, EXTREMELY GREED';

-- 2. Update the stored procedure for sentiment analysis to use refined labels
CREATE OR REPLACE PROCEDURE ANALYTICS.ANALYZE_NEWS_SENTIMENT()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INT DEFAULT 0;
BEGIN
    -- Insert new articles with refined sentiment analysis labels
    INSERT INTO ANALYTICS.NEWS_SENTIMENT (
        ID,
        PUBLISHED_ON,
        PUBLISHED_DATETIME,
        TITLE,
        SOURCE,
        BODY,
        SENTIMENT_SCORE,
        SENTIMENT_LABEL,
        TITLE_SENTIMENT_SCORE
    )
    SELECT 
        n.ID,
        n.PUBLISHED_ON,
        TO_TIMESTAMP(n.PUBLISHED_ON),
        n.TITLE,
        n.SOURCE,
        n.BODY,
        -- Cortex sentiment analysis on body text
        SNOWFLAKE.CORTEX.SENTIMENT(n.BODY),
        -- Classify sentiment into 5 categories
        CASE 
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) <= -0.6 THEN 'EXTREMELY FEAR'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) <= -0.1 THEN 'FEAR'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) < 0.1 THEN 'NEUTRAL'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) < 0.6 THEN 'GREED'
            ELSE 'EXTREMELY GREED'
        END,
        -- Sentiment of title only
        SNOWFLAKE.CORTEX.SENTIMENT(n.TITLE)
    FROM ANALYTICS.NEWS_STREAM n
    WHERE METADATA$ACTION = 'INSERT'
    AND METADATA$ISUPDATE = FALSE;
    
    rows_processed := SQLROWCOUNT;
    
    RETURN 'Processed ' || rows_processed || ' news articles with refined labels';
END;
$$;

-- 3. Update existing records in NEWS_SENTIMENT with the new labels
UPDATE ANALYTICS.NEWS_SENTIMENT
SET SENTIMENT_LABEL = CASE 
    WHEN SENTIMENT_SCORE <= -0.6 THEN 'EXTREMELY FEAR'
    WHEN SENTIMENT_SCORE <= -0.1 THEN 'FEAR'
    WHEN SENTIMENT_SCORE < 0.1 THEN 'NEUTRAL'
    WHEN SENTIMENT_SCORE < 0.6 THEN 'GREED'
    ELSE 'EXTREMELY GREED'
END;

-- 4. Update the aggregation procedures to reflect new labels in counts
CREATE OR REPLACE PROCEDURE ANALYTICS.CALCULATE_HOURLY_FNG_INDEX()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_calculated INT DEFAULT 0;
BEGIN
    -- Calculate hourly aggregations with refined label counts
    MERGE INTO ANALYTICS.FNG_NEWS_INDEX_HOURLY AS target
    USING (
        SELECT 
            DATE_TRUNC('HOUR', PUBLISHED_DATETIME) AS HOUR_START,
            COUNT(*) AS TOTAL_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL IN ('GREED', 'EXTREMELY GREED') THEN 1 ELSE 0 END) AS POSITIVE_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END) AS NEUTRAL_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL IN ('FEAR', 'EXTREMELY FEAR') THEN 1 ELSE 0 END) AS NEGATIVE_ARTICLES,
            AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE,
            ((AVG(SENTIMENT_SCORE) + 1) / 2) * 100 AS FNG_INDEX_VALUE
        FROM ANALYTICS.NEWS_SENTIMENT
        WHERE PUBLISHED_DATETIME >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
        GROUP BY DATE_TRUNC('HOUR', PUBLISHED_DATETIME)
    ) AS source
    ON target.HOUR_START = source.HOUR_START
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_ARTICLES = source.TOTAL_ARTICLES,
        target.POSITIVE_ARTICLES = source.POSITIVE_ARTICLES,
        target.NEUTRAL_ARTICLES = source.NEUTRAL_ARTICLES,
        target.NEGATIVE_ARTICLES = source.NEGATIVE_ARTICLES,
        target.AVG_SENTIMENT_SCORE = source.AVG_SENTIMENT_SCORE,
        target.FNG_INDEX_VALUE = source.FNG_INDEX_VALUE,
        target.FNG_INDEX_LABEL = CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        target.CALCULATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        HOUR_START,
        TOTAL_ARTICLES,
        POSITIVE_ARTICLES,
        NEUTRAL_ARTICLES,
        NEGATIVE_ARTICLES,
        AVG_SENTIMENT_SCORE,
        FNG_INDEX_VALUE,
        FNG_INDEX_LABEL
    ) VALUES (
        source.HOUR_START,
        source.TOTAL_ARTICLES,
        source.POSITIVE_ARTICLES,
        source.NEUTRAL_ARTICLES,
        source.NEGATIVE_ARTICLES,
        source.AVG_SENTIMENT_SCORE,
        source.FNG_INDEX_VALUE,
        CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END
    );
    
    rows_calculated := SQLROWCOUNT;
    
    RETURN 'Calculated hourly F&G index for ' || rows_calculated || ' hours';
END;
$$;

CREATE OR REPLACE PROCEDURE ANALYTICS.CALCULATE_DAILY_FNG_INDEX()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_calculated INT DEFAULT 0;
BEGIN
    -- Calculate daily aggregations with refined label counts
    MERGE INTO ANALYTICS.FNG_NEWS_INDEX_DAILY AS target
    USING (
        WITH daily_stats AS (
            SELECT 
                DATE_TRUNC('DAY', PUBLISHED_DATETIME)::DATE AS DATE,
                COUNT(*) AS TOTAL_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL IN ('GREED', 'EXTREMELY GREED') THEN 1 ELSE 0 END) AS POSITIVE_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END) AS NEUTRAL_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL IN ('FEAR', 'EXTREMELY FEAR') THEN 1 ELSE 0 END) AS NEGATIVE_ARTICLES,
                AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE,
                MIN(SENTIMENT_SCORE) AS MIN_SENTIMENT_SCORE,
                MAX(SENTIMENT_SCORE) AS MAX_SENTIMENT_SCORE,
                STDDEV(SENTIMENT_SCORE) AS STDDEV_SENTIMENT_SCORE,
                ((AVG(SENTIMENT_SCORE) + 1) / 2) * 100 AS FNG_INDEX_VALUE
            FROM ANALYTICS.NEWS_SENTIMENT
            WHERE PUBLISHED_DATETIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('DAY', PUBLISHED_DATETIME)::DATE
        ),
        with_previous AS (
            SELECT 
                *,
                LAG(FNG_INDEX_VALUE) OVER (ORDER BY DATE) AS prev_fng_value
            FROM daily_stats
        )
        SELECT 
            *,
            CASE 
                WHEN prev_fng_value IS NOT NULL 
                THEN FNG_INDEX_VALUE - prev_fng_value 
                ELSE NULL 
            END AS FNG_INDEX_CHANGE,
            CASE 
                WHEN prev_fng_value IS NOT NULL AND prev_fng_value != 0
                THEN ((FNG_INDEX_VALUE - prev_fng_value) / prev_fng_value) * 100
                ELSE NULL 
            END AS FNG_INDEX_CHANGE_PCT
        FROM with_previous
    ) AS source
    ON target.DATE = source.DATE
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_ARTICLES = source.TOTAL_ARTICLES,
        target.POSITIVE_ARTICLES = source.POSITIVE_ARTICLES,
        target.NEUTRAL_ARTICLES = source.NEUTRAL_ARTICLES,
        target.NEGATIVE_ARTICLES = source.NEGATIVE_ARTICLES,
        target.AVG_SENTIMENT_SCORE = source.AVG_SENTIMENT_SCORE,
        target.MIN_SENTIMENT_SCORE = source.MIN_SENTIMENT_SCORE,
        target.MAX_SENTIMENT_SCORE = source.MAX_SENTIMENT_SCORE,
        target.STDDEV_SENTIMENT_SCORE = source.STDDEV_SENTIMENT_SCORE,
        target.FNG_INDEX_VALUE = source.FNG_INDEX_VALUE,
        target.FNG_INDEX_LABEL = CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        target.FNG_INDEX_CHANGE = source.FNG_INDEX_CHANGE,
        target.FNG_INDEX_CHANGE_PCT = source.FNG_INDEX_CHANGE_PCT,
        target.CALCULATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        DATE,
        TOTAL_ARTICLES,
        POSITIVE_ARTICLES,
        NEUTRAL_ARTICLES,
        NEGATIVE_ARTICLES,
        AVG_SENTIMENT_SCORE,
        MIN_SENTIMENT_SCORE,
        MAX_SENTIMENT_SCORE,
        STDDEV_SENTIMENT_SCORE,
        FNG_INDEX_VALUE,
        FNG_INDEX_LABEL,
        FNG_INDEX_CHANGE,
        FNG_INDEX_CHANGE_PCT
    ) VALUES (
        source.DATE,
        source.TOTAL_ARTICLES,
        source.POSITIVE_ARTICLES,
        source.NEUTRAL_ARTICLES,
        source.NEGATIVE_ARTICLES,
        source.AVG_SENTIMENT_SCORE,
        source.MIN_SENTIMENT_SCORE,
        source.MAX_SENTIMENT_SCORE,
        source.STDDEV_SENTIMENT_SCORE,
        source.FNG_INDEX_VALUE,
        CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        source.FNG_INDEX_CHANGE,
        source.FNG_INDEX_CHANGE_PCT
    );
    
    rows_calculated := SQLROWCOUNT;
    
    RETURN 'Calculated daily F&G index for ' || rows_calculated || ' days';
END;
$$;
