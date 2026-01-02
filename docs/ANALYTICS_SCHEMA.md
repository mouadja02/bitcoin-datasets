# Analytics Schema - Sentiment Analysis & Fear & Greed Index

## Overview

The **ANALYTICS** schema provides real-time sentiment analysis on Bitcoin news using Snowflake Cortex ML, generating hourly and daily Fear & Greed News Index metrics.

## Architecture

```
COINDESK.NEWS (Insert)
        ↓
ANALYTICS.NEWS_STREAM (Snowflake Stream)
        ↓
TASK: ANALYZE_NEWS_SENTIMENT (Every 5 min)
        ↓
ANALYTICS.NEWS_SENTIMENT (Cortex ML Analysis)
        ↓
TASK: CALCULATE_HOURLY_FNG (Hourly)
        ↓
ANALYTICS.FNG_NEWS_INDEX_HOURLY
        ↓
TASK: CALCULATE_DAILY_FNG (Daily at 1 AM UTC)
        ↓
ANALYTICS.FNG_NEWS_INDEX_DAILY
```

## Tables

### 1. NEWS_SENTIMENT
**Purpose**: Stores sentiment analysis results for each news article

| Column | Type | Description |
|--------|------|-------------|
| `ID` | NUMBER | News article ID (FK to COINDESK.NEWS) |
| `PUBLISHED_ON` | NUMBER | Unix timestamp |
| `PUBLISHED_DATETIME` | TIMESTAMP_TZ | Converted datetime |
| `TITLE` | STRING | Article title |
| `SOURCE` | STRING | News source |
| `BODY` | STRING | Full article text |
| `SENTIMENT_SCORE` | FLOAT | Overall sentiment (-1 to 1) |
| `SENTIMENT_LABEL` | STRING | EXTREMELY FEAR, FEAR, NEUTRAL, GREED, EXTREMELY GREED |
| `TITLE_SENTIMENT_SCORE` | FLOAT | Sentiment of title only |
| `ANALYZED_AT` | TIMESTAMP_TZ | When analysis was performed |

**Sentiment Scoring**:
- `> 0.6`: EXTREMELY GREED 🤑
- `0.1 to 0.6`: GREED 😊
- `-0.1 to 0.1`: NEUTRAL 😐
- `-0.6 to -0.1`: FEAR 😟
- `<= -0.6`: EXTREMELY FEAR 😱

---

### 2. FNG_NEWS_INDEX_HOURLY
**Purpose**: Hourly aggregated Fear & Greed index (0-100 scale)

| Column | Type | Description |
|--------|------|-------------|
| `HOUR_START` | TIMESTAMP_TZ | Start of the hour |
| `TOTAL_ARTICLES` | INTEGER | Total articles this hour |
| `POSITIVE_ARTICLES` | INTEGER | Number of positive articles |
| `NEUTRAL_ARTICLES` | INTEGER | Number of neutral articles |
| `NEGATIVE_ARTICLES` | INTEGER | Number of negative articles |
| `AVG_SENTIMENT_SCORE` | FLOAT | Average sentiment (-1 to 1) |
| `FNG_INDEX_VALUE` | FLOAT | Fear & Greed index (0-100) |
| `FNG_INDEX_LABEL` | STRING | Index classification |
| `CALCULATED_AT` | TIMESTAMP_TZ | Calculation timestamp |

**Index Scale**:
- `0-24`: EXTREME FEAR 😱
- `25-44`: FEAR 😟
- `45-55`: NEUTRAL 😐
- `56-75`: GREED 😊
- `76-100`: EXTREME GREED 🤑

**Formula**: `((avg_sentiment + 1) / 2) * 100`

---

### 3. FNG_NEWS_INDEX_DAILY
**Purpose**: Daily aggregated Fear & Greed index with trend analysis

| Column | Type | Description |
|--------|------|-------------|
| `DATE` | DATE | Date (UTC) |
| `TOTAL_ARTICLES` | INTEGER | Total articles for the day |
| `POSITIVE_ARTICLES` | INTEGER | Number of positive articles |
| `NEUTRAL_ARTICLES` | INTEGER | Number of neutral articles |
| `NEGATIVE_ARTICLES` | INTEGER | Number of negative articles |
| `AVG_SENTIMENT_SCORE` | FLOAT | Average sentiment |
| `MIN_SENTIMENT_SCORE` | FLOAT | Most negative score |
| `MAX_SENTIMENT_SCORE` | FLOAT | Most positive score |
| `STDDEV_SENTIMENT_SCORE` | FLOAT | Sentiment volatility |
| `FNG_INDEX_VALUE` | FLOAT | Fear & Greed index (0-100) |
| `FNG_INDEX_LABEL` | STRING | Index classification |
| `FNG_INDEX_CHANGE` | FLOAT | Change from previous day |
| `FNG_INDEX_CHANGE_PCT` | FLOAT | % change from previous day |
| `CALCULATED_AT` | TIMESTAMP_TZ | Calculation timestamp |

---

## Streams

### NEWS_STREAM
**Purpose**: Captures new inserts into `COINDESK.NEWS` for real-time processing

```sql
CREATE STREAM ANALYTICS.NEWS_STREAM 
ON TABLE COINDESK.NEWS
APPEND_ONLY = TRUE;
```

- **Mode**: APPEND_ONLY (only captures INSERTs)
- **Trigger**: Checked every 5 minutes by task
- **Consumption**: Automatically consumed by `ANALYZE_NEWS_SENTIMENT` procedure

---

## Stored Procedures

### 1. ANALYZE_NEWS_SENTIMENT()
**Purpose**: Analyzes sentiment of new news articles using Snowflake Cortex

```sql
CALL ANALYTICS.ANALYZE_NEWS_SENTIMENT();
```

**Process**:
1. Reads new articles from `NEWS_STREAM`
2. Applies `SNOWFLAKE.CORTEX.SENTIMENT()` to article body
3. Applies `SNOWFLAKE.CORTEX.SENTIMENT()` to title
4. Classifies sentiment into 5 categories: EXTREMELY FEAR, FEAR, NEUTRAL, GREED, EXTREMELY GREED
5. Inserts results into `NEWS_SENTIMENT`
6. Returns count of processed articles

**Trigger**: Automated task every 5 minutes

---

### 2. CALCULATE_HOURLY_FNG_INDEX()
**Purpose**: Calculates hourly Fear & Greed index

```sql
CALL ANALYTICS.CALCULATE_HOURLY_FNG_INDEX();
```

**Process**:
1. Groups `NEWS_SENTIMENT` by hour
2. Calculates article counts by sentiment label
3. Computes average sentiment score
4. Converts to 0-100 scale
5. Assigns F&G label based on value
6. Merges into `FNG_NEWS_INDEX_HOURLY`

**Trigger**: Automated task hourly at 5 minutes past the hour

---

### 3. CALCULATE_DAILY_FNG_INDEX()
**Purpose**: Calculates daily Fear & Greed index with trends

```sql
CALL ANALYTICS.CALCULATE_DAILY_FNG_INDEX();
```

**Process**:
1. Groups `NEWS_SENTIMENT` by day
2. Calculates comprehensive statistics (avg, min, max, stddev)
3. Computes daily F&G index
4. Calculates change from previous day
5. Merges into `FNG_NEWS_INDEX_DAILY`

**Trigger**: Automated task daily at 1:00 AM UTC

---

## Automated Tasks

### Task Hierarchy

```
TASK_ANALYZE_NEWS_SENTIMENT (Every 5 min, when stream has data)
        ↓ AFTER
TASK_CALCULATE_HOURLY_FNG (Hourly at :05)
        ↓ AFTER
TASK_CALCULATE_DAILY_FNG (Daily at 1:00 AM UTC)
```

### Enable Tasks

```sql
-- Enable in reverse order (child to parent)
ALTER TASK ANALYTICS.TASK_CALCULATE_DAILY_FNG RESUME;
ALTER TASK ANALYTICS.TASK_CALCULATE_HOURLY_FNG RESUME;
ALTER TASK ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT RESUME;
```

### Monitor Tasks

```sql
-- Check task status
SHOW TASKS IN SCHEMA ANALYTICS;

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_ANALYZE_NEWS_SENTIMENT',
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

### Disable Tasks

```sql
-- Disable in order (parent to child)
ALTER TASK ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT SUSPEND;
ALTER TASK ANALYTICS.TASK_CALCULATE_HOURLY_FNG SUSPEND;
ALTER TASK ANALYTICS.TASK_CALCULATE_DAILY_FNG SUSPEND;
```

---

## Views

### VW_LATEST_NEWS_SENTIMENT
**Purpose**: Latest news with sentiment analysis

```sql
SELECT * FROM ANALYTICS.VW_LATEST_NEWS_SENTIMENT
LIMIT 10;
```

**Columns**: ID, PUBLISHED_DATETIME, TITLE, SOURCE, SENTIMENT_SCORE, SENTIMENT_LABEL, URL, UPVOTES

---

### VW_CURRENT_FNG_INDEX
**Purpose**: Current Fear & Greed index (hourly and daily)

```sql
SELECT * FROM ANALYTICS.VW_CURRENT_FNG_INDEX;
```

**Returns**: Latest hourly and daily F&G index values with metadata

---

## Usage Examples

### Manual Processing

```sql
-- 1. Analyze new articles manually
CALL ANALYTICS.ANALYZE_NEWS_SENTIMENT();

-- 2. Calculate hourly index
CALL ANALYTICS.CALCULATE_HOURLY_FNG_INDEX();

-- 3. Calculate daily index
CALL ANALYTICS.CALCULATE_DAILY_FNG_INDEX();
```

### Query Examples

#### Get Latest F&G Index
```sql
SELECT * FROM ANALYTICS.VW_CURRENT_FNG_INDEX;
```

#### 24-Hour Sentiment Trend
```sql
SELECT 
    HOUR_START,
    FNG_INDEX_VALUE,
    FNG_INDEX_LABEL,
    TOTAL_ARTICLES,
    POSITIVE_ARTICLES,
    NEGATIVE_ARTICLES
FROM ANALYTICS.FNG_NEWS_INDEX_HOURLY
WHERE HOUR_START >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY HOUR_START DESC;
```

#### Daily F&G Trend (30 days)
```sql
SELECT 
    DATE,
    FNG_INDEX_VALUE,
    FNG_INDEX_LABEL,
    FNG_INDEX_CHANGE,
    FNG_INDEX_CHANGE_PCT,
    TOTAL_ARTICLES
FROM ANALYTICS.FNG_NEWS_INDEX_DAILY
WHERE DATE >= DATEADD(DAY, -30, CURRENT_DATE())
ORDER BY DATE DESC;
```

#### Most Positive/Negative News (Greed/Fear)
```sql
-- Most Greed (Extremely Positive)
SELECT 
    TITLE,
    SOURCE,
    SENTIMENT_SCORE,
    PUBLISHED_DATETIME,
    URL
FROM ANALYTICS.VW_LATEST_NEWS_SENTIMENT
WHERE SENTIMENT_LABEL = 'EXTREMELY GREED'
ORDER BY SENTIMENT_SCORE DESC
LIMIT 5;

-- Most Fear (Extremely Negative)
SELECT 
    TITLE,
    SOURCE,
    SENTIMENT_SCORE,
    PUBLISHED_DATETIME,
    URL
FROM ANALYTICS.VW_LATEST_NEWS_SENTIMENT
WHERE SENTIMENT_LABEL = 'EXTREMELY FEAR'
ORDER BY SENTIMENT_SCORE ASC
LIMIT 5;
```

#### Sentiment Distribution
```sql
SELECT 
    SENTIMENT_LABEL,
    COUNT(*) AS COUNT,
    AVG(SENTIMENT_SCORE) AS AVG_SCORE,
    MIN(SENTIMENT_SCORE) AS MIN_SCORE,
    MAX(SENTIMENT_SCORE) AS MAX_SCORE
FROM ANALYTICS.NEWS_SENTIMENT
WHERE PUBLISHED_DATETIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY SENTIMENT_LABEL
ORDER BY COUNT DESC;
```

#### F&G Correlation with Price
```sql
SELECT 
    d.DATE,
    d.FNG_INDEX_VALUE,
    d.FNG_INDEX_LABEL,
    h.CLOSE AS BTC_PRICE,
    d.FNG_INDEX_CHANGE_PCT,
    ((h.CLOSE - LAG(h.CLOSE) OVER (ORDER BY h.TIME)) / LAG(h.CLOSE) OVER (ORDER BY h.TIME)) * 100 AS PRICE_CHANGE_PCT
FROM ANALYTICS.FNG_NEWS_INDEX_DAILY d
JOIN COINDESK.HISTODAY h 
    ON d.DATE = TO_DATE(TO_TIMESTAMP(h.TIME))
WHERE d.DATE >= DATEADD(DAY, -30, CURRENT_DATE())
ORDER BY d.DATE DESC;
```

---

## Data Flow

### Real-time Processing

1. **News Ingestion**: CoinDesk data fetcher inserts new articles into `COINDESK.NEWS`
2. **Stream Capture**: `NEWS_STREAM` captures the INSERT
3. **Sentiment Analysis**: Task triggers every 5 minutes when stream has data
4. **Cortex Processing**: Articles are analyzed using Snowflake Cortex ML
5. **Index Calculation**: Hourly and daily tasks aggregate sentiment into F&G index
6. **Querying**: Views provide easy access to latest metrics

### Backfill Historical Data

```sql
-- Analyze all existing news articles
INSERT INTO ANALYTICS.NEWS_SENTIMENT (
    ID, PUBLISHED_ON, PUBLISHED_DATETIME, TITLE, SOURCE, BODY,
    SENTIMENT_SCORE, SENTIMENT_LABEL, TITLE_SENTIMENT_SCORE
)
SELECT 
    ID,
    PUBLISHED_ON,
    TO_TIMESTAMP(PUBLISHED_ON),
    TITLE,
    SOURCE,
    BODY,
    SNOWFLAKE.CORTEX.SENTIMENT(BODY),
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(BODY) <= -0.6 THEN 'EXTREMELY FEAR'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(BODY) <= -0.1 THEN 'FEAR'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(BODY) < 0.1 THEN 'NEUTRAL'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(BODY) < 0.6 THEN 'GREED'
        ELSE 'EXTREMELY GREED'
    END,
    SNOWFLAKE.CORTEX.SENTIMENT(TITLE)
FROM COINDESK.NEWS
WHERE ID NOT IN (SELECT ID FROM ANALYTICS.NEWS_SENTIMENT);

-- Then calculate historical indexes
CALL ANALYTICS.CALCULATE_HOURLY_FNG_INDEX();
CALL ANALYTICS.CALCULATE_DAILY_FNG_INDEX();
```

---

## Cost Optimization

### Cortex ML Costs
- Sentiment analysis is billed per token
- Average news article: ~500 tokens
- Cost: $0.001 per 1K tokens
- Estimated: ~$0.0005 per article

### Task Scheduling
- Tasks only run when needed (stream has data)
- Warehouse suspends automatically when idle
- Set warehouse size based on volume:
  - X-SMALL: < 100 articles/hour
  - SMALL: 100-1000 articles/hour
  - MEDIUM: > 1000 articles/hour

### Monitor Costs

```sql
-- Check Cortex usage
SELECT 
    DATE_TRUNC('DAY', START_TIME) AS DAY,
    COUNT(*) AS ANALYSES,
    SUM(CREDITS_USED) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE SERVICE_TYPE = 'CORTEX_AI'
    AND START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('DAY', START_TIME)
ORDER BY DAY DESC;
```

---

## Troubleshooting

### Stream Not Processing

```sql
-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('ANALYTICS.NEWS_STREAM');

-- View stream contents (without consuming)
SELECT * FROM ANALYTICS.NEWS_STREAM LIMIT 10;
```

### Task Not Running

```sql
-- Check task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_ANALYZE_NEWS_SENTIMENT'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- Check task state
SHOW TASKS LIKE 'TASK_ANALYZE_NEWS_SENTIMENT';
```

### Sentiment Analysis Errors

```sql
-- Check for NULL sentiment scores
SELECT COUNT(*)
FROM ANALYTICS.NEWS_SENTIMENT
WHERE SENTIMENT_SCORE IS NULL;

-- Reprocess failed articles
DELETE FROM ANALYTICS.NEWS_SENTIMENT WHERE SENTIMENT_SCORE IS NULL;
-- Then re-run: CALL ANALYTICS.ANALYZE_NEWS_SENTIMENT();
```

---

## Best Practices

1. **Enable Tasks**: Only enable tasks when ready for production
2. **Monitor Costs**: Track Cortex usage and warehouse credits
3. **Backfill Data**: Process historical news for baseline metrics
4. **Index Tuning**: Adjust F&G thresholds based on your use case
5. **Alert Setup**: Create alerts for extreme F&G values
6. **Data Retention**: Archive old sentiment data if needed

---

**Schema Version**: V1.1.4  
**Created**: 2026-01-02  
**Snowflake Features Used**: Streams, Tasks, Cortex ML, Stored Procedures
