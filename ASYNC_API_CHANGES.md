# Asynchronous API Implementation

This document describes the changes made to support the X Ads API asynchronous analytics endpoints.

## Overview

The `embulk-input-twitter_ads_analytics` plugin has been updated to support both synchronous and asynchronous API calls for retrieving analytics data. The asynchronous API allows for longer date ranges (up to 90 days) and supports segmentation features.

## Changes Made

### 1. New Configuration Parameter

- **`async`** (boolean): Controls whether to use the asynchronous or synchronous API
  - `true`: Uses the asynchronous API (POST/GET stats/jobs/accounts/:account_id)
  - `false`: Uses the original synchronous API (GET stats/accounts/:account_id)

### 2. New Methods Added

#### `request_stats(access_token, entity_ids, chunked_time)`
- Updated to route requests based on the `@async` flag
- Maintains backward compatibility with existing configurations

#### `request_stats_async(access_token, entity_ids, chunked_time)`
- Implements the complete asynchronous workflow:
  1. Creates an async job
  2. Polls for job completion
  3. Downloads and processes the result

#### `create_async_job(access_token, entity_ids, chunked_time)`
- Creates an asynchronous analytics job using POST stats/jobs/accounts/:account_id
- Returns the job ID for status polling

#### `poll_job_status(access_token, job_id)`
- Polls the job status using GET stats/jobs/accounts/:account_id?job_ids=:job_id
- Handles different job statuses: QUEUED, PROCESSING, SUCCESS, FAILED
- Implements timeout and retry logic (max 60 attempts, 10-second intervals)

#### `download_and_process_job_result(access_token, job_data)`
- Downloads the gzipped result file from the provided URL
- Decompresses the data using Zlib::GzipReader
- Parses the JSON response and returns the data

#### `request_stats_sync(access_token, entity_ids, chunked_time)`
- Renamed from the original `request_stats` method
- Maintains the original synchronous API functionality

## Usage

### Configuration Example

```yaml
in:
  type: twitter_ads_analytics
  consumer_key: "your_consumer_key"
  consumer_secret: "your_consumer_secret"
  oauth_token: "your_oauth_token"
  oauth_token_secret: "your_oauth_token_secret"
  account_id: "your_account_id"
  entity: "CAMPAIGN"
  metric_groups: ["ENGAGEMENT"]
  granularity: "DAY"
  placement: "ALL_ON_TWITTER"
  start_date: "2023-01-01"
  end_date: "2023-01-31"
  timezone: "UTC"
  async: true  # Enable asynchronous API
  columns:
    - {name: date, type: timestamp, format: "%Y-%m-%d"}
    - {name: campaign_id, type: string}
    - {name: impressions, type: long}
    - {name: engagements, type: long}
```

### When to Use Asynchronous API

Use the asynchronous API (`async: true`) when:
- Requesting data for date ranges longer than a few days
- Need segmentation features (future enhancement)
- Want to avoid rate limiting issues with large data requests
- Processing large volumes of analytics data

Use the synchronous API (`async: false`) when:
- Requesting small amounts of data
- Need immediate results
- Working with existing configurations that don't require changes

## Benefits

1. **Longer Date Ranges**: Support for up to 90 days of data in a single request
2. **Better Rate Limiting**: Async API uses concurrent job limits instead of request rate limits
3. **Segmentation Support**: Ready for future segmentation features
4. **Backward Compatibility**: Existing configurations continue to work unchanged
5. **Robust Error Handling**: Comprehensive retry logic and error handling

## Technical Details

### API Endpoints Used

- **Job Creation**: `POST https://ads-api.twitter.com/{version}/stats/jobs/accounts/{account_id}`
- **Job Status**: `GET https://ads-api.twitter.com/{version}/stats/jobs/accounts/{account_id}?job_ids={job_id}`
- **Result Download**: Direct HTTPS download from the URL provided in the job result

### Job Status Flow

1. **QUEUED**: Job is waiting to be processed
2. **PROCESSING**: Job is currently being processed
3. **SUCCESS**: Job completed successfully, result URL available
4. **FAILED**: Job failed, error details in response

### Timeout and Retry Logic

- **Polling Timeout**: Maximum 60 attempts (10 minutes)
- **Polling Interval**: 10 seconds between status checks
- **Download Retries**: Up to 5 retry attempts for result download
- **Rate Limit Handling**: Respects X-Rate-Limit-Reset headers

## Error Handling

The implementation includes comprehensive error handling for:
- Job creation failures
- Job timeout scenarios
- Download failures
- Decompression errors
- Network connectivity issues
- API rate limiting

All errors are logged with appropriate detail levels and include retry logic where applicable.

## Testing

The implementation has been validated for:
- ✓ Ruby syntax correctness
- ✓ Method definitions and structure
- ✓ API endpoint implementation
- ✓ Async/sync branching logic
- ✓ Error handling patterns

For production use, test with actual Twitter Ads API credentials to verify end-to-end functionality.
