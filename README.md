# Politician Issues Data Pipeline

This pipeline extracts politician issues data from Snowflake and stores it in Firebase Realtime Database under the `/politician_issues` path.

## Data Structure

The data is stored in Firebase Realtime Database with the following structure:

```
politician_issues/
  └── [politician_id]/
      └── [Issue_Name]: [1 for pro, 0 for neutral, -1 for anti]
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `env.example` to `.env` and fill in the required values:

- **Snowflake Configuration**: Account, user, password, warehouse, database, and schema
- **Firebase Configuration**: 
  - Path to Firebase service account credentials JSON file
  - Firebase Realtime Database URL (format: `https://<project-id>-default-rtdb.firebaseio.com`)

### 3. Firebase Setup

1. Download your Firebase service account credentials JSON file from the Firebase Console
2. Place it in a secure location
3. Update `FIREBASE_CREDENTIALS_PATH` in `.env` with the path to this file
4. Set `FIREBASE_DATABASE_URL` to your Realtime Database URL

## Usage

Run the pipeline:

```bash
python pipeline.py
```

The pipeline will:
1. Connect to Snowflake and execute an UNPIVOT query on the candidate issue ratings view
2. Transform issue values to standardized format (1, 0, -1)
3. Map candidate names to politician IDs using fuzzy matching
4. Store the data in Firebase Realtime Database, overwriting the entire `/politician_issues` path

## Configuration

### Snowflake View Selection

By default, the pipeline uses `ANALYTICS.MRT_ADMIN.CANDIDATE_ISSUE_RATINGS__CURRENT` which contains candidates with issue ratings.

To use the alternative view (`CANDIDATE_ISSUE_RATINGS__NEEDED`), set in `.env`:

```
SNOWFLAKE_VIEW=ANALYTICS.MRT_ADMIN.CANDIDATE_ISSUE_RATINGS__NEEDED
```

### Politician Mapping

The pipeline uses a `politician_mapping.json` file to store the relationship between politician names (from Snowflake) and politician IDs (used in Firebase). This ensures reproducibility and consistency.

The mapping strategy (in order):
1. Check `politician_mapping.json` for exact or fuzzy match
2. Match against existing `/politicians` index in Firebase (if `POLITICIANS_PATH` is configured)
3. Generate deterministic ID from normalized name

You can manually edit `politician_mapping.json` to add or update mappings.

### Issue Value Transformation

The pipeline automatically transforms issue values to the standardized format:
- **1**: Pro/support/for
- **0**: Neutral/no position/null
- **-1**: Anti/oppose/against

The transformation handles various input formats (strings, numbers, null values).

### Fuzzy Matching Threshold

The fuzzy matching threshold can be configured via `POLITICIAN_MATCH_THRESHOLD` in `.env` (default: 0.92). Higher values require closer matches.

## Logging

The pipeline uses Python's logging module. Log level can be controlled via environment variable or by modifying the logging configuration in `pipeline.py`.

## Error Handling

The pipeline will exit with a non-zero status code if:
- Snowflake connection fails
- Firebase connection fails
- Query execution fails
- Firebase write operation fails

All errors are logged with appropriate detail for debugging.
