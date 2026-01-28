"""
Data pipeline to extract politician issues from Snowflake and store them in Firebase.

This module provides a pipeline class that:
- Connects to Snowflake and executes queries to fetch candidate issue ratings
- Transforms the data from wide format (one row per candidate) to long format (one row per candidate-issue pair)
- Maps candidate names to politician IDs using fuzzy matching
- Stores the transformed data in Firebase Realtime Database
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional
from difflib import SequenceMatcher
import snowflake.connector
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
from politician_mapping import PoliticianMapping


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class PoliticianIssuesPipeline:
    """
    Pipeline to synchronize politician issues from Snowflake to Firebase.
    
    The pipeline performs the following operations:
    1. Connects to Snowflake and Firebase
    2. Optionally loads existing politician mappings from Firebase
    3. Executes a query to fetch candidate issue ratings
    4. Transforms issue values to standardized format (1, 0, -1)
    5. Maps candidate names to politician IDs using fuzzy matching
    6. Stores the data in Firebase Realtime Database
    """
    
    def __init__(self):
        """Initialize the pipeline with Snowflake and Firebase connections."""
        self.snowflake_conn = None
        self.rtdb_root = None
        self.politician_mapping = PoliticianMapping()
        self.politicians_index: Dict[str, str] = {}
        self._init_snowflake()
        self._init_firebase()
        self._load_politicians_index()
    
    def _init_snowflake(self):
        """Initialize Snowflake connection using environment variables."""
        try:
            account = os.getenv('SNOWFLAKE_ACCOUNT')
            user = os.getenv('SNOWFLAKE_USER')
            password = os.getenv('SNOWFLAKE_PASSWORD')
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
            database = os.getenv('SNOWFLAKE_DATABASE')
            schema = os.getenv('SNOWFLAKE_SCHEMA')
            role = os.getenv('SNOWFLAKE_ROLE')
            
            if not all([account, user, password, warehouse, database, schema]):
                raise ValueError("Missing required Snowflake configuration in environment variables")
            
            connection_params = {
                'account': account,
                'user': user,
                'password': password,
                'warehouse': warehouse,
                'database': database,
                'schema': schema
            }
            
            if role:
                connection_params['role'] = role
            
            self.snowflake_conn = snowflake.connector.connect(**connection_params)
            logger.info("Snowflake connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            sys.exit(1)
    
    def _init_firebase(self):
        """Initialize Firebase Admin SDK for Realtime Database access."""
        try:
            creds_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
            if not creds_path or not os.path.exists(creds_path):
                raise FileNotFoundError(f"Firebase credentials file not found: {creds_path}")

            database_url = os.getenv("FIREBASE_DATABASE_URL")
            if not database_url:
                raise ValueError("FIREBASE_DATABASE_URL is required for Realtime Database access")
            
            # Initialize Firebase Admin if not already initialized
            if not firebase_admin._apps:
                cred = credentials.Certificate(creds_path)
                firebase_admin.initialize_app(cred, {"databaseURL": database_url})
            
            self.rtdb_root = db.reference("/")
            logger.info("Firebase connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Firebase: {e}")
            sys.exit(1)

    def _load_politicians_index(self):
        """
        Load existing politicians from Firebase to improve name-to-ID matching.
        
        Expected structure in Firebase:
          /politicians/{politician_id}/name OR full_name OR politician_name
        
        This index is used to match candidate names from Snowflake to existing
        politician IDs in Firebase, ensuring consistency across data sources.
        """
        try:
            politicians_path = os.getenv("POLITICIANS_PATH", "/politicians").strip() or "/politicians"
            snap = db.reference(politicians_path).get() or {}
            if not isinstance(snap, dict):
                logger.debug(f"No politicians found at {politicians_path} or invalid format")
                return

            index: Dict[str, str] = {}
            for politician_id, payload in snap.items():
                if not politician_id or not isinstance(payload, dict):
                    continue
                name = (
                    payload.get("name")
                    or payload.get("full_name")
                    or payload.get("politician_name")
                    or payload.get("display_name")
                )
                if isinstance(name, str) and name.strip():
                    index[self._normalize_person_name(name)] = politician_id

            self.politicians_index = index
            if index:
                logger.info(f"Loaded {len(index)} politicians from {politicians_path} for name matching")
        except Exception as e:
            logger.warning(f"Failed to load politicians index (non-fatal): {e}")
            # Non-fatal; we can still proceed with mapping file + generated IDs

    def _best_fuzzy_politician_match(self, politician_name: str, threshold: float = 0.92) -> Optional[str]:
        """
        Find the best fuzzy match for a politician name in the loaded /politicians index.
        
        Uses both raw and token-sorted similarity to be robust to ordering and punctuation
        differences. Returns the politician ID if a match above the threshold is found.
        
        Args:
            politician_name: The candidate name from Snowflake
            threshold: Minimum similarity score required (0-1)
        
        Returns:
            Politician ID if match found, None otherwise
        """
        if not self.politicians_index:
            return None

        q = self._normalize_person_name(politician_name)
        if not q:
            return None

        # Fast path: exact match after normalization
        if q in self.politicians_index:
            return self.politicians_index[q]

        best_id: Optional[str] = None
        best_score = 0.0

        q_tok = self._token_sort_key(q)
        for cand_norm, cand_id in self.politicians_index.items():
            raw = SequenceMatcher(None, q, cand_norm).ratio()
            tok = SequenceMatcher(None, q_tok, self._token_sort_key(cand_norm)).ratio()
            score = max(raw, tok)
            if score > best_score:
                best_score = score
                best_id = cand_id

        if best_id and best_score >= threshold:
            logger.debug(f"Fuzzy match found: {politician_name} -> {best_id} (score: {best_score:.3f})")
            return best_id
        return None
    
    def fetch_politician_issues_from_snowflake(self, query: str) -> List[Dict]:
        """
        Execute a SQL query against Snowflake and return results as a list of dictionaries.
        
        Args:
            query: SQL query string to execute
        
        Returns:
            List of dictionaries, where each dictionary represents a row with column names as keys
        
        Raises:
            Exception: If query execution fails
        """
        cursor = self.snowflake_conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            logger.info(f"Fetched {len(results)} records from Snowflake")
            return results
        except Exception as e:
            logger.error(f"Error executing Snowflake query: {e}")
            raise
        finally:
            cursor.close()
    
    def get_or_create_politician_id(self, politician_name: str) -> str:
        """
        Get existing politician ID or create a new mapping.
        
        Matching strategy (in order):
        1. Check mapping file for exact or fuzzy match
        2. Match against existing /politicians index in Firebase
        3. Generate deterministic ID from normalized name
        
        Args:
            politician_name: Name of the politician from Snowflake
        
        Returns:
            Politician ID to use in Firebase
        """
        # Strategy 1: Check mapping file (most reproducible)
        politician_id = self.politician_mapping.get_politician_id(politician_name)
        
        if politician_id:
            return politician_id

        # Strategy 2: Match against existing /politicians index
        threshold = float(os.getenv("POLITICIAN_MATCH_THRESHOLD", "0.92"))
        politician_id = self._best_fuzzy_politician_match(politician_name, threshold=threshold)
        if politician_id:
            self.politician_mapping.add_mapping(politician_name, politician_id)
            logger.info(f"Mapped {politician_name} to existing politician ID: {politician_id}")
            return politician_id
        
        # Strategy 3: Fallback - generate deterministic ID from normalized name
        logger.warning(f"No mapping found for politician: {politician_name}. Generating ID from name.")
        politician_id = self._slugify_person_name(politician_name)
        
        # Store the mapping for future use
        self.politician_mapping.add_mapping(politician_name, politician_id)
        
        return politician_id
    
    def transform_issue_value(self, value: Any) -> int:
        """
        Transform issue value to standardized format.
        
        Converts various input formats to standardized integer values:
        - 1: Pro/support/for
        - 0: Neutral/no position/null
        - -1: Anti/oppose/against
        
        Args:
            value: Raw value from Snowflake (string, int, float, or None)
        
        Returns:
            Integer value: 1, 0, or -1
        """
        if value is None:
            return 0
        
        # Handle string values
        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in ['pro', '1', 'yes', 'support', 'for']:
                return 1
            elif value_lower in ['anti', '-1', 'no', 'oppose', 'against']:
                return -1
            else:
                return 0
        
        # Handle numeric values
        if isinstance(value, (int, float)):
            if value > 0:
                return 1
            elif value < 0:
                return -1
            else:
                return 0
        
        return 0

    def _normalize_person_name(self, name: str) -> str:
        """
        Normalize person name for comparison.
        
        Performs the following transformations:
        - Convert to lowercase
        - Remove punctuation
        - Collapse whitespace
        - Remove common suffixes and titles
        
        Args:
            name: Raw name string
        
        Returns:
            Normalized name string
        """
        s = name.lower().strip().replace(",", " ").replace(".", " ").replace("'", " ")
        s = " ".join(s.split())
        # Drop common suffixes/titles that create false mismatches
        drop = {"jr", "sr", "ii", "iii", "iv", "sen", "senator", "rep", "representative", "gov", "governor"}
        tokens = [t for t in s.replace("-", " ").split() if t and t not in drop]
        return " ".join(tokens)

    def _token_sort_key(self, s: str) -> str:
        """
        Create a token-sorted key for order-insensitive comparison.
        
        Args:
            s: Input string
        
        Returns:
            String with tokens sorted alphabetically
        """
        toks = [t for t in s.split() if t]
        toks.sort()
        return " ".join(toks)

    def _slugify_person_name(self, name: str) -> str:
        """
        Convert person name to a URL-safe slug format.
        
        Args:
            name: Person name
        
        Returns:
            Slugified string suitable for use as an ID
        """
        normalized = self._normalize_person_name(name)
        return normalized.replace("'", "").replace(".", "").replace(" ", "_")

    def _issue_column_to_display_name(self, col: str) -> str:
        """
        Convert Snowflake column names to human-readable issue names.
        
        Handles special cases for issues with ampersands or specific phrasing,
        and falls back to generic title case conversion for others.
        
        Args:
            col: Column name from Snowflake (e.g., "ABORTION_REPRODUCTIVE_RIGHTS")
        
        Returns:
            Display name (e.g., "Abortion & Reproductive Rights")
        """
        # Special cases with "&" / specific phrasing
        special = {
            "ABORTION_REPRODUCTIVE_RIGHTS": "Abortion & Reproductive Rights",
            "ENVIRONMENT_REGULATIONS_RENEWABLE_ENERGY": "Environment Regulations & Renewable Energy",
            "SOCIAL_SECURITY_MEDICARE_EXPANSION": "Social Security & Medicare Expansion",
            "LGBTQ_RIGHTS": "LGBTQ Rights",
            "DEI": "DEI",
            "ISRAEL": "Israel",
        }
        if col in special:
            return special[col]

        # Generic: Title Case, underscores -> spaces
        parts = col.split("_")
        return " ".join(p.capitalize() if p else p for p in parts)

    def build_unpivot_query(
        self,
        fully_qualified_view: str = "ANALYTICS.MRT_ADMIN.CANDIDATE_ISSUE_RATINGS__CURRENT",
        politician_name_col: str = "CANDIDATE_NAME_FIRST_LAST",
    ) -> str:
        """
        Build a Snowflake UNPIVOT query to transform wide format to long format.
        
        Transforms a view with one row per candidate (with multiple issue columns)
        into a result set with one row per candidate-issue pair.
        
        Args:
            fully_qualified_view: Fully qualified view name (database.schema.view)
            politician_name_col: Column name containing the politician name
        
        Returns:
            SQL query string ready to execute
        """
        return f"""
SELECT
  {politician_name_col} AS POLITICIAN_NAME,
  ISSUE_COL,
  ISSUE_VALUE
FROM {fully_qualified_view}
UNPIVOT(ISSUE_VALUE FOR ISSUE_COL IN (
  ABORTION_REPRODUCTIVE_RIGHTS,
  DEFENSE_SPENDING,
  ENVIRONMENT_REGULATIONS_RENEWABLE_ENERGY,
  GUN_CONTROL,
  UNIVERSAL_HEALTHCARE,
  STRONGER_IMMIGRATION_CONTROL,
  EDUCATION_SPENDING,
  SOCIAL_MEDIA_REGULATION,
  RAISING_MINIMUM_WAGE,
  AFFORDABLE_HOUSING_SPENDING,
  FAMILY_MEDICAL_LEAVE_BENEFITS,
  MILITARY_AID_TO_UKRAINE,
  UNION_SUPPORT,
  SOCIAL_SECURITY_MEDICARE_EXPANSION,
  WORKPLACE_SAFETY,
  LGBTQ_RIGHTS,
  DEI,
  ISRAEL
))
""".strip()
    
    def store_issues_in_firebase(
        self,
        issues_data: List[Dict],
        politician_name_col: str,
        issue_col_col: str,
        issue_value_col: str,
        firebase_root_path: str = "/politician_issues",
    ):
        """
        Store politician issues in Firebase Realtime Database.
        
        Groups issues by politician, transforms issue values, and writes to Firebase.
        The entire path is overwritten with the new data structure.
        
        Args:
            issues_data: List of dictionaries from Snowflake query results
            politician_name_col: Column name containing politician name
            issue_col_col: Column name containing issue column name
            issue_value_col: Column name containing issue value
            firebase_root_path: Firebase path to write data (default: "/politician_issues")
        """
        # Group issues by politician
        politician_issues: Dict[str, Dict[str, int]] = {}
        
        for row in issues_data:
            politician_name = row.get(politician_name_col)
            issue_col = row.get(issue_col_col)
            issue_value = row.get(issue_value_col)
            
            if not politician_name or not issue_col:
                continue
            
            # Get or create politician ID
            politician_id = self.get_or_create_politician_id(politician_name)
            
            # Initialize politician dict if needed
            if politician_id not in politician_issues:
                politician_issues[politician_id] = {}
            
            # Transform and store issue value
            issue_name = self._issue_column_to_display_name(str(issue_col))
            transformed_value = self.transform_issue_value(issue_value)
            politician_issues[politician_id][issue_name] = transformed_value

        # Overwrite the entire /politician_issues path
        firebase_root_path = firebase_root_path.strip()
        if not firebase_root_path.startswith("/"):
            firebase_root_path = "/" + firebase_root_path
        
        try:
            db.reference(firebase_root_path).set(politician_issues)
            logger.info(f"Stored issues for {len(politician_issues)} politicians in Firebase at {firebase_root_path}")
        except Exception as e:
            logger.error(f"Failed to write to Firebase: {e}")
            raise
    
    def run(
        self,
        query: str,
        politician_name_col: str = "POLITICIAN_NAME",
        issue_col_col: str = "ISSUE_COL",
        issue_value_col: str = "ISSUE_VALUE",
        firebase_root_path: str = "/politician_issues",
    ):
        """
        Execute the complete pipeline.
        
        Args:
            query: SQL query to fetch data from Snowflake
            politician_name_col: Column name for politician name in query results
            issue_col_col: Column name for issue column name in query results
            issue_value_col: Column name for issue value in query results
            firebase_root_path: Firebase path to write data
        """
        logger.info("Starting politician issues pipeline")
        
        # Fetch data from Snowflake
        logger.info("Fetching data from Snowflake")
        issues_data = self.fetch_politician_issues_from_snowflake(query)
        
        # Store in Firebase
        logger.info("Storing data in Firebase")
        self.store_issues_in_firebase(
            issues_data,
            politician_name_col,
            issue_col_col,
            issue_value_col,
            firebase_root_path=firebase_root_path,
        )
        
        logger.info("Pipeline completed successfully")
    
    def close(self):
        """Close database connections and clean up resources."""
        if self.snowflake_conn:
            self.snowflake_conn.close()
            logger.info("Snowflake connection closed")


if __name__ == "__main__":
    # Default view: CANDIDATE_ISSUE_RATINGS__CURRENT contains candidates WITH issue ratings
    # Alternative: CANDIDATE_ISSUE_RATINGS__NEEDED contains candidates WITHOUT issue ratings
    DEFAULT_VIEW = os.getenv(
        "SNOWFLAKE_VIEW",
        "ANALYTICS.MRT_ADMIN.CANDIDATE_ISSUE_RATINGS__CURRENT"
    )
    
    pipeline = PoliticianIssuesPipeline()
    try:
        unpivot_query = pipeline.build_unpivot_query(fully_qualified_view=DEFAULT_VIEW)
        pipeline.run(query=unpivot_query)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        pipeline.close()
