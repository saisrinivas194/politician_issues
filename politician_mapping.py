"""
Politician mapping utility for storing and retrieving politician name to ID mappings.

This module provides a mapping class that ensures reproducibility and consistency
in matching politician names from Snowflake to politician IDs in Firebase.
"""

import json
import logging
import os
import re
from typing import Dict, Optional
from difflib import SequenceMatcher


logger = logging.getLogger(__name__)


class PoliticianMapping:
    """
    Manages politician name to ID mappings with fuzzy matching support.
    
    Mappings are persisted to a JSON file to ensure reproducibility across runs.
    The class provides both exact and fuzzy matching capabilities to handle
    variations in name formatting.
    """
    
    def __init__(self, mapping_file: str = "politician_mapping.json"):
        """
        Initialize the politician mapping.
        
        Args:
            mapping_file: Path to the JSON file storing the mappings
        """
        self.mapping_file = mapping_file
        self.mappings: Dict[str, str] = self._load_mappings()
    
    def _load_mappings(self) -> Dict[str, str]:
        """
        Load existing mappings from file.
        
        Returns:
            Dictionary mapping politician names to IDs, empty dict if file doesn't exist
        """
        if os.path.exists(self.mapping_file):
            try:
                with open(self.mapping_file, 'r') as f:
                    mappings = json.load(f)
                    logger.debug(f"Loaded {len(mappings)} mappings from {self.mapping_file}")
                    return mappings
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load mappings file {self.mapping_file}: {e}")
                return {}
        return {}
    
    def _save_mappings(self):
        """Save current mappings to file."""
        try:
            with open(self.mapping_file, 'w') as f:
                json.dump(self.mappings, f, indent=2)
            logger.debug(f"Saved {len(self.mappings)} mappings to {self.mapping_file}")
        except IOError as e:
            logger.error(f"Failed to save mappings file {self.mapping_file}: {e}")
            raise
    
    def get_politician_id(self, politician_name: str, threshold: float = 0.90) -> Optional[str]:
        """
        Get politician ID for a given name using fuzzy matching.
        
        First attempts an exact match (case-insensitive, normalized), then falls
        back to fuzzy matching if no exact match is found.
        
        Args:
            politician_name: The name of the politician from Snowflake
            threshold: Similarity threshold for fuzzy matching (0-1), default 0.90
        
        Returns:
            Politician ID if found, None otherwise
        """
        if not politician_name:
            return None
            
        # Normalize the name for comparison
        normalized_name = self._normalize_name(politician_name)
        
        # First, try exact match (case-insensitive)
        for stored_name, politician_id in self.mappings.items():
            if self._normalize_name(stored_name) == normalized_name:
                return politician_id
        
        # If no exact match, try fuzzy matching
        best_match = None
        best_ratio = 0.0
        
        for stored_name, politician_id in self.mappings.items():
            ratio = self._name_similarity(normalized_name, self._normalize_name(stored_name))
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = politician_id
        
        if best_match:
            logger.debug(f"Fuzzy match found: {politician_name} -> {best_match} (score: {best_ratio:.3f})")
        
        return best_match
    
    def add_mapping(self, politician_name: str, politician_id: str):
        """
        Add a new mapping between politician name and ID.
        
        Args:
            politician_name: The name from Snowflake
            politician_id: The ID to use in Firebase
        """
        if not politician_name or not politician_id:
            logger.warning(f"Attempted to add invalid mapping: {politician_name} -> {politician_id}")
            return
            
        self.mappings[politician_name] = politician_id
        self._save_mappings()
        logger.debug(f"Added mapping: {politician_name} -> {politician_id}")
    
    def _normalize_name(self, name: str) -> str:
        """
        Normalize name for comparison.
        
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
        if not isinstance(name, str):
            return ""
        s = name.lower().strip()
        s = re.sub(r"[.,']", " ", s)
        s = re.sub(r"[^a-z\s-]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

        # Drop common suffixes/titles that create false mismatches
        drop = {
            "jr", "sr", "ii", "iii", "iv",
            "mr", "mrs", "ms", "dr",
            "sen", "senator", "rep", "representative",
            "gov", "governor",
        }
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
        tokens = [t for t in s.split() if t]
        tokens.sort()
        return " ".join(tokens)

    def _name_similarity(self, a: str, b: str) -> float:
        """
        Calculate similarity score between two normalized names.
        
        Uses both raw normalized comparison and token-sorted comparison,
        returning the maximum score to be robust to word ordering differences.
        
        Args:
            a: First normalized name
            b: Second normalized name
        
        Returns:
            Similarity score between 0.0 and 1.0
        """
        if not a or not b:
            return 0.0
        raw = SequenceMatcher(None, a, b).ratio()
        tok = SequenceMatcher(None, self._token_sort_key(a), self._token_sort_key(b)).ratio()
        return max(raw, tok)
    
    def get_all_mappings(self) -> Dict[str, str]:
        """
        Get all current mappings.
        
        Returns:
            Copy of the mappings dictionary
        """
        return self.mappings.copy()
