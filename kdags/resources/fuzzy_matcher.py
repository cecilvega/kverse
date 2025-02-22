# resources/text_analyzer.py

import pandas as pd
from rapidfuzz import fuzz, process
import unicodedata
import re
from typing import Dict, List, Tuple, Optional, Union


class FuzzyMatcher:
    """
    A class for performing fuzzy text analysis and categorization.

    Attributes:
        threshold (int): Default matching threshold score (0-100)
    """

    def __init__(self, threshold: int = 80):
        """
        Initialize TextAnalyzer with a default threshold.

        Args:
            threshold (int): Default threshold for fuzzy matching (0-100)
        """
        self.threshold = threshold

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalizes text by removing accents, special characters and extra spaces.

        Args:
            text (str): Input text to normalize

        Returns:
            str: Normalized text
        """
        if pd.isna(text):
            return ""

        text = str(text)
        text = text.lower()
        text = (
            unicodedata.normalize("NFKD", text)
            .encode("ASCII", "ignore")
            .decode("ASCII")
        )
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[^a-z0-9\s]", "", text)
        return text.strip()

    def find_best_match(
        self, text: str, keywords: List[str], threshold: Optional[int] = None
    ) -> Tuple[Optional[str], float]:
        """
        Finds the best matching keyword in a list using fuzzy matching.

        Args:
            text (str): Text to search in
            keywords (list): List of keywords to search for
            threshold (int, optional): Override default threshold

        Returns:
            tuple: (best_match, score) or (None, 0) if no match above threshold
        """
        threshold = threshold or self.threshold
        text = self.normalize_text(text)

        if not text:
            return None, 0

        normalized_keywords = [self.normalize_text(k) for k in keywords]

        result = process.extractOne(
            text, normalized_keywords, scorer=fuzz.partial_ratio
        )

        if result and result[1] >= threshold:
            return keywords[normalized_keywords.index(result[0])], result[1]
        return None, 0

    def text_contains_any(
        self, text: str, keywords: List[str], threshold: Optional[int] = None
    ) -> bool:
        """
        Checks if text contains any of the keywords using fuzzy matching.

        Args:
            text (str): Text to search in
            keywords (list): List of keywords to search for
            threshold (int, optional): Override default threshold

        Returns:
            bool: True if any keyword matches above threshold
        """
        match, score = self.find_best_match(text, keywords, threshold)
        return score >= (threshold or self.threshold)

    def categorize_text(
        self,
        text: str,
        category_keywords: Dict[str, List[str]],
        default_category: str = "Otros",
        threshold: Optional[int] = None,
    ) -> Tuple[str, Optional[str], float]:
        """
        Categorizes text based on keyword matches.

        Args:
            text (str): Text to categorize
            category_keywords (dict): Dictionary of {category: [keywords]}
            default_category (str): Category to use if no matches found
            threshold (int, optional): Override default threshold

        Returns:
            tuple: (category, matched_keyword, score)
        """
        best_category = default_category
        best_keyword = None
        best_score = 0

        for category, keywords in category_keywords.items():
            match, score = self.find_best_match(text, keywords, threshold)
            if score > best_score:
                best_score = score
                best_category = category
                best_keyword = match

        return best_category, best_keyword, best_score
