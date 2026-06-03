import html
import re
from typing import Optional

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer as SumyTextRankSummarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words

from src.application.strategies.summary.base_summary_strategy import SummaryStrategy


class ExtractiveSummaryStrategy(SummaryStrategy):
    def __init__(self, language: str = "english", sentences_count: int = 5) -> None:
        self.language = language
        self.sentences_count = sentences_count

    def summarize(self, content: str) -> str:
        if not content or not content.strip():
            return ""

        clean = self._strip_html(content)
        sentences = self._split_sentences(clean)

        textrank = self._summarize_textrank(clean)
        if textrank:
            return textrank

        tfidf = self._summarize_tfidf(sentences)
        if tfidf:
            return tfidf

        lsa = self._summarize_lsa(clean)
        if lsa:
            return lsa

        return " ".join(sentences[:min(self.sentences_count, len(sentences))])

    def _summarize_textrank(self, content: str) -> Optional[str]:
        try:
            parser = PlaintextParser.from_string(content, Tokenizer(self.language))
            stemmer = Stemmer(self.language)
            summarizer = SumyTextRankSummarizer(stemmer)
            summarizer.stop_words = get_stop_words(self.language)
            sentences = list(summarizer(parser.document, self.sentences_count))
            if sentences:
                return " ".join(str(s) for s in sentences)
        except Exception:
            return None
        return None

    def _summarize_tfidf(self, sentences: list[str]) -> Optional[str]:
        try:
            vectorizer = TfidfVectorizer(stop_words="english")
            tfidf_matrix = vectorizer.fit_transform(sentences)
            scores = np.array(tfidf_matrix.sum(axis=1)).flatten()
            top_indices = scores.argsort()[-self.sentences_count:][::-1]
            top_indices.sort()
            return " ".join(sentences[i] for i in top_indices)
        except Exception:
            return None

    def _summarize_lsa(self, content: str) -> Optional[str]:
        try:
            from sumy.summarizers.lsa import LsaSummarizer as SumyLsaSummarizer

            parser = PlaintextParser.from_string(content, Tokenizer(self.language))
            stemmer = Stemmer(self.language)
            summarizer = SumyLsaSummarizer(stemmer)
            summarizer.stop_words = get_stop_words(self.language)
            sentences = list(summarizer(parser.document, self.sentences_count))
            if sentences:
                return " ".join(str(s) for s in sentences)
        except Exception:
            return None
        return None

    @staticmethod
    def _strip_html(text: str) -> str:
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _split_sentences(text: str) -> list[str]:
        text = re.sub(r"\s+", " ", text).strip()
        raw = re.split(r"(?<=[.!?])\s+", text)
        return [s.strip() for s in raw if s.strip()]
