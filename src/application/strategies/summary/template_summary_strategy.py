from src.application.strategies.summary.base_summary_strategy import SummaryStrategy


class TemplateSummaryStrategy(SummaryStrategy):
    def summarize(self, content: str) -> str:
        if not content or not content.strip():
            return "No content available for summarization."
        words = content.split()
        word_count = len(words)
        char_count = len(content)
        first_sentence = self._extract_first_sentence(content)
        return (
            f"Summary: This document contains {word_count} words "
            f"({char_count} characters). "
            f"It begins with: \"{first_sentence}\""
        )

    @staticmethod
    def _extract_first_sentence(text: str) -> str:
        for delimiter in (". ", "!\n", "?\n", ".\n", "!", "?"):
            idx = text.find(delimiter)
            if idx != -1:
                return text[: idx + 1].strip()
        return text[:200].strip() + ("..." if len(text) > 200 else "")
