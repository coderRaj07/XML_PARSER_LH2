from src.application.strategies.summary.base_summary_strategy import SummaryStrategy


class SummaryService:
    def __init__(self, strategy: SummaryStrategy) -> None:
        self._strategy = strategy

    def generate_summary(self, content: str) -> str:
        return self._strategy.summarize(content)
