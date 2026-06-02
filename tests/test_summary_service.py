import pytest

from src.application.services.summary_service import SummaryService
from src.application.strategies.summary import TemplateSummaryStrategy


class TestSummaryService:
    def test_generate_summary_delegates_to_strategy(self, sample_content: str) -> None:
        strategy = TemplateSummaryStrategy()
        service = SummaryService(strategy=strategy)
        result = service.generate_summary(sample_content)
        assert "words" in result

    def test_dependency_injection(self) -> None:
        strategy = TemplateSummaryStrategy()
        service = SummaryService(strategy=strategy)
        assert service._strategy is strategy
