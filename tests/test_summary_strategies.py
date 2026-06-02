from src.application.strategies.summary import (
    TemplateSummaryStrategy,
    ExtractiveSummaryStrategy,
)


class TestTemplateSummaryStrategy:
    def test_summarize_returns_template(self, sample_content: str) -> None:
        strategy = TemplateSummaryStrategy()
        result = strategy.summarize(sample_content)
        assert "words" in result
        assert "characters" in result
        assert sample_content.split()[0] in result

    def test_summarize_empty_content(self) -> None:
        strategy = TemplateSummaryStrategy()
        result = strategy.summarize("")
        assert "No content" in result


class TestExtractiveSummaryStrategy:
    def test_summarize_returns_fewer_sentences(self, sample_content: str) -> None:
        strategy = ExtractiveSummaryStrategy(sentences_count=3)
        result = strategy.summarize(sample_content)
        assert result
        assert len(result) < len(sample_content) or result == sample_content

    def test_summarize_short_content(self) -> None:
        strategy = ExtractiveSummaryStrategy()
        short = "Hello world."
        result = strategy.summarize(short)
        assert result == short

    def test_summarize_empty(self) -> None:
        strategy = ExtractiveSummaryStrategy()
        assert strategy.summarize("") == ""
        assert strategy.summarize("   ") == ""