from src.application.strategies.parser import RSSParser


class TestRSSParser:
    def test_parse_rss_returns_records(self, rss_sample: str) -> None:
        parser = RSSParser()
        records = parser.parse(rss_sample)
        assert len(records) == 2
        assert records[0].title == "Article One"
        assert records[0].author == "Author A"
        assert records[1].title == "Article Two"
        assert records[1].author == "Author B"

    def test_parse_handles_empty_feed(self) -> None:
        parser = RSSParser()
        records = parser.parse("<rss><channel></channel></rss>")
        assert records == []