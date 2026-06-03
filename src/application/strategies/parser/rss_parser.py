from datetime import datetime, timezone
from typing import Optional

import feedparser

from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.domain.entities.record import Record


class RSSParser(ParserStrategy):
    def parse(self, raw_xml: str) -> list[Record]:
        parsed = feedparser.parse(raw_xml)
        records: list[Record] = []
        for entry in parsed.entries:
            published = self._parse_date(entry)
            records.append(
                Record(
                    title=entry.get("title", ""),
                    author=self._get_author(entry),
                    published_date=published,
                    description=entry.get("summary", ""),
                    content=entry.get("content", [{}])[0].get("value", "")
                    if entry.get("content")
                    else "",
                    source_link=entry.get("link", ""),
                )
            )
        return records

    @staticmethod
    def _parse_date(entry: dict) -> Optional[datetime]:
        from dateutil import parser as dateparser

        published_str = entry.get("published") or entry.get("updated")
        if published_str:
            try:
                dt = dateparser.parse(published_str)
                if dt is not None and dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _get_author(entry: dict) -> str:
        author = entry.get("author")
        if author:
            return author
        if entry.get("authors"):
            return entry["authors"][0].get("name", "")
        return ""
