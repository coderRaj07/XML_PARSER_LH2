from abc import ABC, abstractmethod

from src.domain.entities.record import Record


class ParserStrategy(ABC):
    @abstractmethod
    def parse(self, raw_xml: str) -> list[Record]:
        ...
