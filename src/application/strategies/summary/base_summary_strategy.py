from abc import ABC, abstractmethod


class SummaryStrategy(ABC):
    @abstractmethod
    def summarize(self, content: str) -> str:
        ...
