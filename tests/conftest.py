import pytest


@pytest.fixture
def rss_sample() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Article One</title>
      <author>Author A</author>
      <pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate>
      <description>Description one.</description>
      <content:encoded><![CDATA[Content one.]]></content:encoded>
    </item>
    <item>
      <title>Article Two</title>
      <author>Author B</author>
      <pubDate>Tue, 02 Jan 2024 00:00:00 GMT</pubDate>
      <description>Description two.</description>
    </item>
  </channel>
</rss>"""


@pytest.fixture
def sample_content() -> str:
    return (
        "Machine learning is a subset of artificial intelligence. "
        "It involves training models on data to make predictions. "
        "Supervised learning uses labeled data for training. "
        "Unsupervised learning finds patterns in unlabeled data. "
        "Reinforcement learning uses rewards to guide behavior. "
        "Deep learning uses neural networks with many layers. "
        "Natural language processing enables computers to understand text. "
        "Computer vision allows machines to interpret images. "
        "These technologies are transforming industries worldwide. "
        "The future of AI depends on responsible development."
    )
