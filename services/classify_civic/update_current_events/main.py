"""Service that updates current events."""
from dataclasses import dataclass

# TODO: update
@dataclass
class NYTimesStory:
    """Dataclass for NYTimes stories."""
    def __init__(self, title: str, url: str, description: str, date: str):
        self.title = title
        self.url = url
        self.description = description
        self.date = date
