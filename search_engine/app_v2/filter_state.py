from typing import Dict, Any, List


class FilterState:
    """
    Manages the state of filters for the Filter Builder Panel.
    Allows adding, removing, clearing filters, and listing active filters as chips.
    """

    def __init__(self) -> None:
        """Initializes an empty filter state."""
        self.filters: Dict[str, Dict[str, Any]] = {}

    def add_filter(self, category: str, key: str, value: Any) -> None:
        """
        Adds or updates a filter in the given category.

        Args:
            category: The filter category (e.g., 'Temporal').
            key: The filter key (e.g., 'date_range').
            value: The filter value.
        """
        if category not in self.filters:
            self.filters[category] = {}
        self.filters[category][key] = value

    def remove_filter(self, category: str, key: str) -> None:
        """
        Removes a filter from the given category.

        Args:
            category: The filter category.
            key: The filter key to remove.
        """
        if category in self.filters and key in self.filters[category]:
            del self.filters[category][key]
            if not self.filters[category]:
                del self.filters[category]

    def clear_filters(self) -> None:
        """
        Clears all filters from the state.
        """
        self.filters.clear()

    def active_chips(self) -> List[Dict[str, Any]]:
        """
        Returns a list of active filters as chip dicts.

        Returns:
            A list of dicts, each with 'category', 'key', and 'value'.
        """
        chips = []
        for category, kv in self.filters.items():
            for key, value in kv.items():
                chips.append({"category": category, "key": key, "value": value})
        return chips
