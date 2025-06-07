import streamlit as st
from filter_state import FilterState


def render_filter_chips_panel(filter_state: FilterState) -> None:
    """
    Renders the active filter chips panel and handles chip removal.

    Args:
        filter_state: The FilterState object managing current filters.
    """
    st.subheader("Active Filters")
    chips = filter_state.active_chips()
    if chips:
        for i, chip in enumerate(chips):
            chip_label = f"{chip['category']}: {chip['key']} = {chip['value']}"
            if st.button(f"❌ {chip_label}", key=f"remove_{i}"):
                filter_state.remove_filter(chip["category"], chip["key"])
                st.rerun()
    else:
        st.write("No active filters.")
