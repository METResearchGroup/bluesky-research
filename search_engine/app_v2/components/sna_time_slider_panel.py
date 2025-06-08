from typing import Any, Dict, Tuple
import streamlit as st
import datetime
import time


def rerun_streamlit():
    """Call st.rerun() if available, else st.experimental_rerun()."""
    try:
        st.rerun()
    except AttributeError:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()


def render_sna_time_slider_panel(state: Dict[str, Any]) -> None:
    """
    Render the Time Slider Animation panel for the SNA tab.
    Renders a date range slider and a play button to animate the network over the selected range.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
    """
    st.subheader("Time Slider Animation")
    start_date = datetime.date(2024, 6, 1)
    end_date = datetime.date(2024, 6, 14)
    date_range = st.slider(
        "Select Date Range",
        min_value=start_date,
        max_value=end_date,
        value=(start_date, end_date),
        format="YYYY-MM-DD",
        key="sna_time_slider",
    )
    # Play button
    if "sna_playing" not in st.session_state:
        st.session_state["sna_playing"] = False
    play_col, _ = st.columns([1, 5])
    play_pressed = play_col.button(
        "Play", disabled=st.session_state["sna_playing"], key="sna_play_btn"
    )
    if play_pressed and not st.session_state["sna_playing"]:
        st.session_state["sna_playing"] = True
        # Animate through each day in the selected range
        current = date_range[0]
        while current <= date_range[1]:
            state["date_range"] = (current, current)
            update_graph_and_metrics(state, (current, current))
            st.session_state["sna_current_day"] = current
            time.sleep(0.3)
            current += datetime.timedelta(days=1)
            rerun_streamlit()
        st.session_state["sna_playing"] = False
    else:
        # If not playing, just update for the selected range
        update_graph_and_metrics(state, date_range)


def update_graph_and_metrics(
    state: Dict[str, Any], date_range: Tuple[datetime.date, datetime.date]
) -> None:
    """
    Update the state with the selected date range. In a real app, this would update the graph and metrics.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
        date_range (Tuple[datetime.date, datetime.date]): The selected date range.
    """
    state["date_range"] = date_range
