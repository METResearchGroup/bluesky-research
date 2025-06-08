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
    st.caption(
        "Use the date range slider to select a time window. Click 'Play' to animate the network over time and observe changes in structure and metrics."
    )
    start_date = datetime.date(2024, 6, 1)
    end_date = datetime.date(2024, 6, 14)
    # Animation state management
    if "sna_playing" not in st.session_state:
        st.session_state["sna_playing"] = False
    if "sna_current_day" not in st.session_state:
        st.session_state["sna_current_day"] = None
    if "sna_animation_start" not in st.session_state:
        st.session_state["sna_animation_start"] = None
    if "sna_animation_end" not in st.session_state:
        st.session_state["sna_animation_end"] = None
    if "sna_last_render_time" not in st.session_state:
        st.session_state["sna_last_render_time"] = None

    # Determine slider value
    if (
        st.session_state["sna_playing"]
        and st.session_state["sna_current_day"] is not None
    ):
        slider_value = (
            st.session_state["sna_animation_start"],
            st.session_state["sna_current_day"],
        )
    else:
        slider_value = state.get("date_range", (start_date, end_date))

    date_range = st.slider(
        "Select Date Range",
        min_value=start_date,
        max_value=end_date,
        value=slider_value,
        format="YYYY-MM-DD",
        key="sna_time_slider",
    )
    state["date_range"] = date_range

    play_col, _ = st.columns([1, 5])
    play_pressed = play_col.button(
        "Play", disabled=st.session_state["sna_playing"], key="sna_play_btn"
    )

    # Animation logic
    if play_pressed and not st.session_state["sna_playing"]:
        # Start animation
        st.session_state["sna_playing"] = True
        st.session_state["sna_animation_start"] = date_range[0]
        st.session_state["sna_animation_end"] = date_range[1]
        st.session_state["sna_current_day"] = date_range[0]
        st.session_state["sna_last_render_time"] = time.time()
        state["date_range"] = (date_range[0], date_range[0])
        update_graph_and_metrics(state, (date_range[0], date_range[0]))
        rerun_streamlit()
    elif st.session_state["sna_playing"]:
        # Animation in progress
        current = st.session_state["sna_current_day"]
        anim_start = st.session_state["sna_animation_start"]
        anim_end = st.session_state["sna_animation_end"]
        last_render = st.session_state["sna_last_render_time"]
        now = time.time()
        # Wait until at least 1 second has passed since last render
        if last_render is None or now - last_render >= 1.0:
            # Advance to next day
            next_day = current + datetime.timedelta(days=1)
            if next_day > anim_end:
                # Animation done
                st.session_state["sna_playing"] = False
                st.session_state["sna_current_day"] = None
                st.session_state["sna_animation_start"] = None
                st.session_state["sna_animation_end"] = None
                st.session_state["sna_last_render_time"] = None
                # Set slider to full range after animation
                state["date_range"] = (anim_start, anim_end)
                update_graph_and_metrics(state, (anim_start, anim_end))
                rerun_streamlit()
            else:
                # Update to next day (right handle moves)
                st.session_state["sna_current_day"] = next_day
                st.session_state["sna_last_render_time"] = time.time()
                state["date_range"] = (anim_start, next_day)
                update_graph_and_metrics(state, (anim_start, next_day))
                rerun_streamlit()
        else:
            # Not enough time has passed; just update graph/metrics for current day
            state["date_range"] = (anim_start, current)
            update_graph_and_metrics(state, (anim_start, current))
    else:
        # Not playing, just update for the selected range
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
