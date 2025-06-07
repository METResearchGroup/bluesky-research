import streamlit as st
from filter_state import FilterState
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data
import pandas as pd


def render_query_preview_panel(
    filter_state: FilterState, show: bool, sample_posts
) -> None:
    """
    Renders the Query Preview panel, showing the top N rows of filtered sample data with lazy loading.
    Args:
        filter_state: The FilterState object managing current filters.
        show: Whether to display the preview (only after query submission).
        sample_posts: The list of sample post dicts to use (already scaled if needed).
    """
    if not show:
        return
    st.subheader("Query Preview")
    filters = filter_state.filters
    with st.spinner("Filtering data and generating preview..."):
        print(f"[DIAG] QueryPreview: sample_posts={len(sample_posts)} records")
        # Get all filtered results up to max_results for the count
        all_filtered = filter_and_preview_sample_data(
            filters, sample_posts, preview=False
        )
        print(
            f"[DIAG] QueryPreview: all_filtered={len(all_filtered)} records after filtering"
        )
        total_matches = len(all_filtered)
        # Clear state feedback
        filters_no_max = {
            k: v
            for k, v in filters.items()
            if not (k == "General" and list(v.keys()) == ["max_results"])
        }
        if not filters_no_max:
            st.info("No filters selected. Showing all data.")
        st.markdown(f"*Found {total_matches} results matching your query*")

        # Filter summary sentence
        if not filters_no_max:
            st.markdown("**No filters applied.**")
        else:
            summary_parts = []
            for cat, params in filters.items():
                if cat == "General" and list(params.keys()) == ["max_results"]:
                    continue
                for k, v in params.items():
                    summary_parts.append(f"{cat}: {k}={v}")
            summary = ", ".join(summary_parts)
            st.markdown(f"**Filters:** {summary}")

        # --- Lazy loading logic ---
        max_preview = 100
        default_preview = 10
        if "preview_rows_mode" not in st.session_state:
            st.session_state["preview_rows_mode"] = "less"
        mode = st.session_state["preview_rows_mode"]
        if mode == "more":
            num_rows = min(max_preview, len(all_filtered))
        else:
            num_rows = min(default_preview, len(all_filtered))
        preview_rows = all_filtered[:num_rows]
        if preview_rows:
            df = pd.DataFrame(preview_rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
            # Button logic
            if len(all_filtered) > default_preview:
                if mode == "less" and len(all_filtered) > default_preview:
                    if st.button("Show more", key="show_more_preview"):
                        st.session_state["preview_rows_mode"] = "more"
                        st.rerun()
                elif mode == "more":
                    if st.button("Show less", key="show_less_preview"):
                        st.session_state["preview_rows_mode"] = "less"
                        st.rerun()
        else:
            st.warning(
                "No matching sample data for the current filters. Try broadening your filters or removing some criteria."
            )
