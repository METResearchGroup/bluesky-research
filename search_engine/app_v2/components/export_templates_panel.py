import streamlit as st
import pandas as pd
import io
import datetime
from search_engine.app_v2.filter_state import FilterState
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data

TEMPLATES = [
    {
        "name": "Toxic Posts Last Month",
        "filters": {
            "Temporal": {"date_range": "2024-05-01 to 2024-05-31"},
            "Sentiment": {"toxicity": "Toxic"},
        },
    },
    {
        "name": "Political Posts by Left-Leaners",
        "filters": {
            "Political": {"political": "Yes", "slant": "left"},
        },
    },
    {
        "name": "Climate Hashtag in June",
        "filters": {
            "Temporal": {"date_range": "2024-06-01 to 2024-06-10"},
            "Content": {"hashtags": ["#climate"]},
        },
    },
]

EXPORT_FORMATS = ["CSV", "Parquet"]


def render_export_templates_panel(filter_state: FilterState, sample_posts) -> None:
    """
    Renders the Export Panel with export format selector and export button.
    Only allows export after a query has been submitted.
    Args:
        filter_state: The FilterState object managing current filters.
        sample_posts: The list of sample post dicts to use (already scaled if needed).
    """
    st.subheader("Export Results Panel")

    # Only allow export if a query has been submitted
    if not st.session_state.get("show_query_preview", False):
        st.info("Submit a query to enable export.")
        return

    # --- Export Format Selector ---
    export_format = st.radio(
        "Export Format", EXPORT_FORMATS, horizontal=True, key="export_format_radio"
    )

    print(f"[DIAG] ExportPanel: sample_posts={len(sample_posts)} records")

    filtered = filter_and_preview_sample_data(
        filter_state.filters, sample_posts, preview=False
    )
    print(f"[DIAG] ExportPanel: filtered={len(filtered)} records after filtering")
    record_count = len(filtered)
    # Mock data size: assume 1KB per record
    data_size_kb = record_count
    # Convert to appropriate unit
    if data_size_kb >= 1_000_000:
        data_size = f"{data_size_kb / 1_000_000:.1f} GB"
    elif data_size_kb >= 1_000:
        data_size = f"{data_size_kb / 1_000:.1f} MB"
    else:
        data_size = f"{data_size_kb} KB"
    st.write(f"**Estimated records:** {record_count}")
    st.write(f"**Estimated data size:** {data_size}")

    # --- Column selection for export (Nice-to-Have) ---
    if filtered:
        all_columns = list(filtered[0].keys())
        selected_columns = st.multiselect(
            "Select columns to include in export",
            options=all_columns,
            default=all_columns,
            help="Choose which columns to include in the exported file.",
        )
    else:
        selected_columns = []

    # --- Export Button ---
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results_{now}.{export_format.lower()}"

    # Floating Action Button (FAB) for export
    st.markdown(
        """
        <style>
        .fab-export {
            position: fixed;
            bottom: 32px;
            right: 32px;
            z-index: 9999;
        }
        .fab-export button {
            background-color: #007bff;
            color: white;
            border-radius: 50px;
            padding: 16px 32px;
            font-size: 18px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            border: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    fab_container = st.container()
    with fab_container:
        st.markdown('<div class="fab-export">', unsafe_allow_html=True)
        export_clicked = st.button(f"Export as {export_format}", key="fab_export_btn")
        st.markdown("</div>", unsafe_allow_html=True)
    if export_clicked:
        st.toast("Export started...")
        st.info(
            f"You are about to export {record_count} records as {export_format} ({data_size})."
        )
        confirm = st.button("Confirm Export", key="confirm_export_btn")
        if confirm:
            with st.spinner("Preparing export file..."):
                df = pd.DataFrame(filtered)
                if selected_columns:
                    df = df[selected_columns]
                if export_format == "CSV":
                    buf = io.StringIO()
                    df.to_csv(buf, index=False)
                    data = buf.getvalue().encode("utf-8")
                    mime = "text/csv"
                else:
                    buf = io.BytesIO()
                    df.to_parquet(buf, index=False)
                    data = buf.getvalue()
                    mime = "application/octet-stream"
            st.success("Export completed! Your file is ready for download.")
            st.download_button(
                label=f"Download {export_format}",
                data=data,
                file_name=filename,
                mime=mime,
                key="export_download_button",
            )

    # --- Have questions or feedback panel ---
    with st.expander("Have questions or feedback?", expanded=False):
        st.markdown(
            """
        Can't find the data you're looking for? Looking for more data than just a 1 week sample? Interested in contributing or collaborating with us? <u>Reach out!</u>
        """,
            unsafe_allow_html=True,
        )
        contact = st.text_input("Your email or contact info", key="contact_info")
        message = st.text_area("Your message", key="contact_message")
        if st.button("Submit Feedback", key="submit_feedback_btn"):
            st.success("Thank you for reaching out! We'll get back to you soon.")
            # TODO: implement later.
            print(f"[DIAG] ExportPanel: contact_info={contact}")
            print(f"[DIAG] ExportPanel: contact_message={message}")
