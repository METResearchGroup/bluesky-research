import pytest
from unittest import mock
from search_engine.app_v2.filter_state import FilterState
import streamlit
from search_engine.app_v2.components import export_templates_panel

class TestExportPanel:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.filter_state = FilterState()
        # Patch session state for each test
        self._orig_session_state = dict(streamlit.session_state)
        yield
        streamlit.session_state.clear()
        streamlit.session_state.update(self._orig_session_state)

    def test_export_button_triggers_csv_download(self):
        """
        Export button is enabled after query submission and downloads CSV with header and at least one row.
        """
        streamlit.session_state["show_query_preview"] = True
        with mock.patch("streamlit.radio") as mock_radio, \
             mock.patch("streamlit.subheader"), \
             mock.patch("streamlit.divider"), \
             mock.patch("streamlit.write"), \
             mock.patch("streamlit.download_button") as mock_download_button:
            mock_radio.return_value = "CSV"
            export_templates_panel.render_export_templates_panel(self.filter_state)
            assert mock_download_button.called, "Download button was not called."
            args, kwargs = mock_download_button.call_args
            assert kwargs["file_name"].endswith(".csv"), "Exported file is not CSV."
            assert kwargs["mime"] == "text/csv"
            data = kwargs["data"]
            csv_text = data.decode("utf-8")
            lines = csv_text.strip().split("\n")
            assert len(lines) > 1, "CSV should have header and at least one row."
            assert "," in lines[0] and "date" in lines[0], "CSV header missing."

    def test_export_button_triggers_parquet_download(self):
        """
        Export button is enabled after query submission and downloads Parquet file.
        """
        streamlit.session_state["show_query_preview"] = True
        with mock.patch("streamlit.radio") as mock_radio, \
             mock.patch("streamlit.subheader"), \
             mock.patch("streamlit.divider"), \
             mock.patch("streamlit.write"), \
             mock.patch("streamlit.download_button") as mock_download_button:
            mock_radio.return_value = "Parquet"
            export_templates_panel.render_export_templates_panel(self.filter_state)
            assert mock_download_button.called, "Download button was not called."
            args, kwargs = mock_download_button.call_args
            assert kwargs["file_name"].endswith(".parquet"), "Exported file is not Parquet."
            assert kwargs["mime"] == "application/octet-stream"
            data = kwargs["data"]
            assert isinstance(data, bytes) and len(data) > 0, "Parquet data is empty or not bytes."

    def test_export_with_no_filters(self):
        """
        Exporting with no filters after query submission downloads the full sample dataset.
        """
        streamlit.session_state["show_query_preview"] = True
        with mock.patch("streamlit.radio") as mock_radio, \
             mock.patch("streamlit.subheader"), \
             mock.patch("streamlit.divider"), \
             mock.patch("streamlit.write"), \
             mock.patch("streamlit.download_button") as mock_download_button:
            mock_radio.return_value = "CSV"
            export_templates_panel.render_export_templates_panel(self.filter_state)
            assert mock_download_button.called, "Download button was not called."
            args, kwargs = mock_download_button.call_args
            assert kwargs["file_name"].endswith(".csv"), "Exported file is not CSV."
            assert kwargs["mime"] == "text/csv"
            data = kwargs["data"]
            csv_text = data.decode("utf-8")
            lines = csv_text.strip().split("\n")
            assert len(lines) > 1, "CSV should have header and at least one row."
            assert "," in lines[0] and "date" in lines[0], "CSV header missing."

    def test_export_button_disabled_before_query_submission(self):
        """
        Export button is not shown and info message is displayed if query has not been submitted.
        """
        streamlit.session_state["show_query_preview"] = False
        with mock.patch("streamlit.radio") as mock_radio, \
             mock.patch("streamlit.subheader"), \
             mock.patch("streamlit.divider"), \
             mock.patch("streamlit.write"), \
             mock.patch("streamlit.download_button") as mock_download_button, \
             mock.patch("streamlit.info") as mock_info:
            export_templates_panel.render_export_templates_panel(self.filter_state)
            assert not mock_download_button.called, "Download button should not be shown."
            assert mock_info.called, "Info message should be shown when export is disabled." 