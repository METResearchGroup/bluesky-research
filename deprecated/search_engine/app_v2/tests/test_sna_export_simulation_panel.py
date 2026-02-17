import csv
import datetime
from io import StringIO
from unittest.mock import MagicMock, patch

import networkx as nx

from search_engine.app_v2.components import sna_export_simulation_panel


def _mock_columns(count: int):
    cols = []
    for _ in range(count):
        col = MagicMock()
        col.__enter__.return_value = col
        col.__exit__.return_value = False
        cols.append(col)
    return cols


def test_graph_to_edge_csv_escapes_and_sorts_edges() -> None:
    graph = nx.Graph()
    graph.add_edge("z,last", "=danger")
    graph.add_edge("a", "b")

    csv_text = sna_export_simulation_panel.graph_to_edge_csv(graph)
    rows = list(csv.reader(StringIO(csv_text)))

    assert rows[0] == ["source", "target"]
    assert rows[1] == ["'=danger", "z,last"]
    assert rows[2] == ["a", "b"]


def test_graph_to_node_metrics_csv_escapes_and_sorts_nodes() -> None:
    graph = nx.Graph()
    graph.add_edge("@formula", "normal")
    graph.add_node("x")

    csv_text = sna_export_simulation_panel.graph_to_node_metrics_csv(graph)
    rows = list(csv.reader(StringIO(csv_text)))

    assert rows[0] == ["node", "degree"]
    assert rows[1] == ["'@formula", "1"]
    assert rows[2] == ["normal", "1"]
    assert rows[3] == ["x", "0"]


def test_render_panel_uses_download_buttons_with_context_suffix() -> None:
    graph = nx.Graph()
    graph.add_edge("alice", "bob")
    state = {
        "edge_type": "retweet",
        "date_range": (datetime.date(2024, 6, 1), datetime.date(2024, 6, 3)),
    }

    with patch("streamlit.subheader"), patch("streamlit.caption"), patch(
        "streamlit.columns", return_value=_mock_columns(3)
    ), patch("streamlit.download_button") as mock_download, patch("streamlit.info"):
        sna_export_simulation_panel.render_sna_export_simulation_panel(state, graph)

    assert mock_download.call_count == 3
    file_names = [call.kwargs["file_name"] for call in mock_download.call_args_list]
    assert "edge_list_retweet_2024-06-01_2024-06-03.csv" in file_names
    assert "node_metrics_retweet_2024-06-01_2024-06-03.csv" in file_names
    assert "network_retweet_2024-06-01_2024-06-03.gexf" in file_names
    assert all(call.kwargs["disabled"] is False for call in mock_download.call_args_list)


def test_render_panel_disables_downloads_for_empty_graph() -> None:
    graph = nx.Graph()
    state = {}

    with patch("streamlit.subheader"), patch("streamlit.caption"), patch(
        "streamlit.columns", return_value=_mock_columns(3)
    ), patch("streamlit.download_button") as mock_download, patch(
        "streamlit.info"
    ) as mock_info:
        sna_export_simulation_panel.render_sna_export_simulation_panel(state, graph)

    mock_info.assert_called_once()
    assert mock_download.call_count == 3
    assert all(call.kwargs["disabled"] is True for call in mock_download.call_args_list)
