import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest


def test_app_renders_title_and_description():
    """
    Test that the Streamlit app renders the correct title, subtitle, and description.
    The app should display a project-relevant title, subtitle, and description at the top of the page.
    """
    at = AppTest.from_file('search_engine/app/main.py')
    at.run()
    assert at.title[0].value == "Bluesky Semantic Search"
    # Subtitle should be present and distinct
    assert any(
        getattr(sub, 'value', '').lower().startswith("search the bluesky dataset")
        for sub in at.subheader
    )
    # Description should be present
    assert any(
        "semantic search engine for bluesky data" in md.value.lower()
        for md in at.markdown
    )


def test_app_query_submission_and_results():
    """
    Test that the app has an explicit submit button for queries.
    Submitting a query via the button shows a dummy response and expandable table.
    """
    at = AppTest.from_file('search_engine/app/main.py')
    at.run()
    print("Button labels:", [btn.label for btn in at.button])
    # There should be a submit button
    assert any("submit" in btn.label.lower() for btn in at.button)
    # Simulate entering a query and clicking submit
    at.text_input[0].set_value("What are the most liked posts this week?")
    # Find the submit button and click it
    for btn in at.button:
        if "submit" in btn.label.lower():
            btn.click()
            break
    at.run()  # Rerun to reflect state update
    # Dummy response should be present
    assert any(
        "dummy response" in md.value.lower() or "example result" in md.value.lower()
        for md in at.markdown
    )
    # Expandable table should be present
    assert at.dataframe or at.table


def test_app_example_queries_clickable():
    """
    Test that example queries are rendered as clickable elements.
    Clicking an example query should populate the input box with that text.
    """
    at = AppTest.from_file('search_engine/app/main.py')
    at.run()
    # There should be at least one clickable example query (button or similar)
    assert any("example query" in btn.label.lower() for btn in at.button)
    # Simulate clicking the first example query button
    example_btn = next(btn for btn in at.button if "example query" in btn.label.lower())
    example_btn.click()
    at.run()
    # The text input should now be populated with the example query text
    assert at.text_input[0].value.strip() != ""


def test_app_feedback_section():
    """
    Test that a feedback section is present under the results area.
    It should include a text field, thumbs up/down, and a submit feedback button.
    """
    at = AppTest.from_file('search_engine/app/main.py')
    at.run()
    # There should be a feedback text input
    assert any("feedback" in inp.label.lower() for inp in at.text_input)
    # There should be thumbs up/down buttons (can be radio, button, or similar)
    assert any("thumbs up" in btn.label.lower() or "thumbs down" in btn.label.lower() for btn in at.button) or any("thumb" in radio.label.lower() for radio in at.radio)
    # There should be a submit feedback button
    assert any("submit feedback" in btn.label.lower() for btn in at.button)


def test_app_renders_input_and_results():
    """
    Test that the Streamlit app renders a text input box and a results area.
    The input box should allow the user to enter a query, and the results area should be present.
    Submitting a query should show a dummy response and an expandable table.
    """
    at = AppTest.from_file('search_engine/app/main.py')
    at.run()
    print("Button labels:", [btn.label for btn in at.button])
    assert any("query" in inp.label.lower() for inp in at.text_input)
    # Simulate entering a query and clicking submit
    at.text_input[0].set_value("What are the most liked posts this week?")
    for btn in at.button:
        if "submit" in btn.label.lower():
            btn.click()
            break
    at.run()  # Rerun to reflect state update
    # Dummy response should be present
    assert any(
        "dummy response" in md.value.lower() or "example result" in md.value.lower()
        for md in at.markdown
    )
    # Expandable table should be present
    assert at.dataframe or at.table


def test_app_renders_example_queries():
    """
    Test that the Streamlit app displays example queries below the input box.
    The app should suggest at least one example query relevant to Bluesky data.
    """
    at = AppTest.from_file('search_engine/app/main.py')
    at.run()
    example_queries = [md.value for md in at.markdown if "example queries" in md.value.lower()]
    assert example_queries, "No example queries found in the app output." 