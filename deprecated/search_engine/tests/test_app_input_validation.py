import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest


def test_empty_input_rejected():
    """
    Test that submitting an empty query is rejected.
    No results should be shown and an error message should be displayed.
    """
    at = AppTest.from_file('search_engine/app/app.py')
    at.run()
    # Submit with empty input
    at.text_input[0].set_value("")
    for btn in at.button:
        if "submit" in btn.label.lower():
            btn.click()
            break
    at.run()
    # Should show an error message
    assert any("error" in err.value.lower() or "empty" in err.value.lower() for err in at.error)
    # Should not show results
    assert not any("dummy response" in md.value.lower() for md in at.markdown)


def test_sql_injection_input_rejected():
    """
    Test that input containing SQL injection-like text is rejected.
    An error message should be displayed and no results shown.
    """
    at = AppTest.from_file('search_engine/app/app.py')
    at.run()
    sql_injection = "SELECT * FROM users; DROP TABLE posts;"
    at.text_input[0].set_value(sql_injection)
    for btn in at.button:
        if "submit" in btn.label.lower():
            btn.click()
            break
    at.run()
    assert any("error" in err.value.lower() or "invalid" in err.value.lower() for err in at.error)
    assert not any("dummy response" in md.value.lower() for md in at.markdown)


def test_profanity_input_rejected():
    """
    Test that input containing profanity is rejected.
    An error message should be displayed and no results shown.
    """
    at = AppTest.from_file('search_engine/app/app.py')
    at.run()
    at.text_input[0].set_value("This is a damn test.")
    for btn in at.button:
        if "submit" in btn.label.lower():
            btn.click()
            break
    at.run()
    assert any("error" in err.value.lower() or "profanity" in err.value.lower() for err in at.error)
    assert not any("dummy response" in md.value.lower() for md in at.markdown)


def test_length_cap_input_rejected():
    """
    Test that input exceeding the length cap is rejected.
    An error message should be displayed and no results shown.
    """
    at = AppTest.from_file('search_engine/app/app.py')
    at.run()
    long_input = "a" * 1001  # Assuming 1000 char cap
    at.text_input[0].set_value(long_input)
    for btn in at.button:
        if "submit" in btn.label.lower():
            btn.click()
            break
    at.run()
    assert any("error" in err.value.lower() or "too long" in err.value.lower() for err in at.error)
    assert not any("dummy response" in md.value.lower() for md in at.markdown)


def test_anti_spam_guard():
    """
    Test that submitting too many queries in a short time is blocked by an anti-spam guard.
    An error message should be displayed after too many submissions.
    """
    at = AppTest.from_file('search_engine/app/app.py')
    at.run()
    for i in range(6):  # Assuming a limit of 5 submissions per minute
        at.text_input[0].set_value(f"Test query {i}")
        for btn in at.button:
            if "submit" in btn.label.lower():
                btn.click()
                break
        at.run()
    assert any("spam" in err.value.lower() or "too many" in err.value.lower() for err in at.error)
    assert not any("dummy response" in md.value.lower() for md in at.markdown) 