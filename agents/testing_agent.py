WRITE_TESTS_PROMPT = """
    Given the file(s) in <file(s)>, write comprehensive unit tests for
    each function in the file(s).

    For any unit tests in Python, I want you to always use Pytest.
    Encapsulate each function's tests into a test class, named with camelcase
    (e.g., for function "foo", name the test class "TestFoo").
    Mock or patch any I/O where necessary, unless specified otherwise and
    testing specific I/O functionalities.

    Write comprehensive docstrings detailing input, output, and expected
    behavior and why that behavior is expected.
"""
