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

# designed to be run by Composer as part of an agentic workflow.
REVIEW_ALL_TESTS_PROMPT = """
    Review the tests listed in @python-ci.yml . Run each of the pytest tests in
    the terminal and check if they work and are all passing. For any that are
    not passing, review the error logs and make a note of them to present as a
    report later. Then detail which tests aren't passing, a summary of the
    errors and what can be fixed from both the test and the file that the test
    is testing. 

    First, activate the conda environment, with
    "conda activate bluesky-research". Then run the tests one by one.
"""
