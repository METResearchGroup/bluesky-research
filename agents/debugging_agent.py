DEBUGGING_PROMPT = """
    I get these errors while running my code: <error message>.

    Explain what is the cause of these errors as well as how to fix it, step
    by step. Reference the behavior in <files> if needed. 

    Explain to me what is the intended behavior in that function, where in the
    code that is done, and how the code goes about implementing that behavior.
    Then, use that to update the functionality and tests where required.
"""

DEBUG_TESTS_PROMPT = """
    I get these errors while running my tests: <error message>.

    Explain what is the cause of these errors as well as how to fix it, step
    by step. Reference the behavior in <files> if needed. 

    Explain to me what is the intended behavior in that function, where in the
    code that is done, and how the code goes about implementing that behavior.
    Then, use that to update the functionality and tests where required.
"""

REVIEW_TESTS_PROMPT = """
    Given the test file <test_file>, explain what is being tested here,
    step by step. Be thorough and comprehensive. Then compare that to the
    functionality in <file>. If the tests are not testing the functionality
    correctly, explain how to fix it. Explain if I am missing any tests
    that I should add. Explain those tests, why they should be added, and add
    those tests, if needed.

    Then, if any are missing comprehensive docstrings, add those docstrings.
    I want docstrings detailing input, output, and expected behavior and why
    that behavior is expected.
"""

REVIEW_CODE_PROMPT = """
    Given the code in <file>, explain what is being done here, step by step.
    Be thorough and comprehensive. Then compare that to the functionality in
    <file>. If the code is not implementing the functionality correctly,
    explain how to fix it.
"""
