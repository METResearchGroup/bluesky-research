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
