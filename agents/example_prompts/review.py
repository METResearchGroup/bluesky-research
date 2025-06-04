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

INITIAL_CONTEXT_GATHERING_CRITERIA = """
    - What problem is this code trying to solve?
    - What are the key requirements and constraints?
    - Which systems/services does this code interact with?
"""

HIGH_LEVEL_ARCHITECTURE_REVIEW_CRITERIA = """
    - Does the solution match the scale of the problem?
    - Are there existing patterns/solutions that could be reused?
    - How does this fit into the broader system architecture?
    - Are there potential performance bottlenecks?
"""

CODE_ORGANIZATION_REVIEW_CRITERIA = """
    - Is the code logically organized and easy to navigate?
    - Are responsibilities clearly separated (Single Responsibility Principle)?
    - Are there clear interfaces between components?
    - Is there appropriate encapsulation?
    - Does the code follow team/organization conventions?
"""

CODE_STYLE_REVIEW_CRITERIA = """
    - Is the code readable and maintainable?
    - Is the code consistent with the overall style of the project?
    - Is the code efficient in terms of time and space complexity?
"""

PERFORMANCE_SCALABILITY_REVIEW_CRITERIA = """
    - Is the code performant and scalable?
    - Are there any potential performance bottlenecks?
    - Are there any opportunities for parallelization?
    - Are there any opportunities for caching?
    - Are there obvious performance issues?
        - Are there unnecessary loops or computations?
        - Are there inefficient data structures?
        - Are there N+1 query problems?
    - How will this code behave under load? How much load is it expected
    to be able to handle?
    - Are there potential memory leaks?
    - Is caching used appropriately?
"""

TESTING_REVIEW_CRITERIA = """
    - Are the tests comprehensive and cover all the important functionality?
    - Are the tests easy to understand and maintain?
    - Are the tests easy to run and interpret?
    - Are the tests easy to write and maintain?
    - Is the code testable?
    - Are there sufficient unit tests?
    - Are edge cases covered?
    - Are there integration tests where needed?
    - Is there proper mocking of external dependencies?
"""

CODE_MAINTAINABILITY_REVIEW_CRITERIA = """
    - Is the code self-documenting?
    - Are complex sections properly commented?
    - Are variable/function names clear and meaningful?
    - Is there unnecessary complexity that could be simplified?
    - Is there proper error handling and logging?
"""

SECURITY_REVIEW_CRITERIA = """
    - Are there any security vulnerabilities?
    - Are there any potential security issues?
    - Are there any security best practices being followed?
"""

BUSINESS_LOGIC_REVIEW_CRITERIA = """
    - Does the code correctly implement the business logic?
    - Are there any potential business logic issues?
    - Are there any opportunities for improvement in the business logic?
"""

DOCUMENTATION_REVIEW_CRITERIA = """
    - Are the docstrings clear and helpful?
    - Are the docstrings complete and accurate?
    - Are the docstrings easy to understand and maintain?
    - Is there sufficient documentation?
    - Are architectural decisions explained?
    - Are there clear usage examples?
    - Is there API documentation if needed?
"""

FUTUREPROOFING_REVIEW_CRITERIA = """
    - Is the code future-proof?
    - Are there any potential future issues?
    - Are there any opportunities for improvement in the future-proofing?
    - Would I be comfortable maintaining this code?
    - Would a junior engineer understand this code?
    - Is this solution future-proof?
    - Are there any tech debt implications?
    - What could go wrong in production?
"""

REVISE_CODE_PROMPT = """
    In your response, do the following:
    - Explain your findings. Keep it brief and direct. Provide examples where
    helpful. Use the stated criteria as the basis for your review.
        - Use a header, [FINDINGS], to explain your findings. Enumerate
        your findings one by one.
    - Give proposed changes.
        - Use a header, [SUGGESTED CHANGES], to explain suggested changes
        and what improvements they provide. Enumerate the changes one by one.
    - Detail next steps.
        - Use a header, [NEXT STEPS], to explain what the next steps are
        for the development of this code. Be specific. Recommend how to 
        improve the high-level design of the service as well as how well the
        code meets the stated purpose of the service.
        - For this, use the criteria in {FUTUREPROOFING_REVIEW_CRITERIA} as
        the basis for your recommendations.

    An example output format would be:

    **[FINDINGS]**
    <findings>
    </findings>
    ---
    **[SUGGESTED CHANGES]**
    <suggested_changes>
    </suggested_changes>
    ---
    **[NEXT STEPS]**
    <next_steps>
    </next_steps>
    ---

    If there are no issues, state that there are no issues.
"""

REVIEW_CODE_QUALITY_PROMPT = f"""
    Imagine that you are a senior staff software engineer reviewing the code
    in <file>. Review the code along the following criteria:

    - Code Organization:
        {CODE_ORGANIZATION_REVIEW_CRITERIA}
    - Code Style:
        {CODE_STYLE_REVIEW_CRITERIA}
    - Testing:
        {TESTING_REVIEW_CRITERIA}
    - Code Maintainability:
        {CODE_MAINTAINABILITY_REVIEW_CRITERIA}

    {REVISE_CODE_PROMPT}
"""

REVIEW_ARCHITECTURE_PROMPT = f"""
    Imagine that you are a senior staff software engineer reviewing the code
    in <file>. Review the code along the following criteria:

    - High-Level Architecture Review:
        {HIGH_LEVEL_ARCHITECTURE_REVIEW_CRITERIA}
    - Performance and Scalability:
        {PERFORMANCE_SCALABILITY_REVIEW_CRITERIA}
    - Business Logic:
        {BUSINESS_LOGIC_REVIEW_CRITERIA}
    - Documentation:
        {DOCUMENTATION_REVIEW_CRITERIA}

    {REVISE_CODE_PROMPT}
"""

REVIEW_CODE_PROMPT = f"""
    Imagine that you are a senior staff software engineer reviewing the code
    in <file>. Review the code along the following criteria:

    - Initial Context Gathering: 
        {INITIAL_CONTEXT_GATHERING_CRITERIA}
    - High-Level Architecture Review:
        {HIGH_LEVEL_ARCHITECTURE_REVIEW_CRITERIA}
    - Code Organization:
        {CODE_ORGANIZATION_REVIEW_CRITERIA}
    - Code Style:
        {CODE_STYLE_REVIEW_CRITERIA}
    - Performance and Scalability:
        {PERFORMANCE_SCALABILITY_REVIEW_CRITERIA}
    - Testing:
        {TESTING_REVIEW_CRITERIA}
    - Code Maintainability:
        {CODE_MAINTAINABILITY_REVIEW_CRITERIA}
    - Security:
        {SECURITY_REVIEW_CRITERIA}
    - Business Logic:
        {BUSINESS_LOGIC_REVIEW_CRITERIA}
    - Documentation:
        {DOCUMENTATION_REVIEW_CRITERIA}

    {REVISE_CODE_PROMPT}
"""
