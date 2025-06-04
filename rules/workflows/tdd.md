# Standard Operating Procedure: Test-Driven Development (TDD) for AI Agent

This document outlines the Test-Driven Development (TDD) workflow to be followed for software development tasks. Adhering to this SOP ensures robust, verifiable code and aligns with the highest engineering practices, as expected of a 10x superstar staff engineer. This procedure is to be used in conjunction with the guidelines in `CODING_AGENT.md`.

## TDD Workflow Steps:

1.  **Understand Requirements and Define Test Cases:**
    *   Based on the user's request and feature specifications, thoroughly analyze the requirements.
    *   Identify clear input/output pairs, expected behaviors, and edge cases that can be translated into specific test cases.
    *   If requirements are ambiguous or incomplete, ask clarifying questions before proceeding, as per Core Responsibility 1 in `CODING_AGENT.md`.

2.  **Write Tests First (Red Phase):**
    *   **Action:** Generate comprehensive unit tests (using Pytest, as per project standards) that cover the defined test cases and expected functionality.
    *   **Critical Instruction:** You must explicitly operate in a TDD mode. This means you will write tests for functionality that does *not yet exist*. Avoid creating mock implementations or placeholder code for the actual functionality being tested at this stage. The tests should define the contract for the code to be written.
    *   **Reference:** Adhere to the "Testing Criteria" outlined in `CODING_AGENT.md`.

3.  **Run Tests and Confirm Failure:**
    *   **Action:** Execute the newly written tests.
    *   **Expected Outcome:** The tests *must* fail, as the corresponding implementation code has not yet been written. This confirms that the tests are correctly targeting the unimplemented functionality and are not trivially passing. This is the "Red" state of the Red-Green-Refactor cycle.
    *   **Critical Instruction:** Do not write any implementation code at this stage. The sole purpose is to validate the tests themselves.

4.  **Commit Failing Tests:**
    *   **Action:** Once satisfied that the tests accurately reflect the requirements and fail as expected, commit the test files to version control.
    *   **Commit Message:** Use the standard format specified in `CODING_AGENT.md`, for example: `[test] Add failing tests for <feature/module_name> (via Composer LLM)`.

5.  **Write Implementation Code (Green Phase):**
    *   **Action:** Develop the *minimum amount of code necessary* to make the previously written (and failing) tests pass.
    *   **Critical Instruction:** Your primary focus is to satisfy the test requirements. Avoid introducing any functionality not explicitly covered or demanded by the tests.
    *   **Constraint:** Do *not* modify the existing tests to make them pass. The tests represent the fixed specification against which the code is being developed.

6.  **Iterate: Run Tests, Implement, Verify:**
    *   **Action:** After writing the initial implementation, run all relevant tests.
    *   **If All Tests Pass:** The code has reached the "Green" state. Proceed to Step 7 (Refactor and Finalize).
    *   **If Any Tests Fail:**
        *   Analyze the failures meticulously, following the "Error Fixing Process" if necessary.
        *   Adjust the implementation code to address the identified issues.
        *   Re-run the tests.
        *   Repeat this cycle (adjust code, run tests) until all tests pass. This iterative process is central to TDD.

7.  **Refactor and Verify (Refactor Phase):**
    *   **Action (Refactor):** With all tests passing, review and refactor the implementation code. Improve its structure, clarity, efficiency, and maintainability without altering its external behavior (i.e., all tests must continue to pass after refactoring).
    *   **Action (Verify Overfitting - Optional but Recommended):** Consider if the implementation is robust or if it merely "overfits" to the specific test cases. This might involve thinking about additional edge cases or scenarios. If capabilities allow and it's deemed necessary, this step could involve strategies to ensure broader correctness.

8.  **Commit Passing Code and Tests:**
    *   **Action:** Once all tests pass, the code has been refactored, and you (and the user, if applicable) are satisfied with the implementation, commit both the implementation code and any test files (which should ideally be unchanged from step 4, or only minimally adjusted for setup/teardown if essential).
    *   **Commit Message:** Use the standard format, e.g., `[feat] Implement <feature/module_name> passing all tests (via Composer LLM)` or `[fix] Correct <bug_description> ensuring tests pass (via Composer LLM)`.

## Guiding Principles for TDD with AI Agent:

*   **Clear Targets for Iteration:** TDD provides explicit, verifiable targets (passing tests) for you to iterate against. This enables systematic progress, self-correction, and incremental improvement until the desired functionality is achieved.
*   **Incremental and Shippable Development:** TDD naturally supports breaking down complex problems into smaller, manageable, and shippable increments, aligning with core engineering best practices.
*   **Enhanced Code Quality and Confidence:** A comprehensive suite of tests, developed through TDD, significantly increases confidence in the code's correctness, reduces regressions, and improves overall reliability and maintainability.

By rigorously following this TDD SOP, you will consistently produce high-quality, robust, and well-tested code, thereby upholding the standards of technical excellence.
