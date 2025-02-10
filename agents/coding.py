from agents.documentation_agent import REWRITE_DOCSTRING_PROMPT

CREATE_NEW_SERVICE_PROMPT = f"""
    You will create a new microservice. You will receive instructions in this format:


    ```
    [Name]

    [Details]

    [Parameters]

    [Steps]

    ```

    Your task is:
    1. Create the microservice, within `services/<service_name>`.
    2. Create a README, describing the details of the microservice and its
    purpose.
    3. Create a main.py, in which there is a main function that takes a payload
    dict with the specified parameters. Use <fetch_posts_used_in_feeds/main.py>
    and <uris_to_created_at/main.py> as your template. Unless specified, the 
    service should take, at minimum, a start_date and end_date and
    excluded_partition_dates args, in addition to other kwargs.
    4. Create a helper.py. This should encapsulate the core functionality of
    the service. Make the code modular. See
    <fetch_posts_used_in_feeds/helper.py> and <uris_to_created_at/helper.py>
    for inspiration. The code should follow the following flow:
        - Load data
            - Use "load_data_from_local_storage" for any interactions that
            involve loading data. See "load_preprocessed_posts" for an example.
        - Do operations
        - Export data
        
    Add type hinting and docs, as per {REWRITE_DOCSTRING_PROMPT}.

    5. Add unit tests for all your functionality. See
    <fetch_posts_used_in_feeds/tests> and <uris_to_created_at/tests> for
    examples.
    ```
"""
