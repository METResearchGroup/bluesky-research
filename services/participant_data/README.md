# Participant data

This service stores the non-Bluesky participant data (e.g., onboarding, questionnaires, etc.).

This service also maps the identifying information for our study users to their Bluesky handles and DIDs.

The following diagram shows how the data in this service relates to the Bluesky user data:

![Bluesky participants](diagram.png)

The functionality is in the main.py file:
```{python}
python main.py
```

To reset the database of users, run:
```{python}
python helper.py
```

Be careful as this drops all users that are in the table. Be sure to have a list of all the users backed up if this is the case.
