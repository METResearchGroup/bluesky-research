"""Service for updating bot accounts database."""


def classify_user_as_bot(user: dict) -> bool:
    """Determines if a user is a possible bot."""
    return False


def update_bot_accounts() -> None:
    """Updates the bot accounts database.
    
    Goes through the user accounts and updates the bot accounts database
    based on the probability that a user is a bot account.

    We aim to be more conservative than not in our predictions - we're OK with
    slightly lower recall if it means that we have higher precision.
    """
    # load all users

    # load users that have already been run through this service.
    # we assume that if a user has already been run through this service, that
    # we don't need to classify them again.

    # for users that haven't been classified yet, run them through the service,
    # then add the results to the DB
    pass


def load_bot_accounts() -> set:
    """Loads the set of user IDs of classified bot accounts from the DB."""
    return set()


if __name__ == "__main__":
    update_bot_accounts()