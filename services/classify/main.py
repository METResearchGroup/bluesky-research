"""Service for performing ML classifications."""

def main(event: dict, context: dict) -> int:
    """Perform ML classifications.
    
    We load in the texts to be classified, and then run them through our
    classification logic.
    """
    return 0


if __name__ == "__main__":
    event = {}
    context = {}
    main(event=event, context=context)
