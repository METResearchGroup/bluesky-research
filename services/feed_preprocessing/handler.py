def main(event: dict, context: dict) -> int:
    return 0


if __name__ == "__main__":
    event = {"process_batch": True, "batch_size": 100}
    context = {}
    main(event=event, context=context)