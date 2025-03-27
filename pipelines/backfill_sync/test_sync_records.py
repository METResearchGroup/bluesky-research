from pipelines.backfill_sync.handler import lambda_handler

if __name__ == "__main__":
    dids = [
        "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
        "did:plc:e4itbqoxctxwrrfqgs2rauga",
        "did:plc:gedsnv7yxi45a4g2gts37vyp",
        "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
    ]
    event = {"dids": ",".join(dids), "skip_backfill": False}
    context = {}
    lambda_handler(event, context)
