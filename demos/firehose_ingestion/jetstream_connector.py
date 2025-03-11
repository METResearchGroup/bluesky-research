"""Connector for the Jetstream pipeline."""
import asyncio
import websockets


PUBLIC_INSTANCES = [
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-west.bsky.network",
]

class JetstreamConnector:
    """Connector for the Jetxstream pipeline."""

    def __init__(self):
        pass


    @staticmethod
    def get_public_instances() -> list[str]:
        """Get the public instances."""
        return PUBLIC_INSTANCES
    

    def generate_uri(self, instance: str, payload: dict) -> str:
        """Generate a URI for a given instance and payload."""
        if instance not in PUBLIC_INSTANCES:
            raise ValueError(f"Instance {instance} is not a public instance.")

        query_params = []
        for key, value in payload.items():
            query_params.append(f"{key}={value}")
        query_string = ",".join(query_params)

        return f"wss://{instance}/subscribe?{query_string}"

    
    async def connect_to_jetstream(self, instance: str, payload: dict) -> str:
        """Connect to the Jetstream pipeline and consume messages."""
        uri = self.generate_uri(instance, payload)
        async with websockets.connect(uri) as websocket:
            message = await websocket.recv()
            return message


if __name__ == "__main__":
    connector = JetstreamConnector()
    print(connector.generate_uri("jetstream2.us-east.bsky.network", {"wantedCollections": "app.bsky.feed.post"}))
