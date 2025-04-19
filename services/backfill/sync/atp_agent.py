import os
import requests
import time

default_service = "https://bsky.social"


class AtpAgent:
    def __init__(self, service: str = default_service):
        self.service = service
        self.handle = os.getenv("BLUESKY_HANDLE") or ""
        self.password = os.getenv("BLUESKY_PASSWORD") or ""
        self.session = None
        self.headers = None

    def _create_session(self):
        """Creates authenticated session.

        See: https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/server/createSession.json
        """
        json = {"identifier": self.handle, "password": self.password}
        response = requests.post(
            f"{self.service}/xrpc/com.atproto.server.createSession",
            json=json,
        )
        return response.json()

    def _auth(self):
        """Authenticates session.

        See: https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/server/createSession.json
        """
        self.session = self._create_session()
        access_jwt = self.session.get("accessJwt")
        self.headers = {"Authorization": f"Bearer {access_jwt}"}

    def foo(self):
        """https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/repo/getRecord.json"""
        pass

    def list_records(
        self,
        repo: str,
        collection: str,
        limit: int = 100,
        cursor: str = None,
    ):
        """Lists records from a collection.

        https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/repo/listRecords.json
        """
        params = {
            "repo": repo,
            "collection": collection,
            "limit": limit,
            "cursor": cursor,
        }
        url = f"{self.service}/xrpc/com.atproto.repo.listRecords"
        response = requests.get(url, headers=self.headers, params=params)
        return response

    def get_repo(self, did: str):
        """Gets a repo from the PDS.

        https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/sync/getRepo.json#L3

        Only works if the current `service` is the same as the profile PDS
        endpoint.
        """
        root_url = os.path.join(self.service, "xrpc")
        joined_url = os.path.join(root_url, "com.atproto.sync.getRepo")
        full_url = f"{joined_url}?did={did}"
        response = requests.get(full_url, headers=self.headers)
        # return response.json()
        return response  # TODO: can't do .json(). Need to get .content.

    def get_repos(self, dids: list[str]):
        """Gets repos from the PDS.

        https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/sync/getRepos.json
        """
        results = []
        request_times = []
        total_dids = len(dids)
        print(f"Getting {total_dids} repos...")
        start_time = time.perf_counter()
        for i, did in enumerate(dids):
            request_start = time.perf_counter()
            results.append(self.get_repo(did))
            request_time = time.perf_counter() - request_start
            request_times.append(request_time)
            print(f"Request {i} time: {request_time} seconds")
        end_time = time.perf_counter()
        print(f"Average request time: {sum(request_times) / len(request_times)}")
        print(f"Total time: {end_time - start_time}\tTotal requests: {total_dids}")
        return results


if __name__ == "__main__":
    pass
