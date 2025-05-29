import requests
import logging
import time
from typing import Callable, Dict, List, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class APIClient:
    def __init__(
        self,
        base_url: str,
        timeout: int = 10,
        max_retries: int = 3,
        per_page: Optional[int] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.per_page = per_page

    def _request_with_retry(self, url: str, method: str = "GET", **kwargs) -> Any:
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.request(method, url, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logging.warning(f"Retry {attempt}/{self.max_retries} - URL: {url} - Error: {e}")
                if attempt == self.max_retries:
                    logging.error("Max retries reached.")
                    raise
                time.sleep(1)

    def fetch_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        per_page_param: Optional[str] = "per_page",
        stop_condition: Optional[Callable[[List[Any]], bool]] = None,
    ) -> List[Any]:
        if params is None:
            params = {}

        if self.per_page and per_page_param:
            params[per_page_param] = self.per_page

        page = 1
        results = []

        while True:
            params[page_param] = page
            url = f"{self.base_url}/{endpoint}"
            data = self._request_with_retry(url, params=params)

            if stop_condition and stop_condition(data):
                break
            if not data:
                break

            results.extend(data)
            page += 1

        logging.info(f"Fetched {len(results)} records")
        return results

    def fetch(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{endpoint}"
        return self._request_with_retry(url, params=params)

# class BreweryAPI:
#     def __init__(self):
#         self.client = APIClient(
#             base_url="https://api.openbrewerydb.org/v1",
#             per_page=100
#         )

#     def get_all_breweries(self) -> List[Dict[str, Any]]:
#         return self.client.fetch_paginated(endpoint="breweries")

#     def get_brewery_metadata(self) -> Dict[str, Any]:
#         return self.client.fetch(endpoint="breweries/meta")


# if __name__ == "__main__":
#     brewery_api = BreweryAPI()
#     data = brewery_api.get_all_breweries()
#     metadata = brewery_api.get_brewery_metadata()
