import json
import time
import requests


class GeographicDataFetcher:
    def __init__(self, config):
        self.config = config
        self.geog_data_json = None

    def create_batch_job(self, addresses):
        res = requests.post(
            f"https://api.geoapify.com/v1/batch/geocode/search?apiKey={self.config['API_KEY']}&format=json",
            json=json.loads(addresses),
        )
        return res.json()["url"]

    def get_geog_data(self, addresses):
        url = self.create_batch_job(addresses)
        res = requests.get(url)
        while res.status_code != 200:
            time.sleep(5)
            res = requests.get(url)

        self.geog_data_json = res.json()
