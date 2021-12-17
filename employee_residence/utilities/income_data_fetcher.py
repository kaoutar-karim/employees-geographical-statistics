import json
import requests


class IncomeDataFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.table_name = None
        self.postal_number_code = None
        self.measures_code = None
        self.average_income_column = None
        self.url = None

    def get_table_name(self):
        list_of_tables_url = f"{self.config['BASE_URL']}/{self.config['INCOME_YEAR']}"
        response = requests.get(list_of_tables_url)
        if response.status_code == 404:
            raise Exception(
                f"Average income data for year {self.config['INCOME_YEAR']} is not available ( url : {self.config['BASE_URL']}"
            )
        elif response.status_code != 200:
            raise Exception(f"Issue with url {list_of_tables_url} : {response.reason}")
        else:
            for res in response.json():
                if self.config["TABLE_DESC"] in res["text"]:
                    self.table_name = res["id"]
        return self.table_name

    def generate_url(self):
        table_name = self.table_name or self.get_table_name()
        self.url = (
            f"{self.config['BASE_URL']}/{self.config['INCOME_YEAR']}/{table_name}"
        )
        return self.url

    def get_metadata(self):
        url = self.url or self.generate_url()
        return requests.get(url).json()

    def get_json_query_attributes(self):
        metadata = self.get_metadata()
        postal_code_dict = metadata["variables"][0]
        measures_dict = metadata["variables"][1]

        self.postal_number_code = postal_code_dict["code"]
        self.measures_code = measures_dict["code"]

        column_description_mapping_dict = zip(
            measures_dict["values"], measures_dict["valueTexts"]
        )
        for column, description in column_description_mapping_dict:
            if "Average income" in description:
                self.average_income_column = column

    def generate_json_query(self):
        self.get_json_query_attributes()
        query = json.dumps(
            dict(
                query=[
                    dict(
                        code=self.measures_code,
                        selection=dict(
                            filter="item", values=[self.average_income_column]
                        ),
                    )
                ],
                response=dict(format="json-stat2"),
            )
        )

        return query

    def fetch_data_from_remote(self):
        self.url = self.url or self.generate_url()
        data = requests.post(
            url=self.url, json=json.loads(self.generate_json_query())
        ).json()

        return data
