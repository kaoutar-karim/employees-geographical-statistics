import json
import re
import pandas as pd
import numpy as np

from employee_residence.utilities.geographic_data_fetcher import GeographicDataFetcher
from employee_residence.utilities.income_data_fetcher import IncomeDataFetcher


class DataPreprocessor:
    def __init__(self, config):
        self.employees = pd.DataFrame()
        self.config = config
        self.geographicDataFetcher = GeographicDataFetcher(config)
        self.incomeDataFetcher = IncomeDataFetcher(config)

    @staticmethod
    def split_street(row):
        if re.search("\d{5}", row["Street"]):
            street_components = re.findall(r"^(.*) (\d{5}) *(\w*)$", row["Street"])
            row["Street"] = street_components[0][0]
            row["Postal code"] = street_components[0][1]
            row["City"] = (
                street_components[0][2]
                if street_components[0][2] != ""
                else row["City"]
            )
        return row

    def generate_employees_dataframe(self):
        employees = pd.read_excel(self.config["EXCEL_FILE"], engine="openpyxl")
        employees.dropna(axis=0, how="all", inplace=True)
        employees.dropna(axis=1, how="all", inplace=True)
        employees = employees.apply(func=DataPreprocessor.split_street, axis="columns")
        employees["City"] = employees["City"].apply(
            lambda x: x.strip("' ,.").title() if isinstance(x, str) else x
        )
        employees["Postal code"] = employees["Postal code"].apply(
            lambda x: str(x).zfill(5) if str(x).isnumeric() else x
        )
        employees["Street"] = employees["Street"].apply(
            lambda x: x.strip("' ,.").title()
        )
        employees["Full address"] = (
            employees["Street"]
            + " "
            + employees["Postal code"]
            + " "
            + employees["City"].fillna("")
        )
        self.employees = employees

    def add_longitude_latitude_to_data(self):
        if self.employees.empty:
            self.generate_employees_dataframe()

        list_of_addresses = self.employees["Full address"].values.tolist()
        self.geographicDataFetcher.get_geog_data(
            json.dumps(list_of_addresses, ensure_ascii=False)
        )
        geog_data = []
        for row in self.geographicDataFetcher.geog_data_json:
            geog_data.append(
                {
                    "query": row["query"]["text"],
                    "Lon": row["lon"],
                    "Lat": row["lat"],
                    "County": row["county"],
                }
            )
        geog_df = pd.DataFrame(geog_data)

        self.employees = self.employees.set_index("Full address", drop=False).join(
            geog_df.set_index("query")
        )

    def add_average_income_to_data(self):
        pxnet_stat_response = self.incomeDataFetcher.fetch_data_from_remote()
        pxnet_stat_postal_codes = pxnet_stat_response["dimension"]["Postinumeroalue"][
            "category"
        ]["index"].keys()
        income_df = pd.DataFrame(
            np.column_stack(
                (list(pxnet_stat_postal_codes), pxnet_stat_response["value"])
            ),
            columns=["postal_code", "average_income"],
        )
        self.employees = self.employees.set_index("Postal code", drop=False).join(
            income_df.set_index("postal_code")
        )
