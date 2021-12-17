import psycopg2
from sqlalchemy import create_engine
from employee_residence.utilities import DataPreprocessor
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_postgre_db(config):
    try:
        psycopg_conn = psycopg2.connect(
            host=config["ADDRESS"], user=config["USR"], password=config["PWD"]
        )
        psycopg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = psycopg_conn.cursor()
        cursor.execute(f"CREATE DATABASE \"{config['DATABASE']}\";")
        psycopg_conn.close()
    except psycopg2.errors.DuplicateDatabase:
        print("database already existing on the server, continuing...")
    except:
        raise Exception("could not create a database")


def create_psycopg_connection(config):
    psycopg_conn = psycopg2.connect(
        host=config["ADDRESS"],
        database=config["DATABASE"],
        user=config["USR"],
        password=config["PWD"],
    )
    try:
        cursor = psycopg_conn.cursor()
        cursor.execute(f"CREATE EXTENSION postgis;")
        cursor.commit()
    except:
        pass

    return psycopg_conn


def create_sql_alchemy_connection(config):
    con = create_engine(
        f"postgresql://{config['USR']}:{config['PWD']}@{config['ADDRESS']}:{config['PORT']}/{config['DATABASE']}"
    )
    return con


class MetricsCalculationModule:
    def __init__(self, config):
        self.config = config
        # create database
        create_postgre_db(self.config)
        # initiate psycopg db connection
        self.psycopg_db_connection = create_psycopg_connection(self.config)
        # initiate data preprocessor utility
        self.data_preprocessor = DataPreprocessor(
            config=self.config,
        )
        # fetch and prepare the data
        self.data_preprocessor.generate_employees_dataframe()
        self.data_preprocessor.add_longitude_latitude_to_data()
        self.data_preprocessor.add_average_income_to_data()
        self.data = self.data_preprocessor.employees

    def push_data_to_db(self):
        self.data.to_sql(
            "employees",
            index=False,
            con=create_sql_alchemy_connection(self.config),
            if_exists="replace",
        )

        # add column of type geographic_point on the db
        psycopg_cursor = self.psycopg_db_connection.cursor()
        psycopg_cursor.execute(
            "ALTER TABLE employees "
            "ADD geographic_point geography(POINT,4326);"
            'UPDATE employees SET geographic_point = ST_SetSRID(ST_MakePoint("Lon", "Lat"), 4326)::geography;'
            "CREATE INDEX geographic_point_gix ON employees USING GIST ( geographic_point );"
        )
        self.psycopg_db_connection.commit()

    def calculate_closest_employee_to_company_HQ(self):
        df = pd.read_sql(
            f"SELECT \"Name\", \"Full address\", ST_Distance('SRID=4326;POINT({self.config['HQ_LONGITUDE']} {self.config['HQ_LATITUDE']})'::geography, geographic_point) "
            "FROM employees "
            f"ORDER BY geographic_point <-> 'SRID=4326;POINT({self.config['HQ_LONGITUDE']} {self.config['HQ_LATITUDE']})'::geography "
            "LIMIT 1;",
            con=create_sql_alchemy_connection(self.config),
        )
        return df

    def calculate_employees_living_within_radius_from_HQ(self, radius=10000):
        df = pd.read_sql(
            'SELECT "Name", "Full address" '
            "FROM employees "
            f"WHERE ST_DWithin(geographic_point, 'SRID=4326;POINT({self.config['HQ_LONGITUDE']} {self.config['HQ_LATITUDE']})'::geography, {radius});",
            con=create_sql_alchemy_connection(self.config),
        )

        return df

    def generate_list_of_employees_living_on_the_wealthiest_regions(self):
        df = pd.read_sql(
            'SELECT "Name", "Full address" '
            "FROM employees "
            "WHERE average_income IS NOT NULL "
            "ORDER BY average_income DESC "
            "LIMIT 3;",
            con=create_sql_alchemy_connection(self.config),
        )

        return df
