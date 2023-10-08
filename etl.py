import os
import requests
import pandas as pd
import sqlite3


class ETL:
    def __init__(
        self,
        api_key: str,
        query: str,
        database_name: str,
        table_name: str,
        page_size=100,
    ):
        self.api_key = api_key
        self.query = query
        self.page_size = page_size
        self.database_name = database_name
        self.table_name = table_name
        self.base_url = "https://newsapi.org/v2/everything"
        self.data = None
        self.extracted_data = None

    def extract(self):
        try:
            params = {
                "apiKey": self.api_key,
                "q": self.query,
                "pageSize": self.page_size,
            }
            response = requests.get(self.base_url, params=params)

            if response.status_code == 200:
                json = response.json()
                self.data = pd.read_json(json.get("articles"))

            else:
                print(
                    f"Failed to fetch data from NewsAPI. Status Code: {response.status_code}"
                )

        except Exception as error:
            print(f"Error extracting data: {str(error)}")

    def transform(self):
        try:
            self.extracted_data = self.data.drop_duplicates()

        except Exception as e:
            print(f"Error transforming data: {str(e)}")

    def load(self):
        try:
            conn = sqlite3.connect(self.database_name)
            self.extracted_data.to_sql(
                self.table_name, conn, if_exists="replace", index=False
            )
            conn.close()
            print(f"Data loaded into {self.database_name}.{self.table_name}")

        except Exception as e:
            print(f"Error loading data: {str(e)}")


if __name__ == "__main__":
    news_api_key = os.environ.get('news_api_key')
    query_term = "technology"

    db_name = "news_data.db"
    table_name = "news_articles"

    pipeline = ETL(
        api_key=news_api_key,
        query=query_term,
        database_name=db_name,
        table_name=table_name,
    )
    pipeline.extract()
    pipeline.transform()
    pipeline.load()
