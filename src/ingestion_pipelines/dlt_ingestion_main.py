import dlt

from src.common.ingestion_flow import bronze_ingestion_flow
from src.utils.functions import load_json_schema


landing_path = spark.conf.get("landing_path")
schema = load_json_schema("../schemas/mock_schema.json")


@dlt.table(name="fake_bronze", comment=f"Data ingested from csv files in landing zone")
def bronze_table():
    return bronze_ingestion_flow(path=landing_path, schema=schema)
