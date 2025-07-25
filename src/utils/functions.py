import json
from pyspark.sql.types import StructType, StructField

from common.type_map import type_map

def load_json_schema(schema_path: str) -> StructType:
    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    schema = StructType([
        StructField(field["name"], type_map[field["type"]], field["nullable"])
        for field in schema_json
    ])

    return schema