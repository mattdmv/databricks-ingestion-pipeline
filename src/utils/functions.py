import json
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    StructType,
    StructField,
)


type_map = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "date": DateType(),
}


def load_json_schema(schema_path: str) -> StructType:
    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    schema = StructType(
        [
            StructField(field["name"], type_map[field["type"]], field["nullable"])
            for field in schema_json
        ]
    )

    return schema
