from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType


type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "date": DateType()
    }