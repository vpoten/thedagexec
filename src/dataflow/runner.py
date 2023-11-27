import json
from datetime import datetime
from itertools import chain
from typing import Dict, Any, Optional

from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf


def create_hdfs_client(url: str, user: str) -> InsecureClient:
    return InsecureClient(url, user=user)


def load_json(path: str, client: InsecureClient = None) -> Dict[str, Any]:
    """
    Loads json from HDFS or local filesystem
    """
    if client is None:
        return json.loads(path)

    with client.read(path, encoding='utf-8') as reader:
        return json.load(reader)


def build_source(spark: SparkSession, source: Dict[str, Any]) -> Dict[str, Any]:
    def _read_dataframe() -> Optional[DataFrame]:
        if source["format"].upper() == 'JSON':
            return spark.read.json(source["path"])
        return None

    return {
        "name": source["name"],
        "format": source["format"],
        "output": {
            source["name"]: _read_dataframe(),
        }
    }


def _zeroary_func(name: str) -> Any:
    """
    Get the result of a pre-defined 0-ary function
    """
    if name == "current_timestamp":
        return datetime.now()
    return None


def _validation_func(field: str, func_name: str) -> Any:
    """
    Creates a pre-defined validation for a single column
    """
    if func_name == "notEmpty":
        return sf.col(field) != sf.lit("")
    elif func_name == "notNull":
        return sf.col(field).isNotNull()
    return None


def build_transformation(trans: Dict[str, Any], df_by_name: Dict[str][DataFrame]) -> Dict[str, Any]:
    def _build_validate_ok(for_ko: bool = False) -> DataFrame:
        source_df = df_by_name[trans["params"]["input"]]
        field_funcs = [[(val["field"], fval) for fval in val["validations"]] for val in trans["params"]["validations"]]
        field_funcs = list(chain.from_iterable(field_funcs))

        # initialize the condition with the first function
        if for_ko is True:
            condition = ~_validation_func(field_funcs[0][0], field_funcs[0][1])
        else:
            condition = _validation_func(field_funcs[0][0], field_funcs[0][1])

        if len(field_funcs) > 1:
            for field, func_name in field_funcs[1:]:
                if for_ko is True:
                    condition |= ~_validation_func(field, func_name)
                else:
                    condition &= _validation_func(field, func_name)
        return source_df.filter(condition)

    def _build_validate_ko() -> DataFrame:
        ko_df = _build_validate_ok(for_ko=True)
        # TODO add error by field
        return ko_df

    def _build_add_fields_transform() -> DataFrame:
        source_df = df_by_name[trans["params"]["input"]]
        for new_field in trans["params"]["addFields"]:
            source_df = source_df.withColumn(new_field["name"], sf.lit(_zeroary_func(new_field["function"])))
        return source_df

    def _build_output() -> Optional[Dict[str, DataFrame]]:
        if trans["type"] == "validate_fields":
            return {
                f'{trans["name"]}_ok': _build_validate_ok(),
                f'{trans["name"]}_ko': _build_validate_ko(),
            }
        elif trans["type"] == "add_fields":
            return {
                trans["name"]: _build_add_fields_transform(),
            }
        return None

    return {
        "name": trans["name"],
        "type": trans["type"],
        "output": _build_output()
    }


def write_to_sink(sink: Dict[str, Any], df_by_name: Dict[str][DataFrame]) -> None:
    if sink["format"].upper() == 'JSON':
        for path in sink.get("paths", []):
            df_by_name[sink["input"]].write.json(path, mode=sink.get("saveMode", "overwrite").lower())
    elif sink["format"].upper() == "KAFKA":
        for topic in sink.get("topics", []):
            pass


def main() -> None:
    pass


if __name__ == "__main__":
    main()
