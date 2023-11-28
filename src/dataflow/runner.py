import json
from datetime import datetime
from itertools import chain
from typing import Dict, Any, Optional, Tuple, List

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
        with open(path, encoding='utf-8') as f:
            return json.load(f)

    with client.read(path, encoding='utf-8') as reader:
        return json.load(reader)


def build_source(spark: SparkSession, source: Dict[str, Any], df_by_name: Dict[str, DataFrame]) -> Dict[str, Any]:
    def _read_dataframe() -> Optional[DataFrame]:
        if source["format"].upper() == 'JSON':
            return spark.read.json(source["path"])
        return None

    output = {
        source["name"]: _read_dataframe(),
    }
    df_by_name.update(output)

    return {
        "name": source["name"],
        "format": source["format"],
        "output": output
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


def _get_validation_fields_and_funcs(trans: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Get the list of tuples (field, function) derived from transformation validations
    """
    aux = [[(val["field"], fval) for fval in val["validations"]] for val in trans["params"]["validations"]]
    return list(chain.from_iterable(aux))


def _get_validation_funcs_by_field(trans: Dict[str, Any]) -> Dict[str, Any]:
    return {val["field"]: val["validations"] for val in trans["params"]["validations"]}


def build_transformation(trans: Dict[str, Any], df_by_name: Dict[str, DataFrame]) -> Dict[str, Any]:
    def _build_validate_ok(for_ko: bool = False) -> DataFrame:
        source_df = df_by_name[trans["params"]["input"]]
        field_funcs = _get_validation_fields_and_funcs(trans)

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
        funcs_by_field = _get_validation_funcs_by_field(trans)

        # add array columns for field errors
        for field, funcs in funcs_by_field.items():
            errors = [sf.when(~_validation_func(field, f), f).otherwise("") for f in funcs]
            array_col_name = f'{field}_errors'
            ko_df = ko_df.withColumn(array_col_name, sf.array(*errors))
            ko_df = ko_df.withColumn(array_col_name, sf.array_remove(sf.col(array_col_name), ""))

        error_cols = [sf.col(f'{f}_errors') for f in funcs_by_field.keys()]

        # merge arrays of errors into a single column
        ko_df = ko_df.withColumn('arraycoderrorbyfield', sf.struct(*error_cols))
        for error_col in error_cols:
            ko_df = ko_df.drop(error_col)
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

    output = _build_output()
    df_by_name.update(output)

    return {
        "name": trans["name"],
        "type": trans["type"],
        "output": output
    }


def write_to_sink(sink: Dict[str, Any], df_by_name: Dict[str, DataFrame]) -> None:
    if sink["format"].upper() == 'JSON':
        for path in sink.get("paths", []):
            df_by_name[sink["input"]].write.json(path, mode=sink.get("saveMode", "overwrite").lower())
    elif sink["format"].upper() == "KAFKA":
        for topic in sink.get("topics", []):
            pass


def exec_dataflow(spark: SparkSession, dataflow: Dict[str, Any]) -> None:
    # store dataframe by name in this dict
    df_by_name = {}

    sources = [build_source(spark, source, df_by_name) for source in dataflow["sources"]]
    transformations = [build_transformation(trans, df_by_name) for trans in dataflow["transformations"]]

    for sink in dataflow["sinks"]:
        write_to_sink(sink, df_by_name)


def main() -> None:
    metadata_path = 'resources/test_dataflow_local.json'
    metadata = load_json(metadata_path)

    spark = SparkSession.builder.getOrCreate()

    for dataflow in metadata["dataflows"]:
        exec_dataflow(spark, dataflow)


if __name__ == "__main__":
    main()
