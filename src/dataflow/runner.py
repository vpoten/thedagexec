import json
from typing import Dict, Any, Optional

from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


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
        "output": _read_dataframe(),
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
