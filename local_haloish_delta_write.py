#!/usr/bin/env python3
#
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import random
from datetime import datetime
import typing

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import AtomicType


DEFAULT_INPUT_PATH = "~/data2/haloish-data"

_INPUT_FILE_PATH_COLUMN = "__kratos_input_file_path__"
_DELTA_TABLE_PROPERTIES = {
    "delta.autoOptimize": "true",
}


def _get_helper_column_name(existing_columns: typing.List[str]) -> str:
    """Return a metadata column name that will not collide with input data."""
    existing = {column.casefold() for column in existing_columns}
    candidate = _INPUT_FILE_PATH_COLUMN
    suffix = 1
    while candidate.casefold() in existing:
        candidate = f"{_INPUT_FILE_PATH_COLUMN}_{suffix}"
        suffix += 1
    return candidate


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Read local HALO-ish Parquet data with schema merging, count records by "
            "input file, and append to a Delta table slice."
        )
    )
    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH,
        help=f"Local Parquet input path. Default: {DEFAULT_INPUT_PATH}",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Delta table name to write, for example database.table_name",
    )
    parser.add_argument(
        "--app-name",
        default="local-haloish-delta-write",
        help="Spark application name",
    )
    return parser.parse_args()


def _drop_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _format_table_properties(properties: typing.Dict[str, str]) -> str:
    return ", ".join(
        f"'{key}' = '{value}'"
        for key, value in sorted(properties.items())
    )


def _select_partition_column(df) -> str:
    candidates = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, AtomicType)
    ]
    if not candidates:
        raise ValueError("no top-level atomic columns available for Delta partitioning")
    return random.choice(candidates)


def _create_delta_table(
    spark: SparkSession, table_name: str, df, partition_column: str
) -> None:
    _drop_table(spark, table_name)
    (
        df.limit(0)
        .write.option("mergeSchema", "true")
        .partitionBy(partition_column)
        .mode("error")
        .format("delta")
        .saveAsTable(table_name)
    )
    spark.sql(
        f"ALTER TABLE {table_name} "
        f"SET TBLPROPERTIES ({_format_table_properties(_DELTA_TABLE_PROPERTIES)})"
    )
    print(f"Created Delta table {table_name} partitioned by {partition_column}")


def main():
    args = parse_args()
    input_path = os.path.expanduser(args.input_path)

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    df = None

    try:
        start = datetime.utcnow()
        read_df = spark.read.option("mergeSchema", "true").parquet(input_path)
        input_file_path_col = _get_helper_column_name(read_df.columns)

        # input_file_name() must be evaluated during the scan; add it before caching.
        df = read_df.withColumn(input_file_path_col, F.input_file_name())
        output_df = df.drop(input_file_path_col)
        # partition_column = _select_partition_column(output_df)
        partition_column = "evaluation_id"

        _create_delta_table(spark, args.table, output_df, partition_column)

        df.cache()

        file_counts = {
            row[input_file_path_col]: row["count"]
            for row in df.groupBy(input_file_path_col).count().collect()
        }

        num_records = sum(file_counts.values())
        print(f"Expect to write {num_records} records")
        for path, count in sorted(file_counts.items()):
            print(f"{count}\t{path}")

        (
            output_df.write.option("mergeSchema", "true")
            .partitionBy(partition_column)
            .mode("append")
            .format("delta")
            .saveAsTable(args.table)
        )

        elapsed = int((datetime.utcnow() - start).total_seconds())
        ts = datetime.utcnow().strftime("%FT%T.%f")[0:-3] + "Z"
        spark_app_id = spark.sparkContext.applicationId
        print(f"Wrote {num_records} records to {args.table}")
        print(f"Processed timestamp: {ts}")
        print(f"Spark app {spark_app_id} with partition column {partition_column} elapsed seconds: {elapsed}")
    finally:
        try:
            if df is not None:
                df.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
