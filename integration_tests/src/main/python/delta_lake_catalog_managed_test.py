# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import spark_jvm, unity_catalog_storage_root, unity_catalog_uri
from delta_lake_utils import (assert_rapids_delta_write, assert_rapids_gpu_delete_ran,
                              delta_meta_allow, delta_writes_enabled_conf,
                              is_oss_delta_lake_42)
from marks import allow_non_gpu, delta_lake, unity_catalog
from spark_session import with_cpu_session, with_gpu_session


pytestmark = pytest.mark.skipif(
    not is_oss_delta_lake_42(),
    reason="OSS Unity Catalog managed tables require OSS Delta Lake 4.2.0")

_CATALOG = "unity"
_SCHEMA = "default"
_CATALOG_MANAGED_PROPERTY = "delta.feature.catalogManaged"
_UC_TABLE_ID_PROPERTY = "io.unitycatalog.tableId"
_STATIC_TOKEN = "static-token"


def _api_client(jvm, uri):
    token_config = jvm.java.util.HashMap()
    token_config.put("type", "static")
    token_config.put("token", _STATIC_TOKEN)
    token_provider = jvm.io.unitycatalog.client.auth.TokenProvider.create(token_config)
    return jvm.io.unitycatalog.client.ApiClientBuilder.create() \
        .uri(uri) \
        .tokenProvider(token_provider) \
        .build()


@pytest.fixture(scope="module")
def unity_catalog_server():
    """
    Connects to the Unity Catalog server started by the test harness.

    The server runs in its own JVM so that only the Unity Catalog Spark connector, and not the
    server and its dependency tree, ends up on the Spark classpath. `DELTA_UC_URI` and
    `DELTA_UC_STORAGE_ROOT` are exported by jenkins/spark-tests.sh; see
    integration_tests/README.md for running this suite by hand.
    """
    jvm = spark_jvm()
    uri = unity_catalog_uri()
    storage_root = unity_catalog_storage_root()
    assert storage_root, "DELTA_UC_STORAGE_ROOT must be set alongside DELTA_UC_URI"

    client = _api_client(jvm, uri)
    catalogs_api = jvm.io.unitycatalog.client.api.CatalogsApi(client)
    catalogs_api.createCatalog(
        jvm.io.unitycatalog.client.model.CreateCatalog()
        .name(_CATALOG)
        .comment("RAPIDS catalog-managed table integration tests"))
    jvm.io.unitycatalog.client.api.SchemasApi(client).createSchema(
        jvm.io.unitycatalog.client.model.CreateSchema()
        .name(_SCHEMA)
        .catalogName(_CATALOG))

    yield {
        "uri": uri,
        "storage_root": storage_root,
        "tables_api": jvm.io.unitycatalog.client.api.TablesApi(client),
    }


def _catalog_conf(unity_catalog_server):
    prefix = f"spark.sql.catalog.{_CATALOG}"
    return {
        **delta_writes_enabled_conf,
        prefix: "io.unitycatalog.spark.UCSingleCatalog",
        f"{prefix}.uri": unity_catalog_server["uri"],
        f"{prefix}.token": _STATIC_TOKEN,
        f"{prefix}.warehouse": _CATALOG,
        f"{prefix}.renewCredential.enabled": "false",
        f"{prefix}.credScopedFs.enabled": "false",
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.format.parquet.reader.type": "PERFILE",
    }


def _new_table_name(prefix):
    table = f"{prefix}_{uuid.uuid4().hex}"
    return table, f"{_CATALOG}.{_SCHEMA}.{table}"


def _drop_table(table, conf):
    with_cpu_session(lambda spark: spark.sql(f"DROP TABLE IF EXISTS {table}").collect(), conf=conf)


def _table_rows(table, conf):
    return with_cpu_session(
        lambda spark: [tuple(row) for row in spark.table(table).orderBy("id").collect()],
        conf=conf)


def _error_class(action):
    try:
        action()
    except Exception as error:
        if hasattr(error, "getErrorClass"):
            error_class = error.getErrorClass()
            if error_class is not None:
                return error_class
        java_error = getattr(error, "java_exception", None)
        for _ in range(20):
            if java_error is None:
                break
            for method_name in ("getCondition", "getErrorClass"):
                try:
                    error_class = getattr(java_error, method_name)()
                    if error_class is not None:
                        return error_class
                except Exception:
                    pass
            try:
                java_error = java_error.getCause()
            except Exception:
                break
        return type(error).__name__
    raise AssertionError("Expected operation to fail")


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_ctas_insert_and_deletion_vector_scan(unity_catalog_server):
    table_name, table = _new_table_name("catalog_managed_smoke")
    conf = _catalog_conf(unity_catalog_server)

    try:
        def create_table(spark):
            return spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT /*+ COALESCE(1) */ * FROM VALUES
                    (1L, 'one'), (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect()

        assert_rapids_delta_write(create_table, conf=conf)
        assert_rapids_delta_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (4L, 'four')").collect(),
            conf=conf)

        session_catalog_matches = with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN spark_catalog.default LIKE '{table_name}'").collect(),
            conf=conf)
        assert session_catalog_matches == []

        detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        table_info = unity_catalog_server["tables_api"].getTable(table, None, None)
        catalog_properties = dict(table_info.getProperties())
        assert detail["location"].startswith("s3://test-bucket0/")
        assert catalog_properties[_CATALOG_MANAGED_PROPERTY] == "supported"
        assert catalog_properties[_UC_TABLE_ID_PROPERTY] == table_info.getTableId()
        assert detail["properties"][_UC_TABLE_ID_PROPERTY] == table_info.getTableId()
        assert detail["id"] != table_info.getTableId()
        assert detail["properties"]["delta.enableDeletionVectors"] == "true"
        assert detail["properties"]["delta.enableRowTracking"] == "true"
        assert detail["properties"]["delta.enableInCommitTimestamps"] == "true"
        assert detail["properties"]["delta.checkpointPolicy"] == "v2"

        with_cpu_session(
            lambda spark: spark.sql(f"DELETE FROM {table} WHERE id = 2").collect(),
            conf=conf)
        delete_metrics = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            .first()["operationMetrics"],
            conf=conf)
        assert int(delete_metrics["numDeletionVectorsAdded"]) > 0

        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)

        with pytest.raises(Exception):
            with_cpu_session(
                lambda spark: spark.read.format("delta").load(detail["location"]).collect(),
                conf=conf)
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_atomic_replace_time_travel_and_cdf(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_replace")
    conf = _catalog_conf(unity_catalog_server)

    try:
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'delta.enableChangeDataFeed' = 'true')
                AS SELECT * FROM VALUES
                    (1L, 'original-one'), (2L, 'original-two') AS source(id, value)
                """).collect(),
            conf=conf)

        original_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        original_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()

        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE OR REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'delta.enableChangeDataFeed' = 'true')
                AS SELECT * FROM VALUES
                    (3L, 'replacement-three'), (4L, 'replacement-four')
                    AS source(id, value)
                """).collect(),
            conf=conf)

        replaced_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        replaced_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()
        assert replaced_detail["id"] == original_detail["id"]
        assert replaced_detail["location"] == original_detail["location"]
        assert replaced_table_id == original_table_id
        assert _table_rows(table, conf) == [
            (3, "replacement-three"), (4, "replacement-four")]

        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(
                f"SELECT id, value FROM {table} VERSION AS OF 0 ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"""
                SELECT id, value, _change_type, _commit_version
                FROM table_changes('{table}', 0)
                ORDER BY _commit_version, _change_type, id
                """),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
    finally:
        _drop_table(table, conf)


@allow_non_gpu("CreateTableExec", "AtomicReplaceTableExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_create_replace_and_rtas(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_create_replace")
    conf = _catalog_conf(unity_catalog_server)

    try:
        with_gpu_session(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table} (id BIGINT, value STRING)
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                """).collect(),
            conf=conf)
        original_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        original_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()

        assert_rapids_delta_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (1L, 'one')").collect(),
            conf=conf)
        with_gpu_session(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table} (id BIGINT, value STRING)
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == []

        replaced_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        replaced_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()
        assert replaced_detail["id"] == original_detail["id"]
        assert replaced_detail["location"] == original_detail["location"]
        assert replaced_table_id == original_table_id

        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(2, "two"), (3, "three")]
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_liquid_clustering(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_clustered")
    conf = _catalog_conf(unity_catalog_server)

    try:
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                CLUSTER BY (id)
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (1L, 'one'), (2L, 'two') AS source(id, value)
                """).collect(),
            conf=conf)

        detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        catalog_properties = dict(
            unity_catalog_server["tables_api"].getTable(table, None, None).getProperties())
        assert detail["clusteringColumns"] == ["id"]
        assert catalog_properties["clusteringColumns"] == '[["id"]]'
        assert catalog_properties["delta.feature.clustering"] == "supported"

        assert_rapids_delta_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (3L, 'three')").collect(),
            conf=conf)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)

        updated_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        assert updated_detail["clusteringColumns"] == ["id"]
    finally:
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_create_replace_rejections_and_abort(unity_catalog_server):
    _, cpu_table = _new_table_name("catalog_managed_cpu_reject")
    _, gpu_table = _new_table_name("catalog_managed_gpu_reject")
    missing_name, missing_table = _new_table_name("catalog_managed_or_create")
    failed_name, failed_table = _new_table_name("catalog_managed_failed_create")
    conf = _catalog_conf(unity_catalog_server)

    def create(spark, table):
        return spark.sql(f"""
            CREATE TABLE {table}
            USING DELTA
            TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
            AS SELECT 1L AS id, 'original' AS value
            """).collect()

    try:
        with_cpu_session(lambda spark: create(spark, cpu_table), conf=conf)
        assert_rapids_delta_write(lambda spark: create(spark, gpu_table), conf=conf)

        def metadata_changing_replace(spark, table):
            return spark.sql(f"""
                CREATE OR REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT 2L AS id, 'replacement' AS value, 'new' AS extra
                """).collect()

        cpu_error = _error_class(
            lambda: with_cpu_session(
                lambda spark: metadata_changing_replace(spark, cpu_table), conf=conf))
        gpu_error = _error_class(
            lambda: with_gpu_session(
                lambda spark: metadata_changing_replace(spark, gpu_table), conf=conf))
        assert cpu_error == gpu_error == "DELTA_OPERATION_NOT_ALLOWED"
        assert _table_rows(cpu_table, conf) == [(1, "original")]
        assert _table_rows(gpu_table, conf) == [(1, "original")]

        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE OR REPLACE TABLE {missing_table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT 3L AS id, 'created' AS value
                """).collect(),
            conf=conf)
        assert _table_rows(missing_table, conf) == [(3, "created")]
        assert with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN spark_catalog.default LIKE '{missing_name}'").collect(),
            conf=conf) == []

        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            with_gpu_session(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {failed_table}
                    USING DELTA
                    TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                    AS SELECT id, 1L / (id - id) AS invalid
                    FROM range(1, 3)
                    """).collect(),
                conf=conf)
        assert with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN {_CATALOG}.{_SCHEMA} LIKE '{failed_name}'").collect(),
            conf=conf) == []

        for statement in (f"OPTIMIZE {gpu_table}", f"REORG TABLE {gpu_table} APPLY (PURGE)"):
            cpu_error = _error_class(
                lambda statement=statement: with_cpu_session(
                    lambda spark: spark.sql(statement).collect(), conf=conf))
            gpu_error = _error_class(
                lambda statement=statement: with_gpu_session(
                    lambda spark: spark.sql(statement).collect(), conf=conf))
            assert cpu_error == gpu_error == \
                "DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION"
    finally:
        for table in (cpu_table, gpu_table, missing_table, failed_table):
            _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_v1_v2_and_overwrite_writes(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_writes")
    conf = _catalog_conf(unity_catalog_server)
    optimized_conf = {
        **conf,
        "spark.databricks.delta.optimizeWrite.enabled": "true",
    }

    try:
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                PARTITIONED BY (p)
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (1L, 'one', 0), (2L, 'two', 1) AS source(id, value, p)
                """).collect(),
            conf=conf)

        assert_rapids_delta_write(
            lambda spark: spark.createDataFrame(
                [(3, "three", 0)], "id LONG, value STRING, p INT")
                .write.format("delta").mode("append").saveAsTable(table),
            conf=optimized_conf)
        assert_rapids_delta_write(
            lambda spark: spark.createDataFrame(
                [(4, "four", 1)], "id LONG, value STRING, p INT")
                .writeTo(table).append(),
            conf=conf)
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                INSERT INTO {table} REPLACE WHERE p = 0
                VALUES (5L, 'five', 0)
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [
            (2, "two", 1), (4, "four", 1), (5, "five", 0)]

        dynamic_conf = {
            **conf,
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.databricks.delta.delete.deletionVectors.persistent": "false",
        }
        assert_rapids_delta_write(
            lambda spark: spark.createDataFrame(
                [(6, "six", 1)], "id LONG, value STRING, p INT")
                .writeTo(table).overwritePartitions(),
            conf=dynamic_conf)
        assert _table_rows(table, conf) == [(5, "five", 0), (6, "six", 1)]

        assert_rapids_delta_write(
            lambda spark: spark.sql(
                f"INSERT OVERWRITE {table} VALUES (7L, 'seven', 2)").collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(7, "seven", 2)]
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_schema_merge_and_overwrite(unity_catalog_server):
    _, cpu_table = _new_table_name("catalog_managed_cpu_schema")
    _, gpu_table = _new_table_name("catalog_managed_gpu_schema")
    conf = _catalog_conf(unity_catalog_server)

    def create(spark, table):
        return spark.sql(f"""
            CREATE TABLE {table}
            USING DELTA
            TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
            AS SELECT 1L AS id, 'original' AS value
            """).collect()

    def append_with_schema_merge(spark, table, with_extra_column):
        schema = "id LONG, value STRING, extra STRING" if with_extra_column else \
            "id LONG, value STRING"
        row = (2, "merged", "extra") if with_extra_column else (2, "merged")
        return spark.createDataFrame([row], schema) \
            .write.format("delta").mode("append") \
            .option("mergeSchema", "true").saveAsTable(table)

    def overwrite_schema(spark, table):
        return spark.createDataFrame([(3, "replacement")], "id LONG, replacement STRING") \
            .write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(table)

    try:
        with_cpu_session(lambda spark: create(spark, cpu_table), conf=conf)
        assert_rapids_delta_write(lambda spark: create(spark, gpu_table), conf=conf)

        assert_rapids_delta_write(
            lambda spark: append_with_schema_merge(spark, gpu_table, False),
            conf=conf)
        assert _table_rows(gpu_table, conf) == [(1, "original"), (2, "merged")]

        cpu_merge_error = _error_class(
            lambda: with_cpu_session(
                lambda spark: append_with_schema_merge(spark, cpu_table, True), conf=conf))
        gpu_merge_error = _error_class(
            lambda: with_gpu_session(
                lambda spark: append_with_schema_merge(spark, gpu_table, True), conf=conf))
        assert cpu_merge_error == gpu_merge_error == "DELTA_OPERATION_NOT_ALLOWED"

        cpu_overwrite_error = _error_class(
            lambda: with_cpu_session(
                lambda spark: overwrite_schema(spark, cpu_table), conf=conf))
        gpu_overwrite_error = _error_class(
            lambda: with_gpu_session(
                lambda spark: overwrite_schema(spark, gpu_table), conf=conf))
        assert cpu_overwrite_error == gpu_overwrite_error == "DELTA_OPERATION_NOT_ALLOWED"
        assert _table_rows(cpu_table, conf) == [(1, "original")]
        assert _table_rows(gpu_table, conf) == [(1, "original"), (2, "merged")]
    finally:
        _drop_table(cpu_table, conf)
        _drop_table(gpu_table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_external_table_regression(unity_catalog_server):
    _, table = _new_table_name("unity_external")
    location = f"s3://test-bucket0{unity_catalog_server['storage_root']}/{uuid.uuid4().hex}"
    conf = _catalog_conf(unity_catalog_server)

    try:
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                LOCATION '{location}'
                AS SELECT * FROM VALUES
                    (1L, 'one'), (2L, 'two') AS source(id, value)
                """).collect(),
            conf=conf)
        table_info = unity_catalog_server["tables_api"].getTable(table, None, None)
        assert table_info.getTableType().toString() == "EXTERNAL"
        assert table_info.getStorageLocation() == location
        assert _CATALOG_MANAGED_PROPERTY not in dict(table_info.getProperties())

        assert_rapids_delta_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (3L, 'three')").collect(),
            conf=conf)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_delete_update_and_merge(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_dml")
    conf = {
        **_catalog_conf(unity_catalog_server),
        "spark.databricks.delta.delete.deletionVectors.persistent": "false",
        "spark.databricks.delta.update.deletionVectors.persistent": "false",
        "spark.databricks.delta.merge.deletionVectors.persistent": "false",
    }

    try:
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT /*+ COALESCE(1) */ * FROM VALUES
                    (1L, 'one'), (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect(),
            conf=conf)

        assert_rapids_gpu_delete_ran(
            lambda spark: spark.sql(f"DELETE FROM {table} WHERE id = 1").collect(),
            conf=conf)
        assert_rapids_delta_write(
            lambda spark: spark.sql(
                f"UPDATE {table} SET value = 'updated-two' WHERE id = 2").collect(),
            conf=conf)

        def merge(spark):
            source = f"catalog_managed_merge_source_{uuid.uuid4().hex}"
            spark.createDataFrame(
                [(2, "merged-two"), (4, "four")], "id LONG, value STRING") \
                .createOrReplaceTempView(source)
            return spark.sql(f"""
                MERGE INTO {table} AS target
                USING {source} AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET value = source.value
                WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
                """).collect()

        assert_rapids_delta_write(merge, conf=conf)
        assert _table_rows(table, conf) == [
            (2, "merged-two"), (3, "three"), (4, "four")]
    finally:
        _drop_table(table, conf)
