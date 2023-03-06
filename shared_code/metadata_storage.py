from __future__ import annotations

import functools
import os
import time
from types import SimpleNamespace
from typing import Any

from shared_code.exceptions import NotFoundInMetadataStorageError
from shared_code.logger import get_traceback
from shared_code.postgres import Postgres


class MetadataStorage:
    def __init__(self, metadata_storage_config: SimpleNamespace):
        self.logger = metadata_storage_config.logger
        self.tenant = metadata_storage_config.tenant
        self.scenario = metadata_storage_config.scenario
        self._db = self._get_db(
            db_config=metadata_storage_config.db_config,
            db_config_masked=metadata_storage_config.db_config_masked
        )
        self.step_uuid = time.time()

    def _get_db(self, db_config, db_config_masked) -> Postgres:
        self.logger.info(f"Connecting to metadata_storage config={db_config_masked}")
        return Postgres(self.logger, db_config)

    def _get_datasource_credentials(self) -> str | None:
        return os.getenv('datasource_password')

    def _get_metadata(self, sql: str, entity: str) -> Any:
        self.logger.info(f"Getting {entity} config from metadata_storage {sql=}")
        data = self._db.execute_query_fetch_results(query=sql, include_header=True)
        if len(data) < 2:
            raise NotFoundInMetadataStorageError(f"{entity} ({sql=})")
        config = SimpleNamespace(**dict(zip(data[0],data[1])))
        return config

    def get_datasource_metadata(
        self, tenant: str, dataproduct: str, dataproduct_version: str,
        datasource_id: str
    ) -> Any:
        sql = """
              SELECT host, database AS db_name, port, schema, username
                FROM tenant_data_source
               WHERE tenant_id = '{}'
                 AND data_product_id = '{}'
                 AND data_product_version = '{}'""".format(tenant, dataproduct, dataproduct_version)
        metadata = self._get_metadata(sql, entity='datasource')
        metadata.password = self._get_datasource_credentials()
        metadata.id = datasource_id
        metadata.name = datasource_id
        return metadata

    def get_dataproduct_metadata(
        self, dataproduct: str, dataproduct_version: str
    ) -> Any:

        sql = """
              SELECT name, storage_path
                FROM data_product_catalog
               WHERE id = '{}'
                 AND version = '{}'""".format(dataproduct, dataproduct_version)
        metadata = self._get_metadata(sql, entity='dataproduct')
        metadata.storage_path = metadata.storage_path.strip("/")
        return metadata

    def get_tenant_metadata(self, tenant: str) -> Any:
        sql = """
              SELECT name
                FROM tenant
               WHERE id = '{}'""".format(tenant)
        return self._get_metadata(sql, entity='tenant')

    def execution_log_insert(self, scenario_task:str, result: str):
        sql = """
              INSERT INTO execution_log (scenario_type, scenario_task, process_step_id, execution_timestamp, tenant_id, result) 
                   VALUES (%s, %s, %s, now(), %s, %s)"""
        params = (self.scenario, scenario_task, self.step_uuid, self.tenant, result)
        self._db.execute_param_query(sql, params)

def execution_log(func):
    @functools.wraps(func)
    def wrapper_metadata_storage_log(self, *args, **kwargs):
        try:
            value = func(self, *args, **kwargs)
        except Exception as ex:
            traceback = get_traceback(ex)
            self.metadata_storage.execution_log_insert(
                scenario_task=func.__name__,
                result=traceback
            )
            raise ex
        else:
            self.metadata_storage.execution_log_insert(scenario_task=func.__name__, result='ok')

        return value
    return wrapper_metadata_storage_log
