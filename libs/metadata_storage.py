from __future__ import annotations

from typing import Any, Dict
from logging import Logger
from libs.postgres import Postgres
from libs.exceptions import NotFoundInMetadataStorageError
from types import SimpleNamespace
from pathlib import Path
import os

class MetadataStorage:
    def __init__(self, logger: Logger, args: Any, config: Dict, scenario_type: str ):
        self.logger = logger
        self.args = args
        self.config = SimpleNamespace(**config)
        self.scenario_type = scenario_type
        self._db = self._get_db()

    def _get_db(self) -> Postgres:
        masked_config = {k:v for k,v in self.config.__dict__.items() if k != 'password'}
        self.logger.info(f"Connecting to metadata_storage config={masked_config}")
        return Postgres(self.logger, self.config)

    def _get_datasource_credentials(self) -> str:
        return os.getenv('datasource_password')
    
    def _get_metadata(self, sql: str, entity: str) -> Any:
        self.logger.info(f"Getting {entity} config from metadata_storage {sql=}")
        data = self._db.execute_query_fetch_results(query=sql, include_header=True)
        if len(data) < 2:
            raise NotFoundInMetadataStorageError(f"{entity} ({sql=})")
        config = SimpleNamespace(**dict(zip(data[0],data[1])))
        return config

    def get_datasource_metadata(self, datasource_id: str) -> Any:
        sql = """
              SELECT host, database AS db_name, port, schema, username
                FROM tenant_data_source
               WHERE tenant_id = '{}'
                 AND data_product_id = '{}'
                 AND data_product_version = '{}'""".format(self.args.tenant, self.args.dataproduct, self.args.dataproduct_version)
        metadata = self._get_metadata(sql, entity='datasource')
        metadata.password = self._get_datasource_credentials()
        metadata.id = datasource_id
        metadata.name = datasource_id
        return metadata

    def get_dataproduct_metadata(self) -> Any:
        sql = """
              SELECT name, storage_path
                FROM data_product_catalog
               WHERE id = '{}'
                 AND version = '{}'""".format(self.args.dataproduct, self.args.dataproduct_version)
        metadata = self._get_metadata(sql, entity='dataproduct')
        metadata.storage_path = metadata.storage_path.strip("/")
        return metadata
    
    def get_tenant_metadata(self) -> Any:
        sql = """
              SELECT name
                FROM tenant
               WHERE id = '{}'""".format(self.args.tenant)
        return self._get_metadata(sql, entity='tenant')
    
    def execution_log(self, result: str):
        sql = """        
              INSERT INTO execution_log (scenario_type, process_step_id, execution_timestamp, tenant_id, result) 
                   VALUES (%s, 'step_uuid', now(), %s, %s)"""
        params = (self.scenario_type, self.args.tenant, result)
        self._db.execute_param_query(sql, params)