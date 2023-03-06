import os
from types import SimpleNamespace
from typing import Dict

DATASOURCE_ID_TMPL = "{data_product_id}_{tenant_id}"
PARENT_WORKSPACE_ID_TMPL = "{data_product_id}_{tenant_id}_parent"
CHILD_WORKSPACE_ID_TMPL = "{data_product_id}_{tenant_id}_child"
USERGROUP_ID_TMPL = "{data_product_id}_{tenant_id}_{usergroup}"

REQUIRED_ENVIRON_METADATA_STORAGE = {
    'metadata_storage_host': 'host',
    'metadata_storage_port': 'port',
    'metadata_storage_user': 'user',
    'metadata_storage_db_name': 'db_name',
    'metadata_storage_schema': 'schema',
    'metadata_storage_password': 'password'
}
REQUIRED_ENVIRON_GOODDATA = {
    'gooddata_host': 'host',
    'gooddata_token': 'token'
}
REQUIRED_ENVIRON_DATAPRODUCT_REPOSITORY = {
    'dataproduct_repository_container_name': 'container_name',
    'dataproduct_repository_connection_string': 'connection_string'
}

def get_child_workspace_id(data_product_id: str, tenant_id: str) -> str:
    return CHILD_WORKSPACE_ID_TMPL.format(
        data_product_id=data_product_id,
        tenant_id=tenant_id
    )

def get_parent_workspace_id(data_product_id: str, tenant_id: str) -> str:
    return PARENT_WORKSPACE_ID_TMPL.format(
        data_product_id=data_product_id,
        tenant_id=tenant_id
    )

def get_datasource_id(data_product_id: str, tenant_id: str) -> str:
    return DATASOURCE_ID_TMPL.format(
        data_product_id=data_product_id,
        tenant_id=tenant_id
    )

def get_usergroup_id(data_product_id: str, tenant_id: str, usergroup: str) -> str:
    return USERGROUP_ID_TMPL.format(
        data_product_id=data_product_id,
        tenant_id=tenant_id,usergroup=usergroup
    )

def get_environ_in_local_names(translatin_map: Dict) -> SimpleNamespace:
    local_environ = {}
    for external_name, internal_name in translatin_map.items():
        local_environ[internal_name] = os.getenv(external_name)
    return SimpleNamespace(**local_environ)

def get_metadata_storage_config(tenant: str, scenario: str, logger) -> SimpleNamespace:
    cnf = SimpleNamespace()
    cnf.db_config = get_environ_in_local_names(REQUIRED_ENVIRON_METADATA_STORAGE)
    public_params = ['host','port','user','db_name','schema']
    cnf.db_config_masked =  {k:v for k,v in cnf.db_config.__dict__.items() if k in public_params}
    cnf.tenant = tenant
    cnf.scenario = scenario
    cnf.logger = logger
    return cnf

def get_gooddata_config(logger) -> SimpleNamespace:
    cnf = SimpleNamespace()
    cnf.config = get_environ_in_local_names(REQUIRED_ENVIRON_GOODDATA)
    public_params = ['host']
    cnf.config_masked =  {k:v for k,v in cnf.config.__dict__.items() if k in public_params}
    cnf.logger = logger
    return cnf

def get_dataproduct_repository_config(logger) -> SimpleNamespace:
    cnf = SimpleNamespace()
    cnf.config = get_environ_in_local_names(REQUIRED_ENVIRON_DATAPRODUCT_REPOSITORY)
    public_params = ['container_name']
    cnf.config_masked =  {k:v for k,v in cnf.config.__dict__.items() if k in public_params}
    cnf.logger = logger
    return cnf
