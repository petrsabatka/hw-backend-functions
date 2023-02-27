from pathlib import Path
from typing import Dict
import os
import yaml

WRK_FOLDER = "tmp"
DOWNLOAD_FOLDER = WRK_FOLDER + "/downloads"
DECLARATIVE_DATAPRODUCT_PATH = DOWNLOAD_FOLDER + "/dataproduct"

DATASOURCE_ID_TMPL = "{data_product_id}_{tenant_id}"
PARENT_WORKSPACE_ID_TMPL = "{data_product_id}_{tenant_id}_parent"
CHILD_WORKSPACE_ID_TMPL = "{data_product_id}_{tenant_id}_child"
USERGROUP_ID_TMPL = "{data_product_id}_{tenant_id}_{usergroup}"

def get_child_workspace_id (data_product_id: str, tenant_id: str) -> str:
    return CHILD_WORKSPACE_ID_TMPL.format(data_product_id=data_product_id, tenant_id=tenant_id)

def get_parent_workspace_id (data_product_id: str, tenant_id: str) -> str:
    return PARENT_WORKSPACE_ID_TMPL.format(data_product_id=data_product_id, tenant_id=tenant_id)

def get_datasource_id (data_product_id: str, tenant_id: str) -> str:
    return DATASOURCE_ID_TMPL.format(data_product_id=data_product_id, tenant_id=tenant_id)

def get_usergroup_id (data_product_id: str, tenant_id: str, usergroup: str) -> str:
    return USERGROUP_ID_TMPL.format(data_product_id=data_product_id, tenant_id=tenant_id,usergroup=usergroup)

class Config:
    def __init__(self, config_file: str = 'shared_code/app_config.yaml'):
        self.config_file = Path(config_file)

    @property
    def config(self) -> Dict:
        with open(Path(self.config_file)) as fp:
            return yaml.safe_load(fp)

    @property
    def metadata_storage(self) -> Dict:
        metadata_storage = self.config['metadata_storage']
        metadata_storage['password'] = os.getenv('metadata_storage_password')
        return metadata_storage

    @property
    def gooddata(self) -> Dict:
        gooddata = self.config['gooddata']
        gooddata['token'] = os.getenv('gooddata_token')
        return gooddata
    
    @property
    def dataproduct_repository(self) -> Dict:
        dataproduct_repository = self.config['dataproduct_repository']
        dataproduct_repository['connection_string'] = os.getenv('azure_storage_connection_string')
        return dataproduct_repository