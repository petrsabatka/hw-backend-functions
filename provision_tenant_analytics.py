from libs.logger import get_logger, get_traceback
from config import (
        Config, DATASOURCE_ID_TMPL, PARENT_WORKSPACE_ID_TMPL, CHILD_WORKSPACE_ID_TMPL,
        DECLARATIVE_DATAPRODUCT_PATH, CREATE_TENANT_SCENARIO
)
from args import provision_tenant_args
from libs.gooddata import (
        get_sdk, create_or_update_data_source, create_or_update_workspace,
        put_declarative_pdm, put_declarative_ldm, put_declarative_am
)
from libs.metadata_storage import MetadataStorage
from libs.dataproduct_repository import DataproductRepository
from pathlib import Path
from dotenv import load_dotenv
from types import SimpleNamespace
import argparse
import os

load_dotenv()

class ProvisionTenant:
    def __init__(self) -> None:
        self.if_error_rollback_required = False
        self.args = self.parse_arguments()
        self.logger = get_logger(ProvisionTenant.__name__, self.args.debug)
        config = Config(self.args.config)
        self.metadata_storage = MetadataStorage(self.logger, self.args, config=config.metadata_storage,
                                                scenario_type=CREATE_TENANT_SCENARIO) 
        self.sdk = get_sdk(config.gooddata['host'], config.gooddata['token'], self.logger)
        self.dataproduct_repository = DataproductRepository(self.logger, self.args, config=config.dataproduct_repository)

    @staticmethod
    def parse_arguments():
        parser = argparse.ArgumentParser()
        provision_tenant_args(parser)
        args = parser.parse_args()
        return args

    def create_datasource(self) -> str:        
        create_or_update_data_source(self.sdk, self.logger, config=self.metadata.datasource)
        return self.metadata.datasource.id

    def create_empty_parent(self) -> str:
        id = PARENT_WORKSPACE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        create_or_update_workspace(self.sdk, self.logger, id=id, name=id)
        return id

    def create_empty_child(self, parent_id: str) -> str:
        id = CHILD_WORKSPACE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        create_or_update_workspace(self.sdk, self.logger, id=id, name=id, parent_id=parent_id)
        return id
    
    def get_metadata(self) -> None:
        self.metadata = SimpleNamespace()        
        datasource_id = DATASOURCE_ID_TMPL.format(data_product_id=self.args.dataproduct)
        self.metadata.datasource = self.metadata_storage.get_datasource_metadata(datasource_id)
        self.metadata.dataproduct = self.metadata_storage.get_dataproduct_metadata()
        self.metadata.tenant = self.metadata_storage.get_tenant_metadata()

    def get_dataproduct(self) -> None:        
        self.dataproduct_repository.get_declarative_dataproduct(self.metadata.dataproduct.storage_path)

    def deploy_dataproduct(self, datasource_id:str, workspace_id: str) -> None:
        src_dir = Path(DECLARATIVE_DATAPRODUCT_PATH)
        put_declarative_pdm(self.sdk, self.logger, src_dir, datasource_id)
        put_declarative_ldm(self.sdk, self.logger, src_dir, workspace_id, datasource_id)
        put_declarative_am(self.sdk, self.logger, src_dir, workspace_id)

    def main(self):
        try:
            self.get_metadata()
            self.get_dataproduct()
            self.if_error_rollback_required = True
            datasource_id = self.create_datasource()
            parent_id = self.create_empty_parent()
            self.create_empty_child(parent_id)
            self.deploy_dataproduct(datasource_id, workspace_id=parent_id)
        except Exception as ex:
            traceback = get_traceback(ex)
            self.metadata_storage.execution_log(traceback)
            self.logger.error(traceback)
            print(f"The execution failed (exeption={ex.__class__.__name__}, rollback_required={self.if_error_rollback_required})")
        else:
            self.metadata_storage.execution_log('ok')
            print("The execution finished successfully")

if __name__ == "__main__":
    ProvisionTenant().main()