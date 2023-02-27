from shared_code import logger
from shared_code import app_config
from shared_code import gooddata
from shared_code import metadata_storage
from shared_code import dataproduct_repository
from args import provision_tenant_args
from pathlib import Path
from dotenv import load_dotenv
from types import SimpleNamespace
import argparse
import fce_config
from mocks.mock import Mock

load_dotenv()

class ProvisionTenant:
    def __init__(self) -> None:
        self.if_error_rollback_required = False
        self.args = self.parse_arguments()
        self.logger = logger.get_logger(ProvisionTenant.__name__, self.args.debug)
        app_conf = app_config.Config()
        self.fce_conf = fce_config.Config(self.args)
        self.metadata_storage = metadata_storage.MetadataStorage(self.logger, self.args, 
                                    config=app_conf.metadata_storage, scenario_type=fce_config.SCENARIO) 
        self.sdk = gooddata.get_sdk(app_conf.gooddata['host'], app_conf.gooddata['token'], self.logger)
        self.dataproduct_repository = dataproduct_repository.DataproductRepository(self.logger, 
                                          self.args, config=app_conf.dataproduct_repository)
        self.args.default_users = Mock(self.args).default_users

    @staticmethod
    def parse_arguments():
        parser = argparse.ArgumentParser()
        provision_tenant_args(parser)
        args = parser.parse_args()
        return args

    @metadata_storage.execution_log
    def create_datasource(self) -> str:
        gooddata.create_or_update_data_source(self.sdk, self.logger, config=self.metadata.datasource)
        return self.metadata.datasource.id

    @metadata_storage.execution_log
    def create_empty_parent(self) -> str:
        id = app_config.PARENT_WORKSPACE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        gooddata.create_or_update_workspace(self.sdk, self.logger, id=id, name=id)
        return id

    @metadata_storage.execution_log
    def create_empty_child(self, parent_id: str) -> str:
        id = app_config.CHILD_WORKSPACE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        gooddata.create_or_update_workspace(self.sdk, self.logger, id=id, name=id, parent_id=parent_id)
        return id
    
    @metadata_storage.execution_log
    def get_metadata(self) -> None:
        self.metadata = SimpleNamespace()        
        datasource_id = app_config.DATASOURCE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        self.metadata.datasource = self.metadata_storage.get_datasource_metadata(datasource_id)
        self.metadata.dataproduct = self.metadata_storage.get_dataproduct_metadata()
        self.metadata.tenant = self.metadata_storage.get_tenant_metadata()

    @metadata_storage.execution_log
    def get_dataproduct(self) -> None:        
        self.dataproduct_repository.get_declarative_dataproduct(self.metadata.dataproduct.storage_path)

    @metadata_storage.execution_log
    def deploy_dataproduct(self, datasource_id:str, workspace_id: str) -> None:
        src_dir = Path(app_config.DECLARATIVE_DATAPRODUCT_PATH)
        gooddata.put_declarative_pdm(self.sdk, self.logger, src_dir, datasource_id)
        gooddata.put_declarative_ldm(self.sdk, self.logger, src_dir, workspace_id, datasource_id)
        gooddata.put_declarative_am(self.sdk, self.logger, src_dir, workspace_id)

    @metadata_storage.execution_log
    def create_user_groups(self) -> None:
        usergroups = self.fce_conf.default_usergroups
        for usergroup in usergroups:
            gooddata.create_or_update_user_group(self.sdk, self.logger, user_group_id=usergroup.id)

    @metadata_storage.execution_log
    def assign_workspace_permissions(self) -> None:
        for permission in self.fce_conf.workspace_permissions:
            gooddata.assign_workspace_usergoup_permissions(self.sdk, self.logger, 
                         workspace_id=permission.workspace_id, usergroups=permission.usergroups)

    @metadata_storage.execution_log
    def provision_default_users(self) -> None:
        user_group = self.fce_conf.get_default_usergroup_for_default_users()        
        for user in self.args.default_users:
            user.user_group_ids = [user_group]
            gooddata.create_or_update_user(self.sdk, self.logger, config=user)

    def main(self):
        try:
            self.get_metadata()
            self.get_dataproduct()
            self.if_error_rollback_required = True
            datasource_id = self.create_datasource()
            parent_id = self.create_empty_parent()
            child_id = self.create_empty_child(parent_id)
            self.deploy_dataproduct(datasource_id, workspace_id=parent_id)
            self.create_user_groups()
            self.assign_workspace_permissions()
            self.provision_default_users()
        except Exception as ex:
            traceback = logger.get_traceback(ex)
            self.logger.error(traceback)
            print(f"The execution failed (exeption={ex.__class__.__name__}, rollback_required={self.if_error_rollback_required})")
        else:
            print("The execution finished successfully")

if __name__ == "__main__":
    ProvisionTenant().main()