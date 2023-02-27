from libs.logger import get_logger, get_traceback
from config import (
        Config, DATASOURCE_ID_TMPL, PARENT_WORKSPACE_ID_TMPL, CHILD_WORKSPACE_ID_TMPL,
        DECLARATIVE_DATAPRODUCT_PATH, CREATE_TENANT_SCENARIO, USERGROUP_ID_TMPL
)
from args import provision_tenant_args
from libs.gooddata import (
        get_sdk, create_or_update_data_source, create_or_update_workspace,
        put_declarative_pdm, put_declarative_ldm, put_declarative_am, create_or_update_user_group,
        assign_workspace_permissions, create_or_update_user
)
from libs.metadata_storage import MetadataStorage, metadata_storage_logger
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

    @metadata_storage_logger
    def create_datasource(self) -> str:
        create_or_update_data_source(self.sdk, self.logger, config=self.metadata.datasource)
        return self.metadata.datasource.id

    @metadata_storage_logger
    def create_empty_parent(self) -> str:
        id = PARENT_WORKSPACE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        create_or_update_workspace(self.sdk, self.logger, id=id, name=id)
        return id

    @metadata_storage_logger
    def create_empty_child(self, parent_id: str) -> str:
        id = CHILD_WORKSPACE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        create_or_update_workspace(self.sdk, self.logger, id=id, name=id, parent_id=parent_id)
        return id
    
    @metadata_storage_logger
    def get_metadata(self) -> None:
        self.metadata = SimpleNamespace()        
        datasource_id = DATASOURCE_ID_TMPL.format(data_product_id=self.args.dataproduct, tenant_id=self.args.tenant)
        self.metadata.datasource = self.metadata_storage.get_datasource_metadata(datasource_id)
        self.metadata.dataproduct = self.metadata_storage.get_dataproduct_metadata()
        self.metadata.tenant = self.metadata_storage.get_tenant_metadata()

    @metadata_storage_logger
    def get_dataproduct(self) -> None:        
        self.dataproduct_repository.get_declarative_dataproduct(self.metadata.dataproduct.storage_path)

    @metadata_storage_logger
    def deploy_dataproduct(self, datasource_id:str, workspace_id: str) -> None:
        src_dir = Path(DECLARATIVE_DATAPRODUCT_PATH)
        put_declarative_pdm(self.sdk, self.logger, src_dir, datasource_id)
        put_declarative_ldm(self.sdk, self.logger, src_dir, workspace_id, datasource_id)
        put_declarative_am(self.sdk, self.logger, src_dir, workspace_id)

    @metadata_storage_logger
    def create_user_groups(self) -> None:
        usergroups = ['chevron5','chevron6']
        for usergroup in usergroups:
            user_group_id = USERGROUP_ID_TMPL.format(tenant_id=self.args.tenant, usergroup=usergroup)
            create_or_update_user_group(self.sdk, self.logger, user_group_id=user_group_id)

    def _get_workspace_permissions_asignee(self, usergroup: str, name: str):
        assignee = {
            "assignee": {
                "id": USERGROUP_ID_TMPL.format(tenant_id=self.args.tenant, usergroup=usergroup),
                "type": "userGroup"
            },
            "name": name
        } 
        return assignee

    @metadata_storage_logger
    def assign_workspace_permissions(self, workspace_id: str) -> None:
        chevron6 =  self._get_workspace_permissions_asignee(usergroup='chevron6', name='VIEW')
        chevron5 =  self._get_workspace_permissions_asignee(usergroup='chevron5', name='MANAGE')
        data = {"hierarchyPermissions": [], "permissions": [chevron6, chevron5]}
        assign_workspace_permissions(self.sdk, self.logger, data, workspace_id)

    @metadata_storage_logger
    def provision_default_users(self) -> None:
        chevron5_user_group = USERGROUP_ID_TMPL.format(tenant_id=self.args.tenant, usergroup='chevron5')
        user_group_ids = [chevron5_user_group]

        tenant_owner = SimpleNamespace()
        tenant_owner.user_id = f"tenant.owner.{self.args.tenant}.com"
        tenant_owner.user_group_ids = user_group_ids
        create_or_update_user(self.sdk, self.logger, config=tenant_owner)

        global_admin = SimpleNamespace()
        global_admin.user_id = 'global.admin.honeywell.com'
        global_admin.user_group_ids = user_group_ids
        create_or_update_user(self.sdk, self.logger, config=global_admin)

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
            self.assign_workspace_permissions(child_id)
            self.provision_default_users()
        except Exception as ex:
            traceback = get_traceback(ex)
            self.logger.error(traceback)
            print(f"The execution failed (exeption={ex.__class__.__name__}, rollback_required={self.if_error_rollback_required})")
        else:
            print("The execution finished successfully")

if __name__ == "__main__":
    ProvisionTenant().main()