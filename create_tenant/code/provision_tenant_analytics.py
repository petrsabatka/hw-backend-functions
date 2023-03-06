from pathlib import Path
from types import SimpleNamespace

from shared_code import (dataproduct_repository, gooddata, logger,
                         metadata_storage)

from . import fce_config


class ProvisionTenant:
    def __init__(self, args, temp_dir) -> None:
        self.fcc = fce_config.FceConfig(args, temp_dir)
        self.gdata = gooddata.GoodData(self.fcc.gooddata_config)
        self.metadata_storage = metadata_storage.MetadataStorage(self.fcc.metadata_storage_config)
        self.dataproduct_repository = dataproduct_repository.DataproductRepository(
            self.fcc.dataproduct_repository_config
        )
        self.metadata = SimpleNamespace()

    @metadata_storage.execution_log
    def create_datasource(self) -> None:
        self.gdata.create_or_update_data_source(config=self.metadata.datasource)

    @metadata_storage.execution_log
    def create_empty_parent(self) -> None:
        workspace_id = self.fcc.parent_workspace_id
        self.gdata.create_or_update_workspace(workspace_id=workspace_id, name=workspace_id)

    @metadata_storage.execution_log
    def create_empty_child(self, parent_id: str) -> None:
        workspace_id = self.fcc.child_workspace_id
        self.gdata.create_or_update_workspace(
            workspace_id=workspace_id,
            name=workspace_id,
            parent_id=parent_id
        )

    @metadata_storage.execution_log
    def get_metadata(self) -> None:
        datasource_id = self.fcc.datasource_id
        self.metadata.datasource = self.metadata_storage.get_datasource_metadata(
             tenant=self.fcc.tenant,
             dataproduct=self.fcc.dataproduct,
             dataproduct_version=self.fcc.dataproduct_version,
             datasource_id=datasource_id
        )
        self.metadata.dataproduct = self.metadata_storage.get_dataproduct_metadata(
             dataproduct=self.fcc.dataproduct,
             dataproduct_version=self.fcc.dataproduct_version
        )
        self.metadata.tenant = self.metadata_storage.get_tenant_metadata(tenant=self.fcc.tenant)

    @metadata_storage.execution_log
    def get_dataproduct(self) -> None:
        self.dataproduct_repository.get_declarative_dataproduct(
            self.metadata.dataproduct.storage_path,
            dest_path=self.fcc.declarative_dataproduct_path
        )

    @metadata_storage.execution_log
    def deploy_dataproduct(self, datasource_id: str, workspace_id: str) -> None:
        src_dir = Path(self.fcc.declarative_dataproduct_path)
        self.gdata.put_declarative_pdm(src_dir, datasource_id)
        self.gdata.put_declarative_ldm(src_dir, workspace_id, datasource_id)
        self.gdata.put_declarative_am(src_dir, workspace_id)

    @metadata_storage.execution_log
    def create_user_groups(self) -> None:
        usergroups = self.fcc.default_usergroups
        for usergroup in usergroups:
            self.gdata.create_or_update_user_group(user_group_id=usergroup.id)

    @metadata_storage.execution_log
    def assign_workspace_permissions(self) -> None:
        for permission in self.fcc.workspace_permissions:
            self.gdata.assign_workspace_usergoup_permissions(
                workspace_id=permission.workspace_id,
                usergroups=permission.usergroups
            )

    @metadata_storage.execution_log
    def provision_default_users(self) -> None:
        user_group = self.fcc.get_default_usergroup_for_default_users()
        for user in self.fcc.default_users:
            user.user_group_ids = [user_group]
            self.gdata.create_or_update_user(config=user)

    def main(self):
        try:
            self.get_metadata()
            self.get_dataproduct()
            self.create_datasource()
            self.create_empty_parent()
            self.create_empty_child(parent_id=self.fcc.parent_workspace_id)
            self.deploy_dataproduct(
                datasource_id=self.fcc.datasource_id,
                workspace_id=self.fcc.parent_workspace_id
            )
            self.create_user_groups()
            self.assign_workspace_permissions()
            self.provision_default_users()
            print("The execution finished successfully")
        except Exception as ex:
            traceback = logger.get_traceback(ex)
            self.fcc.logger.error(traceback)
            print(f"The execution failed (exeption={ex.__class__.__name__})")
            raise
