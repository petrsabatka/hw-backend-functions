from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from gooddata_sdk import (BasicCredentials, CatalogDataSourcePostgres,
                          CatalogUser, CatalogUserGroup, CatalogWorkspace,
                          GoodDataSdk, PostgresAttributes)
from gooddata_sdk.catalog.permission.declarative_model.permission import \
    CatalogDeclarativeWorkspacePermissions


class GoodData:
    def __init__(self, gooddata_config: SimpleNamespace) -> None:
        self.logger = gooddata_config.logger
        self.sdk = self.get_sdk(
            config=gooddata_config.config,
            config_masked=gooddata_config.config_masked
        )

    def get_sdk(self, config: SimpleNamespace, config_masked: str) -> GoodDataSdk:
        self.logger.info(
            f"Connecting to GoodData ({config_masked})"
        )
        sdk = GoodDataSdk.create(config.host, config.token)
        return sdk

    def create_or_update_data_source(self, config: Any) -> None:
        masked_config = {k:v for k,v in config.__dict__.items() if k != 'password'}
        self.logger.info(f"Creating datasource (config={masked_config})")

        data_source = CatalogDataSourcePostgres(
            id=config.id,
            name=config.name,
            db_specific_attributes=PostgresAttributes(
                host=config.host,
                db_name=config.db_name,
                port=config.port),
            schema=config.schema,
            credentials=BasicCredentials(
                username=config.username,
                password=config.password,
            )
        )
        self.sdk.catalog_data_source.create_or_update_data_source(
            data_source=data_source
        )

    def create_or_update_workspace(
        self,
        workspace_id: str,
        name: str,
        parent_id: Optional[str] = None
    ) -> None:
        if parent_id:
            self.logger.info(f"Creating workspace ({workspace_id=}, {parent_id=})")
            workspace = CatalogWorkspace(workspace_id=workspace_id, name=name, parent_id=parent_id)
        else:
            self.logger.info(f"Creating workspace ({workspace_id=})")
            workspace = CatalogWorkspace(workspace_id=workspace_id, name=name)
        self.sdk.catalog_workspace.create_or_update(workspace=workspace)

    def put_declarative_pdm(
        self,
        src_dir: Path,
        datasource_id: str
    ) -> None:
        self.logger.info(f"Putting pdm (tgt_datasource={datasource_id}, {src_dir=})")
        pdm = self.sdk.catalog_data_source.load_pdm_from_disk(path=src_dir)
        self.sdk.catalog_data_source.put_declarative_pdm(
            data_source_id=datasource_id,
            declarative_tables=pdm
        )

    def put_declarative_ldm(
        self,
        src_dir: Path,
        workspace_id: str,
        datasource_id: str
    ) -> None:
        self.logger.info(f"Putting ldm (tgt_ws={workspace_id}, {src_dir=}, {datasource_id=})")
        ldm = self.sdk.catalog_workspace_content.load_ldm_from_disk(path=src_dir)

        if ldm_object := ldm.to_dict().get("ldm"):
            if datasets := ldm_object.get("datasets"):
                data_source_mapping = {}
                for dataset in datasets:
                    if data_source_table_id := dataset.get('dataSourceTableId'):
                        if disk_datasource_id := data_source_table_id.get('dataSourceId'):
                            data_source_mapping[disk_datasource_id] = datasource_id
                ldm.modify_mapped_data_source(data_source_mapping)

        self.sdk.catalog_workspace_content.put_declarative_ldm(
            workspace_id=workspace_id, ldm=ldm
        )

    def put_declarative_am(self, src_dir: Path, workspace_id: str) -> None:
        self.logger.info(f"Putting am  (tgt_ws={workspace_id}, {src_dir=})")
        am = self.sdk.catalog_workspace_content.load_analytics_model_from_disk(
            path=src_dir
        )
        self.sdk.catalog_workspace_content.put_declarative_analytics_model(
            workspace_id=workspace_id,
            analytics_model=am
        )

    def create_or_update_user_group(self, user_group_id: str) -> None:
        self.logger.info(f"Creating user group ({user_group_id=})")
        user_group = CatalogUserGroup.init(user_group_id=user_group_id)
        self.sdk.catalog_user.create_or_update_user_group(user_group=user_group)

    def _build_permission(
            self, assignee_id: str, assignee_type: str, name: str
    ) -> Dict:
        return {
                "assignee": {
                    "id": assignee_id,
                    "type": assignee_type
                },
                "name": name
            }

    def _build_workspace_permissions(
            self, perm: List, hierarchy_perm: List = []
    ) -> Dict:
        return {
                "hierarchyPermissions": hierarchy_perm,
                "permissions": perm
            }

    def assign_workspace_usergoup_permissions(
            self, workspace_id: str, usergroups: Any
    ) -> None:
        permissions = [self._build_permission(assignee_id=g.id, assignee_type='userGroup', name=g.permission) for g in usergroups]
        workspace_perm =  self._build_workspace_permissions(perm=permissions)
        self.logger.info(f"Assigning workspace permissions ({workspace_id=}, {workspace_perm=})")
        catalog_perm = CatalogDeclarativeWorkspacePermissions.from_dict(
            workspace_perm, camel_case=True
        )
        self.sdk.catalog_permission.put_declarative_permissions(
            workspace_id=workspace_id,
            declarative_workspace_permissions=catalog_perm
        )

    def create_or_update_user(self, config: Any) -> None:
        users = self.sdk.catalog_user.list_users()
        user_exist  = next((u for u in users if u.id == config.user_id), None)
        if user_exist:
            user_group_ids = list(set(user_exist.get_user_groups + config.user_group_ids))
        else:
            user_group_ids = config.user_group_ids
        user = CatalogUser.init(user_id=config.user_id, user_group_ids=user_group_ids)
        self.logger.info(f"Creating user ({user=})")
        self.sdk.catalog_user.create_or_update_user(user=user)
