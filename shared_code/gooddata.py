from logging import Logger
from pathlib import Path
from typing import Any, Optional, Dict, List
from gooddata_sdk import (
        GoodDataSdk, BasicCredentials, CatalogDataSourcePostgres, PostgresAttributes,
        CatalogWorkspace, CatalogUserGroup, CatalogUser
)
from gooddata_sdk.catalog.permission.declarative_model.permission import CatalogDeclarativeWorkspacePermissions

def get_sdk(host: str, token: str, logger: Logger) -> GoodDataSdk:
    masked_token = f"{len(token[:-4])*'#'}{token[-4:]}"
    logger.info(
        f"Connecting to GoodData ({host=}, token={masked_token})"
    )
    sdk = GoodDataSdk.create(host, token)
    return sdk

def create_or_update_data_source(sdk: GoodDataSdk, logger: Logger, config: Any) -> str:
    masked_config = {k:v for k,v in config.__dict__.items() if k != 'password'}
    logger.info(f"Creating datasource (config={masked_config})")
    
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
    sdk.catalog_data_source.create_or_update_data_source(data_source=data_source)

def create_or_update_workspace(sdk: GoodDataSdk, logger: Logger, id: str,
                               name: str, parent_id: Optional[str] = None) -> str:
    if parent_id:
        logger.info(f"Creating workspace ({id=}, {parent_id=})")
        workspace = CatalogWorkspace(workspace_id=id, name=name, parent_id=parent_id)
    else:
        logger.info(f"Creating workspace ({id=})")
        workspace = CatalogWorkspace(workspace_id=id, name=name)
    sdk.catalog_workspace.create_or_update(workspace=workspace)

def put_declarative_pdm(sdk: GoodDataSdk, logger: Logger, src_dir: Path,
                        datasource_id: str) -> None:
    logger.info(f"Putting pdm (tgt_datasource={datasource_id}, {src_dir=})")
    pdm = sdk.catalog_data_source.load_pdm_from_disk(path=src_dir)
    sdk.catalog_data_source.put_declarative_pdm(data_source_id=datasource_id, declarative_tables=pdm)

def put_declarative_ldm(sdk: GoodDataSdk, logger: Logger, src_dir: Path, 
                        workspace_id: str, datasource_id: str) -> None:
    logger.info(f"Putting ldm (tgt_ws={workspace_id}, {src_dir=}, {datasource_id=})")
    ldm = sdk.catalog_workspace_content.load_ldm_from_disk(path=src_dir)

    if datasets := ldm.to_dict().get("ldm").get("datasets"):
        data_source_mapping = {}
        for dataset in datasets:
            if disk_datasource_id := dataset.get('dataSourceTableId').get('dataSourceId'):
                data_source_mapping[disk_datasource_id] = datasource_id
        ldm.modify_mapped_data_source(data_source_mapping)

    sdk.catalog_workspace_content.put_declarative_ldm(workspace_id=workspace_id, ldm=ldm)

def put_declarative_am(sdk: GoodDataSdk, logger: Logger, src_dir: Path, workspace_id: str) -> None:
    logger.info(f"Putting am  (tgt_ws={workspace_id}, {src_dir=})")
    am = sdk.catalog_workspace_content.load_analytics_model_from_disk(path=src_dir)
    sdk.catalog_workspace_content.put_declarative_analytics_model(workspace_id=workspace_id, analytics_model=am)

def create_or_update_user_group(sdk: GoodDataSdk, logger: Logger, user_group_id: str) -> None:
    logger.info(f"Creating user group ({user_group_id=})")    
    user_group = CatalogUserGroup.init(user_group_id=user_group_id)
    sdk.catalog_user.create_or_update_user_group(user_group=user_group)

def _build_permission(id: str, type: str, name: str) -> Dict:
    return {
              "assignee": {
                  "id": id,
                  "type": type
              },
              "name": name
           }

def _build_workspace_permissions(perm: List, hierarchy_perm: List = []) -> Dict:
    return {
              "hierarchyPermissions": hierarchy_perm,
              "permissions": perm
           }

def assign_workspace_usergoup_permissions(sdk: GoodDataSdk, logger: Logger, workspace_id: str,
                                          usergroups: Any) -> None:
    permissions = [_build_permission(id=g.id, type='userGroup', name=g.permission) for g in usergroups]
    workspace_perm =  _build_workspace_permissions(perm=permissions)
    logger.info(f"Assigning workspace permissions ({workspace_id=}, {workspace_perm=})")
    catalog_perm = CatalogDeclarativeWorkspacePermissions.from_dict(workspace_perm, camel_case=True)
    sdk.catalog_permission.put_declarative_permissions(
        workspace_id=workspace_id, declarative_workspace_permissions=catalog_perm)

def create_or_update_user(sdk: GoodDataSdk, logger: Logger, config: Any) -> None:
    user_exist  = next((u for u in sdk.catalog_user.list_users() if u.id == config.user_id), None)
    if user_exist:
        user_group_ids = list(set(user_exist.get_user_groups + config.user_group_ids))
    else:
        user_group_ids = config.user_group_ids
    user = CatalogUser.init(user_id=config.user_id, user_group_ids=user_group_ids)
    logger.info(f"Creating user ({user=})")
    sdk.catalog_user.create_or_update_user(user=user)