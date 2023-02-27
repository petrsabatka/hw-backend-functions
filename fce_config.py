from shared_code import app_config
from shared_code import exceptions
from pathlib import Path
from typing import Dict, Any
from types import SimpleNamespace
import yaml
import json

DECLARATIVE_DATAPRODUCT_PATH = app_config.DOWNLOAD_FOLDER + "/dataproduct"
SCENARIO = "CreateTenant"

class Config:
    def __init__(self, args: Any, config_file: str = 'fce_config.yaml'):
        self.args = args     
        self.config_file = Path(config_file)
        self.child_worksapce_id = app_config.get_child_workspace_id(data_product_id=args.dataproduct, tenant_id=args.tenant)
        self.parent_worksapce_id = app_config.get_parent_workspace_id(data_product_id=args.dataproduct, tenant_id=args.tenant)
        self.datasource_id = app_config.get_datasource_id(data_product_id=args.dataproduct, tenant_id=args.tenant)

    @property
    def config(self) -> Any:
        with open(Path(self.config_file)) as fp:
            dct = yaml.safe_load(fp)
            obj = json.loads(json.dumps(dct), object_hook=lambda d: SimpleNamespace(**d))
            return obj

    @property
    def default_usergroups(self) -> Any:
        default_usergroups = self.config.default_usergroups
        for usergroup in default_usergroups:
            usergroup.id = self.get_usergroup_id(usergroup.name)
        return default_usergroups

    @property
    def workspace_permissions(self) -> Any:
        workspace_permissions = self.config.workspace_permissions
        for permission in workspace_permissions:
            permission.workspace_id = self.get_workspace_id(permission.workspace)
            for usergroup in permission.usergroups:
                usergroup.id = self.get_usergroup_id(usergroup.usergroup)
        return workspace_permissions
    
    def get_usergroup_id(self, usrgroup_name: str) -> str:
        return app_config.get_usergroup_id(data_product_id=self.args.dataproduct,
                                           tenant_id=self.args.tenant, usergroup=usrgroup_name)

    def get_workspace_id(self, workspace_type: str) -> str:
        if workspace_type == 'child':
            return self.child_worksapce_id
        elif workspace_type == 'parent':
            return self.parent_worksapce_id
        raise exceptions.UnknownWorkspaceTypeError(f"{workspace_type=}")
    
    def get_default_usergroup_for_default_users(self):
        return self.get_usergroup_id(usrgroup_name=self.config.default_usergroup_for_default_users)