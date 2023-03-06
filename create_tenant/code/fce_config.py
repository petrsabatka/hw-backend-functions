import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from shared_code import app_config, exceptions, logger

from .mocks import Mocks

SCENARIO = "CreateTenant"
FCE_CONFIG_FILE_NAME = 'fce_config.yaml'

REQUIRED_ENVIRON_FCE = {'datasource_password': 'datasource_password'}
REQUIRED_ENVIRON = {
    **app_config.REQUIRED_ENVIRON_METADATA_STORAGE,
    **app_config.REQUIRED_ENVIRON_GOODDATA,
    **app_config.REQUIRED_ENVIRON_DATAPRODUCT_REPOSITORY,
    **REQUIRED_ENVIRON_FCE
}

class FceConfig:
    def __init__(self, args: Any, temp_dir):
        self.check_required_environ()
        self.logger = logger.get_logger(SCENARIO)
        self.dataproduct = args.dataproduct
        self.dataproduct_version = args.dataproduct_version
        self.tenant = args.tenant
        self.default_users = Mocks(args).default_users
        self.child_workspace_id = app_config.get_child_workspace_id(
            data_product_id=args.dataproduct,
            tenant_id=args.tenant
        )
        self.parent_workspace_id = app_config.get_parent_workspace_id(
            data_product_id=args.dataproduct,
            tenant_id=args.tenant
        )
        self.datasource_id = app_config.get_datasource_id(
            data_product_id=args.dataproduct,
            tenant_id=args.tenant
        )
        self.declarative_dataproduct_path = os.path.join(temp_dir, 'dataproduct')
        self.metadata_storage_config = app_config.get_metadata_storage_config(
            tenant=args.tenant,
            scenario=SCENARIO,
            logger=self.logger
        )
        self.gooddata_config = app_config.get_gooddata_config(logger=self.logger)
        self.dataproduct_repository_config = app_config.get_dataproduct_repository_config(
            logger=self.logger
        )

    @property
    def config(self) -> Any:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(current_dir, FCE_CONFIG_FILE_NAME)
        with open(Path(config_file), 'r', encoding='utf-8') as file:
            dct = yaml.safe_load(file)
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
        return app_config.get_usergroup_id(
            data_product_id=self.dataproduct,
            tenant_id=self.tenant,
            usergroup=usrgroup_name
        )

    def get_workspace_id(self, workspace_type: str) -> str:
        if workspace_type == 'child':
            return self.child_workspace_id
        elif workspace_type == 'parent':
            return self.parent_workspace_id
        raise exceptions.UnknownWorkspaceTypeError(f"{workspace_type=}")

    def get_default_usergroup_for_default_users(self):
        return self.get_usergroup_id(
            usrgroup_name=self.config.default_usergroup_for_default_users
        )

    def check_required_environ(self):
        required_environ = REQUIRED_ENVIRON.keys()
        missing_environ = []
        for var in required_environ:
            value = os.getenv(var)
            if value is None:
                missing_environ.append(var)

        if missing_environ:
            raise exceptions.MissingEnvironmentVariablesError(f"{SCENARIO=}, {missing_environ=}")
