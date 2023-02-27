from shared_code.azure_blob_storage import DirectoryClient
from logging import Logger
from typing import Dict
from shared_code import app_config
from pathlib import Path
from types import SimpleNamespace
from typing import Any
import shutil

class DataproductRepository:
    def __init__(self, logger: Logger, args: Any, config: Dict) -> None:
        self.logger = logger
        self.args = args
        self.config = SimpleNamespace(**config)
        self._client = self._get_client()

    def _get_client(self) -> DirectoryClient:
        masked_config = " ".join(list(filter(lambda keyval: 'AccountName' in keyval, self.config.connection_string.split(';'))))
        masked_config += f" container_name={self.config.container_name}"
        self.logger.info(f"Connecting to dataproduct_repository {masked_config}")
        return DirectoryClient(self.config.connection_string, self.config.container_name)

    def get_declarative_dataproduct(self, storage_path: str) -> None:
        dest = Path(app_config.DECLARATIVE_DATAPRODUCT_PATH)
        if dest.exists():
            self.logger.info(f"Deleting dataproduct stage ({dest=})")
            shutil.rmtree(dest)        
        
        dirs = self._client.ls_dirs(path=storage_path, recursive=True)
        subdirs = set(map(lambda relpath: relpath.split('/')[0], dirs))
        self.logger.info(f"Getting dataproduct from dataproduct_repository ({storage_path=})")
        for dir in subdirs:
            self._client.download(source=f"{storage_path}/{dir}", dest=str(dest))