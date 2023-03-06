from pathlib import Path
from types import SimpleNamespace

from shared_code.azure_blob_storage import DirectoryClient


class DataproductRepository:
    def __init__(self, config: SimpleNamespace) -> None:
        self.logger = config.logger
        self._client = self._get_client(
            config=config.config,
            masked_config=config.config_masked
        )

    def _get_client(self, config: SimpleNamespace, masked_config: str) -> DirectoryClient:
        self.logger.info(f"Connecting to dataproduct_repository {masked_config}")
        return DirectoryClient(connection_string=config.connection_string,
                               container_name=config.container_name)

    def get_declarative_dataproduct(self, storage_path: str, dest_path: str) -> None:
        dest = Path(dest_path)
        dirs = self._client.ls_dirs(path=storage_path, recursive=True)
        subdirs = set(map(lambda relpath: relpath.split('/')[0], dirs))
        self.logger.info(f"Getting dataproduct from dataproduct_repository ({storage_path=})")
        for directory in subdirs:
            self._client.download(source=f"{storage_path}/{directory}", dest=str(dest))
