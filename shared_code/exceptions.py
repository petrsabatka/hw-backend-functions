class NotFoundInMetadataStorageError(Exception):
    """The metadata_storage query did not return any data"""

class UnknownWorkspaceTypeError(Exception):
    """Unknown Workspace Type"""

class MissingEnvironmentVariablesError(Exception):
    """Missing Environment Variables Detected"""
