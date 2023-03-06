import logging
import tempfile
from types import SimpleNamespace

import azure.functions as func

from .code.provision_tenant_analytics import ProvisionTenant


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    dataproduct = req.params.get('dataproduct')
    dataproduct_version = req.params.get('dataproduct_version')
    tenant = req.params.get('tenant')

    if dataproduct and dataproduct_version and tenant:
        args = SimpleNamespace()
        args.dataproduct = dataproduct
        args.dataproduct_version = dataproduct_version
        args.tenant = tenant
        with tempfile.TemporaryDirectory() as temp_dir:
            ProvisionTenant(args, temp_dir).main()
        return func.HttpResponse(f"{args=}\n\nThe execution finished successfully")
    else:
        return func.HttpResponse(
             "Pass dataproduct=&dataproduct_version=&tenant= in the query string to trigger provisioning.",
             status_code=200
        )
