import argparse

def provision_tenant_args(parser: argparse.ArgumentParser):
    parser.add_argument('-t', '--tenant',  type=str, required=True,
                        help='Tenant ID to be created or updated.')
    parser.add_argument('-p', '--dataproduct',  type=str, required=True,
                        help='Product ID, the template with tenant definition.')
    parser.add_argument('-v', '--dataproduct_version',  type=str, required=True,
                        help='Product ID version')
    parser.add_argument('-c', '--config', default='config.yaml',
                        help="Config file (default='config.yaml')")
    parser.add_argument('-d', '--debug', action='store_true', default=False,
                        help='Increase logging level to DEBUG (default=False)')
