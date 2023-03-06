from types import SimpleNamespace
from typing import Any, List


def get_default_users(tenant: str) -> List:
    tenant_owner = SimpleNamespace()
    tenant_owner.user_id = f"tenant.owner.{tenant}.com"
    tenant_owner.first_name = 'Tenant'
    tenant_owner.second_name = 'Owner'
    tenant_owner.email = f"tenant.owner@{tenant}.com"

    global_admin = SimpleNamespace()
    global_admin.user_id = 'global.admin.honeywell.com'
    global_admin.first_name = 'Global'
    global_admin.second_name = 'Admin'
    global_admin.email = 'global.admin@honeywell.com'

    return [tenant_owner, global_admin]

class Mocks:
    def __init__(self, args: Any):
        self.args = args
        self.default_users = get_default_users(args.tenant)
