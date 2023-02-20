# hw-backend-functions
## Initial Setup
### Setup virtual environment

```bash
# Create virtual env
python3 -m venv .venv
# Activate virtual env
source .venv/bin/activate
#You should see a `(.venv)` appear at the beginning of your terminal prompt indicating that you are working inside the `virtualenv`.
# Deactivate virtual env once you are done
deactivate

```

### Install dependencies
```bash
pip install -r requirements.txt

```

### Edit `config.yaml` file
If you want to test against your gooddata, edit host in gooddata dictionary. The host token is stored in gooddata_token environment variable described below.

## Tenant provisioning
Single tenant provisioning. Script creates one datasource and two workspaces in gooddata. When you run script with diferent parameters, another set of workspaces is provisioned. When you re-run script with the same parameters, resources are overwritten.

Workspaces are in parent-child relationship. Frontend is only in parent workspace, child workspace is for user's customizations.

### Activate virtual env
```bash
source .venv/bin/activate

```
### Set environment variables
```bash
export gooddata_token=***
export metadata_storage_password=***
export azure_storage_connection_string=***
export datasource_password=***

```
### Run script
```bash
# check required arguments
python3 provision_tenant_analytics.py -h
# provision tenat tenant1 based on dataproduct dp1
python3 provision_tenant_analytics.py --tenant tenant1 --dataproduct dp1 --dataproduct_version v01
# short version of the provision tenant call
python3 provision_tenant_analytics.py -t tenant1 -p dp1 -v v01

```