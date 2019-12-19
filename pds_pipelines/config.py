import os
# Database credentials
credentials = {'test': {'user': 'upcmgr',
                        'pass': '1234',
                        'host': 'localhost',
                        'port': '9010',
                        'db': 'upc'}
               }

# Redis path(?) info
redis_info = {'host': 'localhost', 'port': '6379', 'db': '0'}

# POW / MAP2 base path
pow_map2_base = '/pds_san/PDS_Services/'

web_base = 'https://pdsimage.wr.usgs.gov/Missions/'
archive_base = '/pds_san/PDS_Archive/'
link_dest = '/pds_san/PDS_Archive_Links/'

# Recipe base path
recipe_base = '/home/acpaquette/repos/PDS-Pipelines/recipe/new/'

pds_info = '/home/acpaquette/repos/PDS-Pipelines/pds_pipelines/PDSinfo.json'

pds_log = '/home/acpaquette/repos/PDS-Pipelines/logs/'

slurm_log = '/home/acpaquette/repos/PDS-Pipelines/output/'

cmd_dir = '/home/acpaquette/repos/PDS-Pipelines/pds_pipelines/'

keyword_def = '/home/acpaquette/repos/PDS-Pipelines/recipe/Keyword_Definition.json'

# workarea = '/home/acpaquette/repos/PDS-Pipelines/products/'
workarea = '/home/acpaquette/repos/PDS-Pipelines/workarea/'
default_namespace = 'adampaquette_queue'

pds_db = 'pdsdi'
upc_db = 'upcprd'

scratch = '/scratch/pds_services/'

lock_obj = 'processes'

upc_error_queue = 'UPC_ErrorQueue'

disk_usage_ratio = 0.4
