import redis
import json
from pds_pipelines.config import redis_info as ri
from pds_pipelines.config import default_namespace

redis_queues = {'DI':'DI_ReadyQueue', 'Browse':'Browse_ReadyQueue', 'UPC':'UPC_ReadyQueue', 'Thumbnail':'Thumbnail_ReadyQueue', 'Ingest':'Ingest_ReadyQueue', 'Pilot': 'PilotB_ReadyQueue'}
redis_keys = {'NFS':'nfs_load'}

rdb = redis.StrictRedis(host=ri['host'], port=ri['port'], db=ri['db'])

status = {}

for key, value in redis_queues.items():
    queue_name = '%s:%s' % (default_namespace, value)
    status[key] = rdb.llen(queue_name)

for key, value in redis_keys.items():
    status[key] = rdb.llen(value)

# json.dumps(status)
print (status)
