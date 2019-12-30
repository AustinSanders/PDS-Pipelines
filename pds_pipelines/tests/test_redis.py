from ast import literal_eval

from pds_pipelines.RedisLock import RedisLock
from pds_pipelines.RedisQueue import RedisQueue

def test_redis_queue():
    # Redis Queue Objects
    RQ_main = RedisQueue('UPC_ReadyQueue')
    RQ_main.QueueAdd(("/Path/to/my/file.img", "1", "ARCHIVE"))

    if int(RQ_main.QueueSize()) > 0:
        # get a file from the queue
        item = literal_eval(RQ_main.QueueGet())
        inputfile = item[0]
        fid = item[1]
        archive = item[2]
        
    assert inputfile == "/Path/to/my/file.img"
    assert fid == "1"
    assert archive == "ARCHIVE"
