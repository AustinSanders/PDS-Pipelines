#!/usgs/apps/anaconda/bin/python

import redis
from config import redis_info as ri

class RedisQueue(object):

    def __init__(self, name, namespace='queue'):

        # self.__db=redis.Redis(host=ri['host'], port=ri['port'], db=ri['db'])
        self.__db=redis.StrictRedis(host=ri['host'], port=ri['port'], db=ri['db'])
        self.id_name = '%s:%s' %(namespace, name)

    def RemoveAll(self):
        self.__db.delete(self.id_name)

    def getQueueName(self):
        return self.id_name

    def QueueSize(self):
        return self.__db.llen(self.id_name)

    def QueueAdd(self, element):
        self.__db.rpush(self.id_name, element)

    def QueueGet(self):
        item = self.__db.rpop(self.id_name)
        return item
    
    def ListGet(self):
        list = self.__db.lrange(self.id_name, 0, -1)
        return list

    def RecipeGet(self): 
        recipe = self.__db.lrange(self.id_name, 0, -1)
        return recipe

    def QueueRemove(self, element):
        value = int(0)
        self.__db.lrem(self.id_name, value, element)

    def Qfile2Qwork(self, popQ, pushQ):
        item = self.__db.rpoplpush(popQ, pushQ)
        return item
