#!/usgs/apps/anaconda/bin/python

import redis
from config import redis_info as ri

class RedisHash(object):

    def __init__(self, name, namespace='user'):

        self.__db=redis.StrictRedis(host=ri['host'], port=ri['port'], db=ri['db'])
        self.id_name = '%s:%s' %(namespace, name)

    def IsInHash(self, element):
        test = self.__db.hexists(self.id_name, element)
        return test

    def HashCount(self):
        item = self.__db.hlen(self.id_name)
        return item

    def AddHash(self, element):
        self.__db.hmset(self.id_name, element)
 
    def RemoveAll(self):
        self.__db.delete(self.id_name)

    def Status(self, element):
        self.__db.hset(self.id_name, 'status', element)
  
    def MAPname(self, element):
        self.__db.hset(self.id_name, 'MAPname', element)
     
    def getMAPname(self):
        item = self.__db.hget(self.id_name, 'MAPname')
        return item

    def getStatus(self):
        item = self.__db.hget(self.id_name, 'status')
        return item

    def FileCount(self, element):
        self.__db.hset(self.id_name, 'filecount', element)

    def getFileCount(self):
        item = self.__db.hget(self.id_name, 'filecount')
        return item 

    def Service(self):
        item = self.__db.hget(self.id_name, 'service')
        return item

    def Format(self): 
        item = self.__db.hget(self.id_name, 'fileformat')
        return item

    def OutBit(self):
        item = self.__db.hget(self.id_name, 'outbit')
        return item

    def getGRtype(self):
        item = self.__db.hget(self.id_name, 'grtype')
        return item

    def getMinLat(self):
        item = self.__db.hget(self.id_name, 'minlat')
        return item

    def getMaxLat(self):
        item = self.__db.hget(self.id_name, 'maxlat')
        return item

    def getMinLon(self):
        item = self.__db.hget(self.id_name, 'minlon')
        return item

    def getMaxLon(self):
        item = self.__db.hget(self.id_name, 'maxlon')
        return item

    def addError(self, infile, error):
        self.__db.hset(self.id_name, infile, error)
  
    def getKeys(self):
        item = self.__db.hkeys(self.id_name) 
        return item

    def getError(self, file):
        item = self.__db.hget(self.id_name, file)
        return item 
