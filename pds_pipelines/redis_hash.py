#!/usr/bin/env python

import redis
from pds_pipelines.config import redis_info as ri


class RedisHash(object):

    def __init__(self, name, namespace='user'):
        """
        Parameters
        ----------
        name : str
        namespace : str
        """

        self._db = redis.StrictRedis(
            host=ri['host'], port=ri['port'], db=ri['db'])
        self.id_name = '%s:%s' % (namespace, name)


    def IsInHash(self, element):
        """
        Parameters
        ----------
        element : str

        Returns
        ------
        str
            test
        """
        test = self._db.hexists(self.id_name, element)
        return test


    def HashCount(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hlen(self.id_name)
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass
        return item


    def AddHash(self, element):
        """
        Parameters
        ----------
        element : str
        """
        self._db.hmset(self.id_name, element)


    def RemoveAll(self):
        self._db.delete(self.id_name)


    def Status(self, element):
        """
        Parameters
        ----------
        element : str
        """
        self._db.hset(self.id_name, 'status', element)
        

    def MAPname(self, element):
        """
        Parameters
        ----------
        element : str
        """
        self._db.hset(self.id_name, 'MAPname', element)


    def getMAPname(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'MAPname')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getStatus(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'status')

        try:
            item =  item.decode('utf-8')
        except AttributeError:
            pass

        return item

    def FileCount(self, element):
        """
        Parameters
        ----------
        element : str
        """
        self._db.hset(self.id_name, 'filecount', element)


    def getFileCount(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'filecount')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def Service(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'service')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def Format(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'fileformat')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def OutBit(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'outbit')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getGRtype(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'grtype')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getMinLat(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'minlat')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getMaxLat(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'maxlat')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getMinLon(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'minlon')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getMaxLon(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, 'maxlon')
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def addError(self, infile, error):
        """
        Parameters
        ----------
        infile : str
        error : str

        Returns
        ------
        str
            item
        """
        self._db.hset(self.id_name, infile, error)


    def getKeys(self):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hkeys(self.id_name)
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item


    def getError(self, input_file):
        """
        Returns
        ------
        str
            item
        """
        item = self._db.hget(self.id_name, input_file)
        try:
            item = item.decode('utf-8')
        except AttributeError:
            # If the item is not found, it can't be decoded
            pass

        return item
