#!/usgs/apps/anaconda/bin/python

import os, subprocess, sys
import json
import redis
from collections import OrderedDict

from Recipe import *

class Process(object):

    def __init__(self):

        self.processName = ""

    def Process2JSON(self):

#        processSTR = json.dumps(element)
        processSTR = json.dumps(self.process)
        return processSTR

    def JSON2Process(self, element):
        
        JSONout = json.loads(element, object_pairs_hook=OrderedDict)
 
        processDict = {}
        for process in JSONout:
            processDict[str(process)] = OrderedDict()
            self.process = processDict
            self.processName = process
            for key, value in JSONout[process].items():
                self.process[self.processName][str(key)] = str(value)
 
        return JSONout

    def Process2Redis(self, redisOBJ):

        jsonSTR = json.dumps(self.process)
        redisOBJ.QueueAdd(jsonSTR)

    def setProcess(self, process):
        self.processName = str(process)

    def ChangeProcess(self, newproc):

        NewDict = {}
        NewDict[newproc] = OrderedDict()
        for k, v, in self.process[self.processName].items():
            NewDict[newproc][k] = v
        self.process = NewDict        
        self.processName = newproc

    def getProcess(self):
        return self.process

    def getProcessName(self):
        return self.processName

    def LogCommandline(self):
        tempSTR = self.processName
        for key, value in self.process[self.processName].items():
            if key == 'from_' or key == 'to' or key == 'map':
                subfile = value.split('/')
                value = subfile[-1]
            tempSTR += ' ' + key + '=' + value
            
        commandSTR = tempSTR.replace('from_', 'from')         
        return commandSTR

    def LogHelpLink(self):
        helplink = 'https://isis.astrogeology.usgs.gov/Application/presentation/Tabbed/' + self.processName + '/' + self.processName + '.html'
        return helplink 

    def ProcessFromRecipe(self, process, recipe): 

        for Rprocess in recipe:
            for key, value in Rprocess.items():
                if key == process:
                    self.processName = key
                    self.process = Rprocess
        return self.process

    def updateParameter(self, param, newValue):
            
        for key, value in self.process[self.processName].items():
            if key == param:
                self.process[self.processName][key] = newValue

    def newProcess(self, process):

        processDict = {}
        processDict[process] = OrderedDict()
        self.process = processDict
        self.processName = process

    def AddParameter(self, param, newValue):

        testDict = {param: newValue}

        test = []
        test.append(param)
        test.append(newValue)
        
        for k, v in testDict.items():
            self.process[self.processName][str(k)] = str(v)  

    def GDAL_OBit(self, ibit):

        bitDICT = {'unsignedbyte': 'Byte',
                   'signedword': 'Int16',
                   'real': 'Float32'
                  }

        return bitDICT[ibit]

    def GDAL_Creation(self, format):

        cDICT = {'JPEG': 'quality=100',
                 'JP2KAK': 'quality=100',
                 'GTiff': 'bigtiff=if_safer'
                 }

        return cDICT[format]

 
          



