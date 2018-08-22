#!/usgs/apps/anaconda/bin/python

import os, subprocess, sys

import redis
import json
from collections import OrderedDict

from Process import *
from RedisQueue import *

class Recipe(Process):

    def __init__(self):

        self.recipe = []

    def AddJsonFile(self, file):

        testjson = json.loads(open(file).read(), object_pairs_hook=OrderedDict)
        
        for IP in testjson['recipe']:
            process = str(IP)
            processDict = {}
            processDict[process] = OrderedDict()
            for k, v in testjson['recipe'][process].items(): 
                processDict[process][str(k)] = str(v)

            self.recipe.append(processDict)

    def getRecipe(self):

        return self.recipe

    def getRecipeJSON(self, mission, process):

        if process == 'service':
            servicedict = {'ISSNA': '/usgs/cdev/PDS/recipe/POWrecipeISSNA.json',
                           'CTX': '/usgs/cdev/PDS/recipe/POWrecipeCTX.json',
                           'SSI': '/usgs/cdev/PDS/recipe/POWrecipe_galileoSSI.json',
                           'NIR': '/usgs/cdev/PDS/recipe/POWrecipeCLEM_NIR.json',
                           'LWIR': '/usgs/cdev/PDS/recipe/POWrecipeCLEM_LWIR.json',
                           'HIRES': '/usgs/cdev/PDS/recipe/POWrecipeCLEM_HIRES.json',
                           'UVVIS': '/usgs/cdev/PDS/recipe/POWrecipeCLEM_UVVIS.json',
                           'THEMIS_IR': '/usgs/cdev/PDS/recipe/POWrecipeTHMIR.json',
                           'NACL': '/usgs/cdev/PDS/recipe/POWrecipeLRO_NACL.json',
                           'NACR': '/usgs/cdev/PDS/recipe/POWrecipeLRO_NACR.json',
                           'MOC-NA': '/usgs/cdev/PDS/recipe/POWrecipe_MOCNA.json',
                           'MOC_WA': '/usgs/cdev/PDS/recipe/POWrecipe_MOCWA.json',
                           'MDIS-NAC': '/usgs/cdev/PDS/recipe/POWrecipeMDIS_NAC.json',
                           'MDIS-WAC': '/usgs/cdev/PDS/recipe/POWrecipeMDIS_WAC.json',
                           'MOC-WA': '/usgs/cdev/PDS/recipe/POWrecipeMGS_MOCWA.json',
                           'VISUAL_IMAGING_SUBSYSTEM_CAMERA_A': '/usgs/cdev/PDS/recipe/POWrecipeVikVisA.json',
                           'VISUAL_IMAGING_SUBSYSTEM_CAMERA_B': '/usgs/cdev/PDS/recipe/POWrecipeVikVisB.json',
                           'MAP': '/usgs/cdev/PDS/recipe/MAPrecipe.json'
                          }
            output = servicedict[mission] 
        elif process == 'upc':
            upcdict = {'mroCTX': '/usgs/cdev/PDS/recipe/UPCrecipeCTX.json',
                       'themisIR_EDR': '/usgs/cdev/PDS/recipe/UPCrecipeTHMIR.json',
                       'themisVIS_EDR': '/usgs/cdev/PDS/recipe/UPCrecipeTHMVIS.json',
                       'lrolrcEDR': '/usgs/cdev/PDS/recipe/UPCrecipeLROLROC_EDR.json'
                      }
            output = upcdict[mission]
        
        elif process == 'thumbnail':
            thumbdict = {'mroCTX': '/usgs/cdev/PDS/recipe/thumbnailrecipeCTX.json',
                         'themisIR_EDR': '/usgs/cdev/PDS/recipe/thumbnailrecipeTHMIR.json',  
                         'lrolrcEDR': '/usgs/cdev/PDS/recipe/thumbnailrecipeLROLROC_EDR.json'
                        
                        }
            output = thumbdict[mission]

        elif process == 'browse':
            browsedict = {'mroCTX': '/usgs/cdev/PDS/recipe/browserecipeCTX.json', 
                          'themisIR_EDR': '/usgs/cdev/PDS/recipe/browserecipeTHMIR.json',
                          'lrolrcEDR': '/usgs/cdev/PDS/recipe/browserecipeLROLROC_EDR.json'
                         }
            output = browsedict[mission]

        elif process == 'projectionbrowse':
            projectionbrowsedict = {'CTX': '/usgs/cdev/PDS/recipe/projectionbrowserecipeCTX.json',
                                   'NACL': '/usgs/cdev/PDS/recipe/projectionbrowserecipeLRO_NACL.json'
                                   }
            output = projectionbrowsedict[mission]
  

        return output




    def getProcesses(self):

        processList = []
        for Tkey in self.recipe:
            for key, value in Tkey.items():
                processList.append(key)

        return processList 

    def AddProcess(self, process):

        self.recipe.append(process)

    def TestgetStep(self, file):
        stepList = []
        testjson = json.loads(open(file).read(), object_pairs_hook=OrderedDict)
 
        for IP in testjson['recipe']:
            stepList.append(IP)

        return stepList

    def TestRecipe(self, file, element):

        testjson = json.loads(open(file).read(), object_pairs_hook=OrderedDict)

        for IP in testjson['recipe'][element]:
            process = str(IP)
            processDict = {}
            processDict[process] = OrderedDict()
            for k, v in testjson['recipe'][element][process].items():
                processDict[process][str(k)] = str(v)

            self.recipe.append(processDict)




