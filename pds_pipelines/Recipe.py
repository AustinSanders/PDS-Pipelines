#!/usgs/apps/anaconda/bin/python

import os
import subprocess
import sys

import redis
import json
from collections import OrderedDict

from pds_pipelines.Process import *
from pds_pipelines.RedisQueue import *

from pds_pipelines.config import recipe_dict


class Recipe(Process):
    """
    Parameters
    ----------
    Process

    Attributes
    ----------
    recipe : list
    """

    def __init__(self):

        self.recipe = []

    def AddJsonFile(self, file, proc):
        """
        Parameters
        ----------
        file : str
        """

        testjson = json.loads(open(file).read(), object_pairs_hook=OrderedDict)

        for IP in testjson[proc]['recipe']:
            process = str(IP)
            processDict = {}
            processDict[process] = OrderedDict()
            for k, v in testjson[proc]['recipe'][process].items():
                processDict[process][str(k)] = str(v)

            self.recipe.append(processDict)

    def getRecipe(self):
        """
        Returns
        -------
        list
            self.recipe
        """
        return self.recipe

    def getRecipeJSON(self, mission):
        """
        Parameters
        ----------
        mission : str

        Returns
        -------
        str
            output
        """

        return recipe_dict[mission]


    def getProcesses(self):
        """
        Returns
        -------
        list
            processList
        """

        processList = []
        for Tkey in self.recipe:
            for key, value in Tkey.items():
                processList.append(key)

        return processList

    def AddProcess(self, process):
        """
        Parameters
        ----------
        process : str
        """
        self.recipe.append(process)

    def TestgetStep(self, file):
        """
        Parameters
        ----------
        file : str

        Returns
        -------
        list
            stepList
        """
        stepList = []
        testjson = json.loads(open(file).read(), object_pairs_hook=OrderedDict)

        for IP in testjson['recipe']:
            stepList.append(IP)

        return stepList
