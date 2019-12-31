#!/usr/bin/env python

import os
import subprocess
import sys

import redis
import json
from collections import OrderedDict

from pds_pipelines.process import Process
from pds_pipelines.redis_queue import RedisQueue

from pds_pipelines.config import recipe_base

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
        """ Adds a recipe JSON dictionary to the recipe list.

        Parameters
        ----------
        file : str
            The name of the JSON to load.
        proc : str
            The process pipeline.
        """

        # @TODO use 'with' statement for file read
        testjson = json.loads(open(file).read(), object_pairs_hook=OrderedDict)

        for IP in testjson[proc]['recipe']:
            process = str(IP)
            processDict = {}
            processDict[process] = OrderedDict()
            for k, v in testjson[proc]['recipe'][process].items():
                processDict[process][str(k)] = str(v)

            self.recipe.append(processDict)

    def addMissionJson(self, mission, proc):
        """ Adds a recipe JSON for a specific mission.

        Parameters
        ----------
        mission : str
            The name of the mission.
        proc : str
            The process pipeline.
        """

        recipe_json = recipe_base + "/" + mission + '.json'
        self.AddJsonFile(recipe_json, proc)

    def getRecipe(self):
        """ Returns list of recipe dictionaries.

        Returns
        -------
        list
            Recipe dictionaries.
        """
        return self.recipe

    def getProcesses(self):
        """ Returns list of process names.

        Returns
        -------
        list
            List of process names.
        """

        processList = []
        for Tkey in self.recipe:
            for key, value in Tkey.items():
                processList.append(key)

        return processList

    def AddProcess(self, process):
        """ Adds process to recipe.

        Parameters
        ----------
        process : str
            The name of the process pipeline.
        """
        self.recipe.append(process)
