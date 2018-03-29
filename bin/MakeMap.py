#!/usgs/apps/anaconda/bin/python

import pvl
import json
from collections import OrderedDict


class MakeMap(object):

    def __init__(self):

        self.mapDICT = {}
        self.mapDICT = OrderedDict()
        self.mapDICT['Group'] = 'Mapping'

    def Projection(self, proj):
        self.mapDICT['ProjectionName'] = proj

    def CLat(self, clat):
        self.mapDICT['CenterLatitude'] = clat

    def CLon(self, clon):
        self.mapDICT['CenterLongitude'] = clon

    def FirstParallel(self, firstParallel):
        self.mapDICT['FirstStandardParallel'] = firstParallel

    def SecondParallel(self, secondParallel):
        self.mapDICT['SecondStandardParallel'] = secondParallel

    def Target(self, target):
        self.mapDICT['TargetName'] = target

    def ERadius(self, ER):
        self.mapDICT['EquatorialRadius'] = ER

    def PRadius(self, PR):
        self.mapDICT['PolarRadius'] = PR

    def LatType(self, lattype):
        self.mapDICT['LatitudeType'] = lattype

    def LonDirection(self, londir):
        self.mapDICT['LongitudeDirection'] = londir

    def LonDomain(self, londom):
        self.mapDICT['LongitudeDomain'] = londom

    def MinLat(self, item):
        self.mapDICT['MinimumLatitude'] = item

    def MaxLat(self, item):
        self.mapDICT['MaximumLatitude'] = item

    def MinLon(self, item):
        self.mapDICT['MinimumLongitude'] = item

    def MaxLon(self, item):
        self.mapDICT['MaximumLongitude'] = item

    def PixelRes(self, res):
        self.mapDICT['PixelResolution'] = res

    def Map2pvl(self):

        self.mapDICT['End_Group'] = 'Mapping'

        mappvl = pvl.dumps(self.mapDICT)
        self.mappvl = mappvl

        return mappvl

    def Map2JSON(self):

        JSONout = json.dumps(self.mapDICT)
        return JSONout

    def Map2File(self, filename):

        self.mapDICT['End_Group'] = 'Mapping'
        tempPVL = pvl.dumps(self.mapDICT)

        file = open(filename, 'w')
        file.write(tempPVL)
        file.close()
