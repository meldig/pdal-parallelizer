"""
Bounds class.

A bound is composed of :
- Its minimal x-value (minx)
- Its minimal y-value (miny)
- Its maximal x-value (maxx)
- Its maximal y-value (maxy)
- A srs (optional)
"""

import json


def removeBuffer(bounds):
    crop = '{"type": "filters.crop"}'
    parsed = json.loads(crop)
    parsed['bounds'] = str(bounds)
    return parsed


class Bounds:
    def __init__(self, minx, miny, maxx, maxy, srs=None):
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy
        self.srs = srs
        self.distX = self.maxx - self.minx
        self.distY = self.maxy - self.miny

    def getDistX(self):
        return self.distX

    def getDistY(self):
        return self.distY

    def buffer(self, buffer):
        if buffer < 0:
            minx = self.minx + buffer
            miny = self.miny + buffer
            maxx = self.maxx - buffer
            maxy = self.maxy - buffer
        else:
            minx = self.minx - buffer
            miny = self.miny - buffer
            maxx = self.maxx + buffer
            maxy = self.maxy + buffer

        return self, Bounds(minx, miny, maxx, maxy, self.srs)

    def __str__(self):
        return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"
