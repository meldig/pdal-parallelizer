"""
Bounds class.

A bound is composed of :
- Its minimal x-value (minx)
- Its minimal y-value (miny)
- Its maximal x-value (maxx)
- Its maximal y-value (maxy)
- A resolution (resolution)
- A srs (optional)
"""

import json


def removeBuffer():
    return json.loads('{"type": "filters.range", "limits": "Classification[0:112], Classification[114:]"}')


class Bounds:
    def __init__(self, minx, miny, maxx, maxy, resolution, srs=None):
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy
        self.resolution = resolution
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

        assign = '{"type": "filters.assign"}'
        parsed = json.loads(assign)
        parsed['value'] = f'Classification=113 WHERE X > {self.maxx} || X < {self.minx} || Y > {self.maxy} || Y < {self.miny}'
        return Bounds(minx, miny, maxx, maxy, self.resolution, self.srs), parsed

    def __str__(self):
        return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"
