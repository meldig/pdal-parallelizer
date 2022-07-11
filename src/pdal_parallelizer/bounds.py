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

    def __str__(self):
        return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"
