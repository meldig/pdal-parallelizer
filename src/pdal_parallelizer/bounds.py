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

"""
    def split(self):
        centerX = self.minx + self.distX / 2.0
        centerY = self.miny + self.distY / 2.0

        low_left = Bounds(self.minx, self.miny, centerX, centerY, self.resolution, self.srs)
        upper_left = Bounds(self.minx, centerY, centerX, self.maxy, self.resolution, self.srs)
        low_right = Bounds(centerX, self.miny, self.maxx, centerY, self.resolution, self.srs)
        upper_right = Bounds(centerX, centerY, self.maxx, self.maxy, self.resolution, self.srs)
        bounds = [low_left, upper_left, low_right, upper_right]

        output = yield from bounds
"""