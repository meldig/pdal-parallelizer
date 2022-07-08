import pdal
from pyproj import CRS
from bounds import Bounds


class COPC:
    def __init__(self, filepath, bounds=None):
        self.filepath = filepath
        self.info = self.compute_quickinfo(bounds)

        bounds_dict = self.info['bounds']
        minx, miny, = (
            bounds_dict['minx'],
            bounds_dict['miny']
        )

        maxx, maxy = (
            bounds_dict['maxx'],
            bounds_dict['maxy']
        )

        srs = CRS(self.info['srs']['compoundwkt'])
        self.bounds = Bounds(minx, miny, maxx, maxy, self.info['resolution'], srs)

    def getCount(self):
        return self.info['num_points']
    count = property(getCount)

    def reader(self, bounds=None, resolution=None):
        reader = pdal.Reader.copc(self.filepath)

        if bounds:
            reader._options['bounds'] = str(bounds)
        if resolution:
            reader._options['resolution'] = resolution

        return reader

    def compute_quickinfo(self, bounds):
        if not bounds:
            resolution = 20000
        else:
            resolution = bounds.resolution

        reader = self.reader(bounds, resolution)
        info = reader.pipeline().quickinfo['readers.copc']
        info['resolution'] = resolution

        return info

    def __str__(self):
        return f'COPC - {self.bounds}'