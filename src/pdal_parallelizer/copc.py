"""
COPC class.

A COPC is composed of :
- Its path (filepath)
- Its limits (bounds)
"""

import pdal
from pyproj import CRS
from bounds import Bounds


class COPC:
    def __init__(self, filepath, bounds=None):
        self.filepath = filepath
        self.info = self.compute_quickinfo(bounds)

        # Get the copc information to set its bounds
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
        # Create bounds for the copc
        self.bounds = Bounds(minx, miny, maxx, maxy, self.info['resolution'], srs)

    # Get the number of points in the copc
    def getCount(self):
        return self.info['num_points']
    count = property(getCount)

    def reader(self):
        """Returns the reader of the copc to get more information about it in the above function"""
        reader = pdal.Reader.copc(self.filepath)
        return reader

    def compute_quickinfo(self, bounds):
        """Returns some information about the copc."""
        if not bounds:
            resolution = 20000
        else:
            resolution = bounds.resolution

        reader = self.reader()
        # Get the copc information
        info = reader.pipeline().quickinfo['readers.copc']
        info['resolution'] = resolution

        return info

    def __str__(self):
        return f'COPC - {self.bounds}'
