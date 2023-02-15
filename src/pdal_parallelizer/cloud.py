"""
Cloud class.

A Cloud is composed of :
- Its path (filepath)
- Its limits (bounds)
"""

import subprocess
import json
from bounds import Bounds
from os import listdir


def crop(bounds):
    crop = '{"type": "filters.crop"}'
    parsed = json.loads(crop)
    parsed['bounds'] = str(bounds)
    return parsed


def merge(output_dir, filename, writers):
    outputs = ""
    # Default values according to the pdal writers.las documentation
    compression = 'none'
    minor_version = 2
    dataformat_id = 3

    for f in listdir(output_dir):
        if f.split('.').count('png') <= 0:
            outputs += '"' + output_dir + '/' + f + '",'

    if outputs != "":
        extension = listdir(output_dir)[0].split('.')[1]
        if extension == 'laz':
            writers_extension = 'las'
            compression = 'laszip'
        else:
            writers_extension = extension

        try:
            minor_version = writers[0]['minor_version']
            dataformat_id = writers[0]['dataformat_id']
        except KeyError:
            pass

        merge = '[' + outputs + '{"type": "writers.' + writers_extension + '", "filename":"' + output_dir + '/' + \
                filename + '.' + extension + '","extra_dims": "all", "compression": "' + compression + '", ' + \
                '"minor_version": ' + str(minor_version) + ', "dataformat_id": ' + str(dataformat_id) + '}]'

        return merge


def addClassFlags():
    ferry = '{"type": "filters.ferry", "dimensions": "=>ClassFlags"}'
    return json.loads(ferry)


def compute_quickinfo(filepath):
    """Returns some information about the cloud."""
    # Get the cloud information
    pdal_info = subprocess.run(['pdal', 'info', filepath, '--summary'],
                               stderr=subprocess.PIPE,
                               stdout=subprocess.PIPE)
    info = json.loads(pdal_info.stdout.decode())

    return info


class Cloud:
    def __init__(self, filepath, bounds=None):
        self.filepath = filepath
        self.info = compute_quickinfo(self.filepath)
        self.classFlags = self.hasClassFlags()

        # Get the cloud information to set its bounds
        if bounds:
            minx, miny, maxx, maxy = bounds.minx, bounds.miny, bounds.maxx, bounds.maxy
        else:
            bounds_dict = self.info['summary']['bounds']
            minx, miny, = (
                bounds_dict['minx'],
                bounds_dict['miny']
            )

            maxx, maxy = (
                bounds_dict['maxx'],
                bounds_dict['maxy']
            )

        # Create bounds for the cloud
        self.bounds = Bounds(minx, miny, maxx, maxy)

    # Get the number of points in the cloud
    def getCount(self):
        return self.info['summary']['num_points']

    def hasClassFlags(self):
        """Check if the cloud has the ClassFlags dimension"""
        info = compute_quickinfo(self.filepath)
        dimensions = info['summary']['dimensions']
        return 'ClassFlags' in dimensions

    def __str__(self):
        return f'Cloud - {self.bounds}'
