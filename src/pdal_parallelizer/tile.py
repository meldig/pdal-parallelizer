"""
Tile class.

A tile is composed of :
- Its path (filepath)
- A directory to store the results files (output_dir)
- A pdal pipeline (json_pipeline)
- A name (optional) (name)
- Its limits (optional) (bounds)
"""

import sys
import pdal
import json
import os
from . import copc
from . import bounds


class Tile:
    def __init__(self, filepath, output_dir, json_pipeline, name=None, bounds=None, buffer=None, remove_buffer=False):
        self.filepath = filepath
        self.output_dir = output_dir
        self.json_pipeline = json_pipeline

        if name:
            self.name = name
        else:
            self.name = os.path.basename(self.filepath).split('.')[0]

        self.buffer = buffer
        self.remove_buffer = remove_buffer

        if self.buffer:
            self.bounds, self.assign = bounds.buffer(self.buffer)
        else:
            self.bounds = bounds

        if self.bounds:
            self.copc = copc.COPC(filepath, bounds)

    def getName(self):
        return self.name

    def pipeline(self, copc=False):
        """Assign a pipeline to the tile"""
        output_dir = self.output_dir

        # Open the pipeline
        with open(self.json_pipeline, 'r') as pipeline:
            p = json.load(pipeline)

            # If there is a buffer
            if self.buffer:
                # Assign the class 113 to it by adding an assign step to the pipeline
                p.insert(1, self.assign)
                # If the user wants to remove the buffer, it is removed by adding a range step to the pipeline
                if self.remove_buffer:
                    p.insert(len(p) - 1, bounds.removeBuffer())

            print(p)

            # Create the name of the temp file associated to the pipeline
            temp_name = 'temp__' + self.getName()
            output_filename = f'{output_dir}/{self.getName()}'
            # Get the reader and the writer
            reader = list(filter(lambda x: x['type'].startswith('reader'), p))
            writer = list(filter(lambda x: x['type'].startswith('writers'), p))
            # Get the extension for the output
            extension = '.' + writer[0]['type'].split('.')[1] + '.las' if writer[0]['type'].split('.')[1] == 'copc' else '.' + writer[0]['type'].split('.')[1]

            # The pipeline must contains a reader AND a writer
            if not reader:
                sys.exit("Please add a reader to your pipeline.")
            elif not writer:
                sys.exit("Please add a writer to your pipeline.")

            # If it's a copc, bounds are added to divide the copc in small tiles
            if copc:
                reader[0]['bounds'] = str(self.bounds)

            # Add the filename option in the pipeline's reader to get the right file
            reader[0]['filename'] = self.filepath
            # Add the filename option in the pipeline's write to write the result in the right file
            writer[0]['filename'] = output_filename + extension

            p = pdal.Pipeline(json.dumps(p))

        return p, temp_name

    def split(self, distTileX, distTileY, nTiles=None):
        """Split the tile in small parts of given sizes"""
        current_minx = self.bounds.minx
        current_maxx = current_minx + distTileX
        current_miny = self.bounds.miny
        current_maxy = current_miny + distTileY
        # If it's a dry run, 'cpt' will count the number of tiles created
        cpt = 0

        while current_maxx < self.bounds.maxx and current_maxy < self.bounds.maxy and (cpt < nTiles if nTiles else True):
            # Create the bounds for the small tile
            b = bounds.Bounds(current_minx, current_miny, current_maxx, current_maxy, self.bounds.resolution)
            # Create it's name (minx_miny)
            name = str(int(b.minx)) + '_' + str(int(b.miny))
            # Create the tile
            t = Tile(filepath=self.filepath, output_dir=self.output_dir, json_pipeline=self.json_pipeline, name=name, bounds=b, buffer=self.buffer, remove_buffer=self.remove_buffer)
            # Add the width given by the user to shift right to create a new tile
            current_minx += distTileX
            current_maxx += distTileX

            # If the current maxx value exceeds the right edge of the copc
            if current_maxx >= self.copc.bounds.maxx:
                # Return to the left edge to create new tiles
                current_minx = self.bounds.minx
                current_maxx = current_minx + distTileX
                # Move down from the height value given by the user
                current_miny += distTileY
                current_maxy += distTileY

            if t.pipeline(True)[0].quickinfo['readers.copc']['num_points'] != 0:
                cpt += 1
                yield t
            else:
                print('There is no points in this tile, skipped.')

    def __str__(self):
        if self.bounds:
            return f'{self.bounds} - {self.filepath}'
        else:
            return f'{self.filepath}'
