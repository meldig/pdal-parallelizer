"""
This is the tile class. Each file represent a tile, and for each tile we assign a pipeline written by the user
"""

import sys
import pdal
import json
import os
import copc
import bounds
import uuid


class Tile:
    def __init__(self, filepath, output_dir, json_pipeline, bounds=None):
        self.filepath = filepath
        self.output_dir = output_dir
        self.json_pipeline = json_pipeline
        self.bounds = bounds
        self.copc = copc.COPC(filepath, bounds)

    def pipeline(self, copc=None):
        output_dir = self.output_dir

        with open(self.json_pipeline, 'r') as pipeline:
            p = json.load(pipeline)
            if not copc:
                filename = os.path.basename(self.filepath).split('.')[0]
                reader = list(filter(lambda x: x['type'] == 'readers.las', p))
            else:
                filename = str(uuid.uuid4())
                reader = list(filter(lambda x: x['type'] == 'readers.copc', p))
                reader[0]['bounds'] = str(self.bounds)

            name = 'temp__' + filename
            output_filename = f'{output_dir}/{filename}.las'
            writer = list(filter(lambda x: x['type'] == 'writers.las', p))

            if not reader:
                sys.exit("Please add a reader to your pipeline.")
            elif not writer:
                sys.exit("Please add a writer to your pipeline.")

            reader[0]['filename'] = self.filepath
            writer[0]['filename'] = output_filename

            p = pdal.Pipeline(json.dumps(p))

        return p, name

    def split(self, distTileX, distTileY):
        current_minx = self.bounds.minx
        current_maxx = current_minx + distTileX
        current_miny = self.bounds.miny
        current_maxy = current_miny + distTileY

        while current_maxx < self.bounds.maxx and current_maxy < self.bounds.maxy:
            b = bounds.Bounds(current_minx, current_miny, current_maxx, current_maxy, self.bounds.resolution)
            t = Tile(self.filepath, self.output_dir, self.json_pipeline, b)
            current_minx += distTileX
            current_maxx += distTileX

            if current_maxx >= self.bounds.maxx:
                current_minx = self.bounds.minx
                current_maxx = current_minx + distTileX
                current_miny += distTileY
                current_maxy += distTileY

            yield t

    def __str__(self):
        return f'{self.bounds} - {self.filepath}'


