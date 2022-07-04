"""
This is the tile class. Each file represent a tile, and for each tile we assign a pipeline written by the user
"""

import sys
import pdal
import json
import os


class Tile:
    def __init__(self, filepath, output_dir, json_pipeline):
        self.filepath = filepath
        self.output_dir = output_dir
        self.json_pipeline = json_pipeline

    def pipeline(self):
        filename = os.path.basename(self.filepath).split('.')[0]
        name = 'temp__' + filename
        output_dir = self.output_dir
        output_filename = f'{output_dir}/{filename}.las'

        with open(self.json_pipeline, 'r') as pipeline:
            p = json.load(pipeline)
            reader = list(filter(lambda x: x['type'] == 'readers.las', p))
            writer = list(filter(lambda x: x['type'] == 'writers.las', p))

            if not reader:
                sys.exit("Please add a reader to your pipeline.")
            elif not writer:
                sys.exit("Please add a writer to your pipeline.")

            reader[0]['filename'] = self.filepath
            writer[0]['filename'] = output_filename

            p = pdal.Pipeline(json.dumps(p))

        return p, name

    def __str__(self):
        return f'{self.filepath}'
