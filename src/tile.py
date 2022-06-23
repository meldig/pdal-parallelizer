import sys
import uuid
import pdal
import json


class Tile:
    def __init__(self, filename, output_dir):
        self.filename = filename
        self.output_dir = output_dir

    def pipeline(self):
        name = uuid.uuid4()
        output_dir = self.output_dir
        output_filename = f'{output_dir}/{name}.las'

        with open("pipeline.json", 'r') as pipeline:
            p = json.load(pipeline)
            reader = list(filter(lambda x: x['type'] == 'readers.las', p))
            writer = list(filter(lambda x: x['type'] == 'writers.las', p))

            if not reader:
                sys.exit("Please add a reader to your pipeline.")
            elif not writer:
                sys.exit("Please add a writer to your pipeline.")

            reader[0]['filename'] = self.filename
            writer[0]['filename'] = output_filename

            p = pdal.Pipeline(json.dumps(p))

        return p, name

    def __str__(self):
        return f'{self.filename}'
