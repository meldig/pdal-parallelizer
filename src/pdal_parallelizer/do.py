import dask
from dask.distributed import Lock
import tile
import pickle
import os


@dask.delayed
def process(pipeline, temp_dir=None):
    if temp_dir:
        with Lock(str(pipeline[1])):
            temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
        pipeline[0].execute()
        try:
            os.remove(temp_file)
        except FileNotFoundError:
            print('Trying to supress ' + temp_dir + temp_file + ' but cannot found this file.')
    else:
        pipeline[0].execute()


def processPipelines(output_dir, json_pipeline, temp_dir=None, files=None, pipelines=None, dry_run=False):
    delayedPipelines = []
    if pipelines:
        for p in pipelines:
            delayedPipelines.append(dask.delayed(process)(p, temp_dir))
    else:
        tiles = []
        for file in files:
            tiles.append(tile.Tile(file, output_dir, json_pipeline))

        for t in tiles:
            pipeline = t.pipeline()
            if not dry_run:
                temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
                with open(temp_file, 'wb') as outfile:
                    pickle.dump(pipeline, outfile)
                delayedPipelines.append(dask.delayed(process)(pipeline, temp_dir))
            else:
                delayedPipelines.append(dask.delayed(process)(pipeline))

    return delayedPipelines
