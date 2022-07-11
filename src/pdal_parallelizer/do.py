"""
This is the do file. Here, we create the tiles in the createTiles function and then we execute their pipelines in the process function
"""
import dask
from dask.distributed import Lock
import tile
import copc
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
            print('Trying to suppress ' + temp_dir + temp_file + ' but cannot found this file.')
    else:
        pipeline[0].execute()


def process_serialized_pipelines(temp_dir, iterator):
    delayedPipelines = []
    if iterator:
        while True:
            try:
                p = next(iterator)
            except StopIteration:
                break

            delayedPipelines.append(dask.delayed(process)(p, temp_dir))

    return delayedPipelines


def process_pipelines(output_dir, json_pipeline, iterator, temp_dir=None, dry_run=False, copc=False):
    delayedPipelines = []
    while True:
        try:
            t = next(iterator) if copc else tile.Tile(next(iterator), output_dir, json_pipeline)
            p = t.pipeline(copc)
            if not dry_run:
                serializePipeline(p, temp_dir)
                delayedPipelines.append(dask.delayed(process)(p, temp_dir))
            else:
                delayedPipelines.append(dask.delayed(process)(p))
        except StopIteration:
            break

    return delayedPipelines


def splitCopc(filepath, output_dir, json_pipeline, resolution, tile_bounds, nTiles=None):
    c = copc.COPC(filepath)
    c.bounds.resolution = resolution
    t = tile.Tile(filepath=c.filepath, output_dir=output_dir, json_pipeline=json_pipeline, bounds=c.bounds)
    return t.split(tile_bounds[0], tile_bounds[1], nTiles)


def serializePipeline(pipeline, temp_dir):
    temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
    with open(temp_file, 'wb') as outfile:
        pickle.dump(pipeline, outfile)