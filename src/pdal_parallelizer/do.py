"""
This is the do file. Here, we create the tiles in the createTiles function and then we execute their pipelines in the process function
"""
import types
import uuid
import dask
from dask.distributed import Lock
import tile
import copc
import pickle
import os


@dask.delayed
def process(pipeline, temp_dir=None, copc=False):
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


def createTiles(output_dir, json_pipeline, temp_dir=None, file_iterator=None, pipeline_iterator=None, dry_run=False):
    delayedPipelines = []
    if pipeline_iterator:
        while True:
            try:
                p = next(pipeline_iterator)
            except StopIteration:
                break

            delayedPipelines.append(dask.delayed(process)(p, temp_dir))
    else:
        while True:
            try:
                f = next(file_iterator)
                t = tile.Tile(f, output_dir, json_pipeline)
                p = t.pipeline()
                if not dry_run:
                    delayedPipelines.append(serializePipeline(p, temp_dir))
                else:
                    delayedPipelines.append(dask.delayed(process)(p))
            except StopIteration:
                break

    return delayedPipelines


def splitCopc(filepath, output_dir, json_pipeline, resolution, tile_bounds):
    c = copc.COPC(filepath)
    c.bounds.resolution = resolution
    t = tile.Tile(filepath=c.filepath, output_dir=output_dir, json_pipeline=json_pipeline, bounds=c.bounds)
    return t.split(tile_bounds[0], tile_bounds[1])


def serializePipeline(pipeline, temp_dir, copc=False):
    temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
    with open(temp_file, 'wb') as outfile:
        pickle.dump(pipeline, outfile)
    return dask.delayed(process)(pipeline=pipeline, temp_dir=temp_dir, copc=copc)