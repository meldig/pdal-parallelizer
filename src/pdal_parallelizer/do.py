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


def createTiles(output_dir, json_pipeline, temp_dir=None, files=None, pipelines=None, dry_run=False):
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


def splitCopc(filepath, output_dir, json_pipeline, resolution, bounds):
    c = copc.COPC(filepath)
    c.bounds.resolution = resolution
    t = tile.Tile(c.filepath, output_dir, json_pipeline, c.bounds)
    return t.split(bounds[0], bounds[1])


def serializeTiles(iterator, temp_dir):
    delayedTiles = []
    while True:
        try:
            tile = next(iterator)
            temp_file = temp_dir + '/' + str(uuid.uuid4()) + '.pickle'
            with open(temp_file, 'wb') as outfile:
                pickle.dump(tile, outfile)
            delayedTiles.append(dask.delayed(process)(tile.pipeline(True)))
        except StopIteration:
            break

    return delayedTiles

"""
def doBatch(seq):
    for x in seq:
        x[0].execute()


def unpack(g, batch, batchsize, batches):
    while True:
        try:
            k = next(g)
            if isinstance(k, types.GeneratorType):
                unpack(k, batch, batchsize, batches)
            if isinstance(k, tile.Tile):
                p = k.pipeline(True)
                batch.append(p)
                break
        except StopIteration:
            if len(batch):
                d = dask.delayed(doBatch)(batch)
                batches.append(d)
                batch = []
            return batches
            break
"""