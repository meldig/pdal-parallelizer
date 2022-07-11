"""
Do file.

Responsible of all the executions, serializations or creations of object we need to process the pipelines.
"""

import dask
from dask.distributed import Lock
import tile
import copc
import pickle
import os


@dask.delayed
def process(pipeline, temp_dir=None):
    """Process pipeline and delete the associate temp file if it's not a dry run"""
    if temp_dir:
        with Lock(str(pipeline[1])):
            # Get the temp file associated with the pipeline
            temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
        # Execute the pipeline
        pipeline[0].execute()
        try:
            # Remove the temp file
            os.remove(temp_file)
        except FileNotFoundError:
            print('Trying to suppress ' + temp_dir + temp_file + ' but cannot found this file.')
    # Don't need to get and suppress the temp file if it's a dry run
    else:
        pipeline[0].execute()


def process_serialized_pipelines(temp_dir, iterator):
    """Create the array of delayed tasks for pipelines in the temp directory (if there is some)"""
    delayedPipelines = []
    # yield loop syntax
    while True:
        try:
            p = next(iterator)
        except StopIteration:
            break

        # Add the delayed task to the array
        delayedPipelines.append(dask.delayed(process)(p, temp_dir))

    return delayedPipelines


def process_pipelines(output_dir, json_pipeline, iterator, temp_dir=None, dry_run=False, copc=False):
    """
    Create the array of delayed tasks for pipelines in the input directory if it's not a copc.
    If it's a copc, create the array of delayed tasks of each tile's pipeline
    """
    delayedPipelines = []
    while True:
        try:
            # If it's a copc, 'next(iterator)' is a tile. Else, 'next(iterator)' is a filepath so a tile must be created
            t = next(iterator) if copc else tile.Tile(next(iterator), output_dir, json_pipeline)
            p = t.pipeline(copc)
            # If it's not a dry run, the pipeline must be serialized
            if not dry_run:
                serializePipeline(p, temp_dir)
                delayedPipelines.append(dask.delayed(process)(p, temp_dir))
            # If it's a dry run, the pipeline is not serialized
            else:
                delayedPipelines.append(dask.delayed(process)(p))
        except StopIteration:
            break

    return delayedPipelines


def splitCopc(filepath, output_dir, json_pipeline, resolution, tile_bounds, nTiles=None):
    """Split the copc in many tiles"""
    # Create a copc object
    c = copc.COPC(filepath)
    # Set its bounds resolution
    c.bounds.resolution = resolution
    # Create a tile the size of the copc
    t = tile.Tile(filepath=c.filepath, output_dir=output_dir, json_pipeline=json_pipeline, bounds=c.bounds)
    # Split the tile in small parts of given sizes
    return t.split(tile_bounds[0], tile_bounds[1], nTiles)


def serializePipeline(pipeline, temp_dir):
    """Serialize the pipelines"""
    # Create the temp file path
    temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
    with open(temp_file, 'wb') as outfile:
        # Serialize the pipeline
        pickle.dump(pipeline, outfile)