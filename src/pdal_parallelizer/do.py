"""
Do file.

Responsible for all the executions, serializations or creations of object we need to process the pipelines.
"""

import dask
from dask.distributed import Lock
import tile
import cloud
import bounds
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
        if pipeline[0].streamable:
            pipeline[0].execute_streaming()
        else:
            pipeline[0].execute()

        del pipeline
        try:
            # Remove the temp file
            os.remove(temp_file)
        except FileNotFoundError:
            pass
    # Don't need to get and suppress the temp file if it's a dry run
    else:
        if pipeline[0].streamable:
            pipeline[0].execute_streaming()
        else:
            pipeline[0].execute()

        del pipeline


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


def process_pipelines(output_dir, json_pipeline, iterator, temp_dir=None, dry_run=False, is_single=False):
    delayedPipelines = []
    while True:
        try:
            # If it's a cloud, 'next(iterator)' is a tile. Else, 'next(iterator)' is a filepath so a tile must be
            # created
            t = next(iterator) if is_single else tile.Tile(next(iterator), output_dir, json_pipeline)
            p = t.pipeline(is_single)
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


def splitCloud(filepath, output_dir, json_pipeline, tile_bounds, nTiles=None, buffer=None, remove_buffer=False, bounding_box=None):
    """Split the cloud in many tiles"""
    bds = bounds.Bounds(bounding_box[0], bounding_box[1], bounding_box[2], bounding_box[3]) if bounding_box else None
    # Create a cloud object
    c = cloud.Cloud(filepath, bounds=bds)
    # Create a tile the size of the cloud
    t = tile.Tile(filepath=c.filepath, output_dir=output_dir, json_pipeline=json_pipeline, bounds=c.bounds, buffer=buffer, remove_buffer=remove_buffer, cloud_object=c)
    # Split the tile in small parts of given sizes
    return t.split(tile_bounds[0], tile_bounds[1], nTiles)


def serializePipeline(pipeline, temp_dir):
    """Serialize the pipelines"""
    # Create the temp file path
    temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
    with open(temp_file, 'wb') as outfile:
        # Serialize the pipeline
        pickle.dump(pipeline, outfile)
