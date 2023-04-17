import os
import dask
import pickle
import numpy as np
from cloud import Cloud
from tile import Tile


@dask.delayed
def execute_stages_standard(stages):
    readers_stage = stages.pop(0)
    readers = readers_stage.pipeline()
    readers.execute()
    arr = readers.arrays[0]

    for stage in stages:
        pipeline = stage.pipeline(arr)
        pipeline.execute()
        if len(pipeline.arrays[0]) > 0:
            arr = pipeline.arrays[0]

    return arr


def execute_stages_streaming(array, stages, tile_name, temp, dry_run=None):
    for stage in stages:
        pipeline = stage.pipeline(array)
        pipeline.execute()
        array = pipeline.arrays[0]

    if not dry_run:
        os.remove(temp + "/" + tile_name + ".pickle")

    # filters = stages.pop(0).pipeline(array)
    # iterator = filters.iterator()
    # arrays = []
    #
    # for arr in iterator:
    #     for stage in stages:
    #         pipeline = stage.pipeline(arr)
    #         pipeline.execute()
    #         arr = pipeline.arrays[0]
    #     arrays.append(arr)
    #
    # return arrays


@dask.delayed
def write_cloud(array, writers, name=None, temp=None, dry_run=None):
    writers.pipeline(array).execute_streaming()
    if not dry_run:
        os.remove(temp + '/' + name + ".pickle")


def process_serialized_tiles(serialized_data, temp):
    delayed_tasks = []

    for tile in serialized_data:
        stages = tile.stages
        writers = stages.pop()
        array = execute_stages_standard(stages)
        result = write_cloud(array, writers, tile.name, temp, None)

        delayed_tasks.append(result)

    return delayed_tasks


def process_several_clouds(files, pipeline, output, temp, buffer=None, remove_buffer=None, dry_run=None):
    delayed_tasks = []

    for file in files:
        c = Cloud(file)
        t = Tile(c, c.bounds, pipeline, output, buffer, remove_buffer, os.path.basename(c.filepath).split(".")[0])
        p = t.link_pipeline(False)

        if not dry_run:
            serialize(t, temp)

        stages = p.stages
        writers = stages.pop()
        array = execute_stages_standard(stages)
        result = write_cloud(array, writers, t.name, temp, dry_run)
        delayed_tasks.append(result)

    return delayed_tasks


def cut_image_array(tiles, image_array, temp, dry_run=None):
    results = []

    for tile in tiles:
        p = tile.link_pipeline(True)

        array = image_array[np.where((image_array["X"] > tile.bounds.min_x) &
                                     (image_array["X"] < tile.bounds.max_x) &
                                     (image_array["Y"] > tile.bounds.min_y) &
                                     (image_array["Y"] < tile.bounds.max_y))]

        stages = p.stages

        if not dry_run:
            serialize(tile, temp)

        stages.pop(0)
        results.append((array, stages, tile.name))

    return results


def serialize(tile, temp):
    temp_file = temp + '/' + tile.name + ".pickle"
    with open(temp_file, 'wb') as outfile:
        # Serialize the pipeline
        pickle.dump(tile, outfile)
