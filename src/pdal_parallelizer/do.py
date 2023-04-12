import os
import dask
import pickle
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


@dask.delayed
def execute_stages_streaming(array, stages):
    filters = stages.pop(0).pipeline(array)
    iterator = filters.iterator()
    arrays = []

    for arr in iterator:
        for stage in stages:
            pipeline = stage.pipeline(arr)
            pipeline.execute()
            arr = pipeline.arrays[0]
        arrays.append(arr)

    return arrays


@dask.delayed
def write_cloud(array, writers, name=None, temp=None):
    writers.pipeline(array).execute_streaming()
    if temp:
        os.remove(temp + '/' + name + ".pickle")


def process_serialized_stages(serialized_data, temp):
    delayed_tasks = []

    for (stages, temp_file) in serialized_data:
        writers = stages.pop()
        array = execute_stages_standard(stages)
        result = write_cloud(array, writers, temp_file, temp)

        delayed_tasks.append(result)

    return delayed_tasks


def process_several_clouds(files, pipeline, output, temp, buffer=None, remove_buffer=None, dry_run=None):
    delayed_tasks = []

    for file in files:
        c = Cloud(file)
        t = Tile(c, c.bounds, pipeline, output, buffer, remove_buffer, os.path.basename(c.filepath).split(".")[0])
        p = t.link_pipeline(False)

        stages = p.stages

        if not dry_run:
            serialize(stages, t.name, temp)

        writers = stages.pop()
        array = execute_stages_standard(stages)
        result = write_cloud(array, writers, t.name, temp)
        delayed_tasks.append(result)

    return delayed_tasks


def process_single_cloud(tiles, image_array, temp, dry_run=None):
    delayed_tasks = []

    for tile in tiles:
        p = tile.link_pipeline(True)

        stages = p.stages
        stages.pop(0)

        if not dry_run:
            serialize(p, temp)

        result = execute_stages_streaming(image_array, stages)
        delayed_tasks.append(result)

    return delayed_tasks


def serialize(stages, tile_name, temp):
    temp_file = temp + '/' + tile_name + ".pickle"
    with open(temp_file, 'wb') as outfile:
        # Serialize the pipeline
        pickle.dump((stages, tile_name), outfile)
