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

    for arr in iterator:
        for stage in stages:
            pipeline = stage.pipeline(arr)
            pipeline.execute()
            arr = pipeline.arrays[0]


@dask.delayed
def write_cloud(array, writers, pipeline=None, temp=None):
    writers.pipeline(array).execute_streaming()
    if temp:
        os.remove(temp + '/' + pipeline.name + ".pickle")


@dask.delayed
def process_serialized_pipelines(pipelines, temp):
    delayed_tasks = []

    for pipeline in pipelines:
        if pipeline[1] is None:
            stages = pipeline[0].stages
            writers = stages.pop()
            array = execute_stages_standard(stages)
            result = write_cloud(array, writers, pipeline[0], temp)

        delayed_tasks.append(result)

    return delayed_tasks


@dask.delayed
def process_several_clouds(files, pipeline, output, temp, buffer=None, remove_buffer=None, dry_run=None):
    delayed_tasks = []

    for file in files:
        c = Cloud(file)
        t = Tile(c, c.bounds, pipeline, output, buffer, remove_buffer)
        p = t.link_pipeline(False)

        stages = p.stages
        writers = stages.pop()

        if not dry_run:
            serialize(pipeline, temp)

        array = execute_stages_standard(stages)
        result = write_cloud(array, writers, pipeline, temp)
        delayed_tasks.append(result)

    return delayed_tasks


def serialize(pipeline, temp):
    temp_file = temp + '/' + pipeline.name + ".pickle"
    with open(temp_file, 'wb') as outfile:
        # Serialize the pipeline
        pickle.dump(pipeline, outfile)
