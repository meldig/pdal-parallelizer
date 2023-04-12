import json
import logging
import os.path
import sys
import click
import dask

import file_manager
import do
from cloud import Cloud
from dask import config as cfg
from dask.distributed import LocalCluster, Client, progress
from distributed.diagnostics import MemorySampler


def query_yes_no(question, default='no'):
    valid = {'yes': True, 'y': True, 'ye': True, 'no': False, 'n': False}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError(f'Invalid default answer: {default}')

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write('Please respond with "yes" or "no" (or "y" or "n").\n')


def config_dask(n_workers, threads_per_worker, timeout):
    if not timeout:
        timeout = input('After how long of inactivity do you want to kill your worker (timeout)\n')

    cfg.set({'interface': 'lo'})
    cfg.set({'distributed.scheduler.worker-ttl': None})
    cfg.set({'distributed.comm.timeouts.connect': timeout})
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker, memory_limit=None,
                           silence_logs=logging.ERROR)
    client = Client(cluster)
    return client


def process_pipelines(
        config,
        input_type,
        timeout=None,
        n_workers=3,
        threads_per_worker=1,
        dry_run=None,
        diagnostic=None,
        tile_size=(256, 256),
        buffer=None,
        remove_buffer=None,
        bounding_box=None,
        merge_tiles=None,
        remove_tiles=None
):
    with open(config, 'r') as c:
        config_file = json.load(c)
        input = config_file.get('input')
        output = config_file.get('output')
        temp = config_file.get('temp')
        pipeline = config_file.get('pipeline')

    client = config_dask(n_workers, threads_per_worker, timeout)

    if len(os.listdir(temp)) != 0:
        print("Something went wrong during previous execution, there is some temp files in your temp " +
              "directory.\nBeginning of the execution\n")
        serialized_data = file_manager.get_serialized_data(temp)
        tasks = do.process_serialized_stages(serialized_data, temp)
    else:
        print("Beginning of the execution.\n")
        if input_type == "dir":
            files = file_manager.get_files(input, dry_run)
            tasks = do.process_several_clouds(files, pipeline, output, temp, buffer, remove_buffer, dry_run)
        else:
            c = Cloud(input, bounding_box)
            tiles = c.split(tile_size, pipeline, output, dry_run)
            image_array = c.load_image_array(pipeline)
            tasks = do.process_single_cloud(tiles, image_array, temp, dry_run)

    print("Starting parallelization.\n")

    if diagnostic:
        ms = MemorySampler()
        with ms.sample(label="execution", client=client):
            future = client.persist(tasks)
            progress(future)
        ms.plot()
    else:
        future = client.persist(tasks)
        progress(future)


if __name__ == '__main__':
    process_pipelines(
        config="D:\\data_dev\\pdal-parallelizer\\config.json",
        input_type="dir",
        timeout=500,
        n_workers=6
    )
