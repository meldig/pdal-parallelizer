import json
import logging
import os.path
import sys
from os.path import join
import numpy as np
import matplotlib.pyplot as plt
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


def trigger_warnings(n_workers, input_type, input, output, temp, tile_size):
    if n_workers >= os.cpu_count():
        answer = query_yes_no(
            f'\nWARNING - You choose to launch {n_workers} workers but your machine has only {os.cpu_count()}'
            f' CPUs, please reduce the number of workers.\nDo you want to continue ?'
        )
        if not answer:
            return

    if input_type == 'single':
        c = Cloud(input)
        if tile_size == (256, 256):
            answer = query_yes_no(
                f'WARNING - You are using the default value of the tile_size option (256 by 256 meters). Please '
                f'check if your points cloud\'s dimensions are greater than this value.\nDo you want to continue ? '
            )
            if not answer:
                return

    if input_type == 'dir':
        if input == output or input == temp:
            answer = query_yes_no(
                f'WARNING - Your input folder is the same as your output or temp folder. This could be a problem. '
                f'Please choose three separate directories.\n Do you want to continue ? '
            )
            if not answer:
                return

    if len(os.listdir(output)) > 0:
        answer = query_yes_no(
            f'WARNING - Your output directory is not empty.\n Do you want to continue ? '
        )
        if not answer:
            return


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

    trigger_warnings(n_workers, input_type, input, output, temp, tile_size)

    if not os.path.exists(temp):
        os.mkdir(temp)

    if not os.path.exists(output):
        os.mkdir(output)

    client = config_dask(n_workers, threads_per_worker, timeout)
    ms = MemorySampler()

    if input_type == "single":
        futures = []
        c = Cloud(input, bounding_box)
        if len(os.listdir(temp)) != 0:
            print("Something went wrong during previous execution, there is some temp files in your temp " +
                  "directory.\nBeginning of the execution\n")
            tiles = file_manager.get_serialized_tiles(temp)
        else:
            tiles = c.split(tile_size, pipeline, output, buffer, dry_run)

        print("Opening the cloud.\n")
        image_array = c.load_image_array(pipeline)

        if bounding_box:
            image_array = image_array[np.where((image_array["X"] > bounding_box[0]) &
                                               (image_array["X"] < bounding_box[2]) &
                                               (image_array["Y"] > bounding_box[1]) &
                                               (image_array["Y"] < bounding_box[3]))]

        data = do.cut_image_array(tiles, image_array, temp, dry_run)

        print(f"Starting parallelization, visit the dashboard ({client.dashboard_link}) to follow the execution.\n")

        with ms.sample("Execution"):
            for (array, stages, tile) in data:
                if len(array) > 0:
                    big_future = client.scatter(array)
                    futures.append(
                        client.submit(do.execute_stages_streaming, big_future, stages, tile, temp, remove_buffer, dry_run))
                else:
                    if not dry_run:
                        os.remove(temp + "/" + tile.name + ".pickle")

            client.gather(futures)

        if merge_tiles:
            c.merge(output, pipeline)

        if remove_tiles:
            for f in os.listdir(output):
                if f != os.path.basename(c.filepath) and f.split(".")[1] != "png":
                    os.remove(join(output, f))
    else:
        if len(os.listdir(temp)) != 0:
            print("Something went wrong during previous execution, there is some temp files in your temp " +
                  "directory.\nBeginning of the execution\n")
            serialized_data = file_manager.get_serialized_tiles(temp)
            tasks = do.process_serialized_tiles(serialized_data, temp)
        else:
            print("Beginning of the execution.\n")
            files = file_manager.get_files(input, dry_run)
            tasks = do.process_several_clouds(files, pipeline, output, temp, buffer, dry_run)

        print("Starting parallelization.\n")

        with ms.sample("Execution"):
            future = client.persist(tasks)
            progress(future)

    if diagnostic:
        ms.plot()
        plt.savefig(output + "/diagnostic.png")


if __name__ == '__main__':
    process_pipelines(
        config="D:\\data_dev\\pdal-parallelizer\\config.json",
        input_type="single",
        tile_size=(35, 35),
        timeout=500,
        n_workers=6,
        buffer=2,
        remove_buffer=True,
        diagnostic=True
    )
