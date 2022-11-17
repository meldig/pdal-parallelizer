"""
Main file.

Contains the process-pipelines function you call in command line
"""

import json
import sys
import click
import dask
from dask import config as cfg
from dask.distributed import LocalCluster, Client, progress
from distributed.diagnostics import MemorySampler
from os import listdir
from . import do
from . import file_manager
from matplotlib import pyplot as plt
import gc


@click.group()
@click.version_option('1.10.19')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


def config_dask(n_workers, threads_per_worker):
    """Make some configuration to avoid workers errors due to heartbeat or timeout problems. Set the number of cores
    to process the pipelines """
    timeout = input('After how long of inactivity do you want to kill your worker (timeout)\n')

    cfg.set({'interface': 'lo'})
    cfg.set({'distributed.scheduler.worker-ttl': None})
    cfg.set({'distributed.comm.timeouts.connect': timeout})
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker)
    client = Client(cluster)
    return client


@main.command()
@click.option('-c', '--config', required=True, type=click.Path(exists=True))
@click.option('-nw', '--n_workers', required=False, type=int, default=3)
@click.option('-tpw', '--threads_per_worker', required=False, type=int, default=1)
@click.option('-dr', '--dry_run', required=False, type=int)
@click.option('-d', '--diagnostic', is_flag=True, required=False)
@click.option('-it', '--input_type', required=True, type=click.Choice(['single', 'dir']))
@click.option('-ts', '--tile_size', required=False, nargs=2, type=int, default=(256, 256))
@click.option('-b', '--buffer', required=False, type=int)
@click.option('-rb', '--remove_buffer', is_flag=True, required=False)
@click.option('-bb', '--bounding_box', required=False, nargs=4, type=float)
def process_pipelines(**kwargs):
    """Processing pipelines on many points cloud in parallel"""
    with open(kwargs.get('config'), 'r') as c:
        config = json.load(c)
        input = config.get('input')
        output = config.get('output')
        temp = config.get('temp')
        pipeline = config.get('pipeline')

    # Get all the options
    n_workers = kwargs.get('n_workers')
    threads_per_worker = kwargs.get('threads_per_worker')
    dry_run = kwargs.get('dry_run')
    diagnostic = kwargs.get('diagnostic')
    input_type = kwargs.get('input_type')
    tile_size = kwargs.get('tile_size')
    buffer = kwargs.get('buffer')
    remove_buffer = kwargs.get('remove_buffer')
    bounding_box = kwargs.get('bounding_box')

    # If there is some temp file in the temp directory, these are processed
    if len(listdir(temp)) != 0:
        click.echo(
            'Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution\n')
        # Get all the deserialized pipelines
        pipeline_iterator = file_manager.getSerializedPipelines(temp_directory=temp)
        # Process pipelines
        delayed = do.process_serialized_pipelines(temp_dir=temp, iterator=pipeline_iterator)
    else:
        click.echo('Beginning of the execution\n')
        # If the user don't specify the dry_run option
        if not dry_run:
            # If the user wants to process a single file, it is split. Else, get all the files of the input directory
            iterator = do.splitCloud(filepath=input,
                                     output_dir=output,
                                     json_pipeline=pipeline,
                                     tile_bounds=tile_size,
                                     buffer=buffer,
                                     remove_buffer=remove_buffer,
                                     bounding_box=bounding_box) if input_type == 'single' \
                else file_manager.getFiles(input_directory=input)
            # Process pipelines
            delayed = do.process_pipelines(output_dir=output, json_pipeline=pipeline, temp_dir=temp, iterator=iterator,
                                           is_single=(input_type == 'single'))
        else:
            # If the user wants to process a single file, it is split and get the number of tiles given by the user.
            # Else, get the number of files we want to do the test execution (not serialized)
            iterator = do.splitCloud(filepath=input,
                                     output_dir=output,
                                     json_pipeline=pipeline,
                                     tile_bounds=tile_size,
                                     nTiles=dry_run,
                                     buffer=buffer,
                                     remove_buffer=remove_buffer,
                                     bounding_box=bounding_box) if input_type == 'single' \
                else file_manager.getFiles(input_directory=input, nFiles=dry_run)
            # Process pipelines
            delayed = do.process_pipelines(output_dir=output, json_pipeline=pipeline, iterator=iterator,
                                           dry_run=dry_run, is_single=(input_type == 'single'))

    client = config_dask(n_workers=n_workers, threads_per_worker=threads_per_worker)

    click.echo('Parallelization started.\n')
    # compute_and_graph(client=client, tasks=delayed, output_dir=output, diagnostic=diagnostic)

    if diagnostic:
        ms = MemorySampler()
        with ms.sample(label='execution', client=client):
            delayed = client.persist(delayed)
            progress(delayed)
            futures = client.compute(delayed)
            client.gather(futures)
        ms.plot()
        plt.savefig(output + '/memory-usage.png')
    else:
        delayed = client.persist(delayed)
        progress(delayed)
        futures = client.compute(delayed)
        client.gather(futures)

    # At the end, collect the unmanaged memory for all the workers
    client.run(gc.collect)

    file_manager.getEmptyWeight(output_directory=output)


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal_parallelizer")
    main()
