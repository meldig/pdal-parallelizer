"""
Main file.

Contains the process-pipelines and the process-copc functions you call in command line
"""

import json
import sys
import click
import dask
from dask import config as cfg
from dask.distributed import LocalCluster, Client
from distributed.diagnostics import MemorySampler
from os import listdir
from . import do
from . import file_manager
from matplotlib import pyplot as plt


@click.group()
@click.version_option('1.7.8')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


def config_dask(n_workers, threads_per_worker):
    """Make some configuration to avoid workers errors due to heartbeat or timeout problems. Set the number of cores to process the pipelines"""

    timeout = input('After how long of inactivity do you want to kill your worker (timeout)\n')

    cfg.set({'interface': 'lo'})
    cfg.set({'distributed.scheduler.worker-ttl': None})
    cfg.set({'distributed.comm.timeouts.connect': timeout})
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker)
    client = Client(cluster)
    return client


def compute_and_graph(client, tasks, output_dir, diagnostic):
    """Compute all the pipelines and produce a memory usage graph if requested by the user"""

    if diagnostic:
        ms = MemorySampler()
        with ms.sample(label='execution', client=client):
            dask.compute(*tasks)
        ms.plot()
        plt.savefig(output_dir + '/memory-usage.png')
    else:
        dask.compute(*tasks)


@main.command()
@click.option('-c', '--config', required=True, type=click.Path(exists=True))
@click.option('-nw', '--n_workers', required=False, type=int, default=3)
@click.option('-tpw', '--threads_per_worker', required=False, type=int, default=1)
@click.option('-dr', '--dry_run', required=False, type=int)
@click.option('-d', '--diagnostic', is_flag=True, required=False)
def process_pipelines(**kwargs):
    """Processing pipelines on many points cloud in parallel"""
    with open(kwargs.get('config'), 'r') as c:
        config = json.load(c)
        output_dir = config.get('directories').get('output_dir')
        temp_dir = config.get('directories').get('temp_dir')

    dry_run = kwargs.get('dry_run')

    # If there is some temp file in the temp directory, these are processed
    if len(listdir(temp_dir)) != 0:
        click.echo('Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution\n')
        # Get all the deserialized pipelines
        pipeline_iterator = file_manager.getSerializedPipelines(temp_dir)
        # Process pipelines
        delayed = do.process_serialized_pipelines(temp_dir=temp_dir, iterator=pipeline_iterator)
    else:
        click.echo('Beginning of the execution\n')
        # If the user don't specify the dry_run option
        if not dry_run:
            # Get all the files of the input directory
            file_iterator = file_manager.getFiles(config.get('directories').get('input_dir'))
            # Process pipelines
            delayed = do.process_pipelines(output_dir=output_dir, json_pipeline=config.get('pipeline'), temp_dir=temp_dir, iterator=file_iterator)
        else:
            # Get the number of files we want to do the test execution (not serialized)
            file_iterator = file_manager.getFiles(config.get('directories').get('input_dir'), nFiles=dry_run)
            # Process pipelines
            delayed = do.process_pipelines(output_dir=output_dir, json_pipeline=config.get('pipeline'), iterator=file_iterator, dry_run=dry_run)

    client = config_dask(kwargs.get('n_workers'), kwargs.get('threads_per_worker'))

    click.echo('Parallelization started.\n')
    compute_and_graph(client, delayed, output_dir, kwargs.get('diagnostic'))

    click.echo('Job just finished.\n')


@main.command()
@click.option('-f', '--file', required=True, type=click.Path(exists=True))
@click.option('-c', '--config', required=True, type=click.Path(exists=True))
@click.option('-r', '--resolution', required=False, type=int)
@click.option('-nw', '--n_workers', required=False, type=int, default=3)
@click.option('-tpw', '--threads_per_worker', required=False, type=int, default=1)
@click.option('-ts', '--tile_size', required=False, nargs=2, type=int, default=(256, 256))
@click.option('-dr', '--dry_run', required=False, type=int)
@click.option('-d', '--diagnostic', is_flag=True, required=False)
def process_copc(**kwargs):
    """Cut a copc file in many tiles and process pipeline on all these tiles"""
    with open(kwargs.get('config'), 'r') as c:
        config = json.load(c)
        output_dir = config.get('directories').get('output_dir')
        temp_dir = config.get('directories').get('temp_dir')
        pipeline = config.get('pipeline')

    dry_run = kwargs.get('dry_run')

    # If there is some temp file in the temp directory, these are processed
    if len(listdir(temp_dir)) != 0:
        click.echo('Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution\n')
        # Get all the deserialized pipelines
        pipelines_iterator = file_manager.getSerializedPipelines(temp_dir)
        # Process pipelines
        delayed = do.process_serialized_pipelines(temp_dir=temp_dir, iterator=pipelines_iterator)
    else:
        print('Beginning of the execution')
        if not dry_run:
            # Split the copc in many tiles whose dimensions are entered by the user
            iterator = do.splitCopc(kwargs.get('file'), output_dir, pipeline, kwargs.get('resolution'), kwargs.get('tile_size'))
            # Process pipelines
            delayed = do.process_pipelines(output_dir=output_dir, json_pipeline=pipeline, iterator=iterator,
                                           temp_dir=temp_dir, copc=True)
        else:
            # Get the number of tiles we want to do the test execution (not serialized)
            iterator = do.splitCopc(kwargs.get('file'), output_dir, pipeline, kwargs.get('resolution'), kwargs.get('tile_size'), dry_run)
            # Process pipelines (not serialized)
            delayed = do.process_pipelines(output_dir=output_dir, json_pipeline=config.get('pipeline'), iterator=iterator, dry_run=dry_run, copc=True)

    client = config_dask(kwargs.get('n_workers'), kwargs.get('threads_per_worker'))

    print('Parallelization started')

    compute_and_graph(client, delayed, output_dir, kwargs.get('diagnostic'))

    print('Job just finished')


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal_parallelizer")
    main()
