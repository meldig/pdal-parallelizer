"""
This is the main file, it contains the process-pipelines function you call in command line
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
@click.version_option('0.7.4')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


@main.command()
@click.option('-c', '--config', required=True, type=click.Path(exists=True))
@click.option('-nw', '--n_workers', required=False, type=int, default=3)
@click.option('-tpw', '--threads_per_worker', required=False, type=int, default=1)
@click.option('-dr', '--dry_run', required=False, type=int)
def process_pipelines(**kwargs):
    """Processing pipelines on many points cloud in parallel"""
    with open(kwargs.get('config'), 'r') as c:
        config = json.load(c)

    output_dir = config.get('directories').get('output_dir')
    temp_dir = config.get('directories').get('temp_dir')
    if len(listdir(temp_dir)) != 0:
        click.echo('Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution\n')
        pipelines = file_manager.getSerializedPipelines(temp_dir)
        delayed = do.createTiles(output_dir=output_dir, temp_dir=temp_dir, json_pipeline=config.get('pipeline'), pipelines=pipelines)
    else:
        click.echo('Beginning of the execution\n')
        if not kwargs.get('dry_run'):
            files = file_manager.getFiles(config.get('directories').get('input_dir'))
            delayed = do.createTiles(output_dir=output_dir, json_pipeline=config.get('pipeline'), temp_dir=temp_dir, files=files)
        else:
            files = file_manager.getFiles(config.get('directories').get('input_dir'), nFiles=kwargs.get('dry_run'))
            delayed = do.createTiles(output_dir=output_dir, json_pipeline=config.get('pipeline'), files=files, dry_run=kwargs.get('dry_run'))

    timeout = input('After how long of inactivity do you want to kill your worker (timeout)\n')

    cfg.set({'interface': 'lo'})
    cfg.set({'distributed.scheduler.worker-ttl': None})
    cfg.set({'distributed.comm.timeouts.connect': timeout})
    cluster = LocalCluster(n_workers=kwargs.get('n_workers'), threads_per_worker=kwargs.get('threads_per_worker'))
    client = Client(cluster)
    ms = MemorySampler()
    click.echo('Parallelization started.\n')
    with ms.sample(label='execution', client=client):
        dask.compute(*delayed)

    ms.plot()
    plt.savefig(config.get('directories').get('output_dir') + '/memory-usage.png')

    click.echo('Job just finished.\n')


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal_parallelizer")
    main()
