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
import do
import file_manager
import bounds
from matplotlib import pyplot as plt


@click.group()
@click.version_option('0.7.8')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


def config_dask(n_workers, threads_per_worker):
    timeout = input('After how long of inactivity do you want to kill your worker (timeout)\n')

    cfg.set({'interface': 'lo'})
    cfg.set({'distributed.scheduler.worker-ttl': None})
    cfg.set({'distributed.comm.timeouts.connect': timeout})
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker)
    client = Client(cluster)
    return client


def compute_and_graph(client, tasks, output_dir, diagnostic):
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

    if len(listdir(temp_dir)) != 0:
        click.echo('Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution\n')
        pipeline_iterator = file_manager.getSerializedPipelines(temp_dir)
        delayed = do.createTiles(output_dir=output_dir, temp_dir=temp_dir, json_pipeline=config.get('pipeline'), pipeline_iterator=pipeline_iterator)
    else:
        click.echo('Beginning of the execution\n')
        if not kwargs.get('dry_run'):
            file_iterator = file_manager.getFiles(config.get('directories').get('input_dir'))
            delayed = do.createTiles(output_dir=output_dir, json_pipeline=config.get('pipeline'), temp_dir=temp_dir, file_iterator=file_iterator)
        else:
            file_iterator = file_manager.getFiles(config.get('directories').get('input_dir'), nFiles=kwargs.get('dry_run'))
            delayed = do.createTiles(output_dir=output_dir, json_pipeline=config.get('pipeline'), file_iterator=file_iterator, dry_run=kwargs.get('dry_run'))

    client = config_dask(kwargs.get('n_workers'), kwargs.get('threads_per_worker'))

    # ms = MemorySampler()
    click.echo('Parallelization started.\n')
    compute_and_graph(client, delayed, output_dir, kwargs.get('diagnostic'))
    # with ms.sample(label='execution', client=client):
    #     dask.compute(*delayed)

    # if kwargs.get('diagnostic'):
    #     ms.plot()
    #     plt.savefig(config.get('directories').get('output_dir') + '/memory-usage.png')

    click.echo('Job just finished.\n')


@main.command()
@click.option('-f', '--file', required=True, type=click.Path(exists=True))
@click.option('-c', '--config', required=True, type=click.Path(exists=True))
@click.option('-r', '--resolution', required=False, type=int)
@click.option('-nw', '--n_workers', required=False, type=int, default=3)
@click.option('-tpw', '--threads_per_worker', required=False, type=int, default=1)
@click.option('-ts', '--tile_size', required=False, nargs=2, type=int, default=(256, 256))
@click.option('-d', '--diagnostic', is_flag=True, required=False)
def process_copc(**kwargs):
    with open(kwargs.get('config'), 'r') as c:
        config = json.load(c)
        output_dir = config.get('directories').get('output_dir')
        temp_dir = config.get('directories').get('temp_dir')
        pipeline = config.get('pipeline')

    print('Beginning of the execution')

    iterator = do.splitCopc(kwargs.get('file'), output_dir, pipeline, kwargs.get('resolution'), kwargs.get('tile_size'))

    delayed = []
    while True:
        try:
            t = next(iterator)
            p = t.pipeline(True)
            delayed.append(do.serializePipeline(p, temp_dir, True))
        except StopIteration:
            break

    client = config_dask(kwargs.get('n_workers'), kwargs.get('threads_per_worker'))

    print('Parallelization started')

    compute_and_graph(client, delayed, output_dir, kwargs.get('diagnostic'))

    print('Job just finished')


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal_parallelizer")
    main()
