import json
import sys
import click
import dask
from dask.distributed import Client
from os import listdir
import do
import base


@click.group()
@click.version_option('0.1.7')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


@main.command()
@click.argument('config', required=True)
def process_pipelines(**kwargs):
    """Processing pipelines on many points cloud in parallel"""
    with open(kwargs.get('config'), 'r') as c:
        config = json.load(c)

    output_dir = config.get('directories').get('output_dir')
    temp_dir = config.get('directories').get('temp_dir')
    if len(listdir(temp_dir)) != 0:
        click.echo('Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution.n')
        pipelines = base.getSerializedPipelines(temp_dir)
        delayed = do.processPipelines(output_dir=output_dir, pipelines=pipelines)
    else:
        click.echo('Beginning of the execution\n')
        files = base.getFiles(config.get('directories').get('input_dir'))
        delayed = do.processPipelines(output_dir=output_dir, temp_dir=temp_dir, json_pipeline=config.get('pipeline'), files=files)

    client = Client(n_workers=2, threads_per_worker=1, asynchronous=True)
    click.echo('Parallelization started.\n')
    dask.compute(*delayed)
    click.echo('Job just finished.\n')


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal_parallelizer")
    main()