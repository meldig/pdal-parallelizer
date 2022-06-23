import sys
import click
import dask
from dask.distributed import Client
from os import listdir
from do import processPipelines
from base import getSerializedPipelines, getFiles


@click.group()
@click.version_option('0.0.1')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


@main.command()
@click.argument('config', required=True)
def process_pipelines(**kwargs):
    """Processing pipelines on many points cloud in parallel"""
    config = kwargs.get('config')
    output_dir = config.get('directories').get('output_dir')
    if len(listdir('./temp')) != 0:
        click.echo('Something went wrong during previous execution, there is some temp files in your temp directory.\n Beginning of the execution.n')
        pipelines = getSerializedPipelines('./temp')
        delayed = processPipelines(output_dir=output_dir, pipelines=pipelines)
    else:
        click.echo('Beginning of the execution\n')
        files = getFiles(config.get('directories').get('input_dir'))
        delayed = processPipelines(output_dir=output_dir, files=files)

    client = Client(n_workers=2, threads_per_worker=1)
    click.echo('Parallelization started.\n')
    dask.compute(*delayed)
    click.echo('Job just finished.\n')


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal-parallelizer")
    main()