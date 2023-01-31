"""
Main file.

Contains the process-pipelines function you call in command line
"""
import sys
import click
from pdal_parallelizer import process_pipelines as process


@click.group()
@click.version_option('2.0.2')
def main():
    """A simple parallelization tool for 3d point clouds treatment"""
    pass


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
@click.option('-mt', '--merge_tiles', required=False, is_flag=True)
@click.option('-rt', '--remove_tiles', required=False, is_flag=True)
def process_pipelines(**kwargs):
    # Get all the options
    config = kwargs.get('config')
    n_workers = kwargs.get('n_workers')
    threads_per_worker = kwargs.get('threads_per_worker')
    dry_run = kwargs.get('dry_run')
    diagnostic = kwargs.get('diagnostic')
    input_type = kwargs.get('input_type')
    tile_size = kwargs.get('tile_size')
    buffer = kwargs.get('buffer')
    remove_buffer = kwargs.get('remove_buffer')
    bounding_box = kwargs.get('bounding_box')
    merge_tiles = kwargs.get('merge_tiles')
    remove_tiles = kwargs.get('remove_tiles')

    process(
        config=config,
        input_type=input_type,
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        dry_run=dry_run,
        diagnostic=diagnostic,
        tile_size=tile_size,
        buffer=buffer,
        remove_buffer=remove_buffer,
        bounding_box=bounding_box,
        merge_tiles=merge_tiles,
        remove_tiles=remove_tiles,
        process=True
    )


if __name__ == "__main__":
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("pdal_parallelizer")
    main()
