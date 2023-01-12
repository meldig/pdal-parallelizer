import json
import click
from dask import config as cfg
from dask.distributed import LocalCluster, Client, progress
from distributed.diagnostics import MemorySampler
from os import listdir
from . import do
from . import file_manager
from matplotlib import pyplot as plt
import gc


def config_dask(n_workers, threads_per_worker, timeout):
    """Make some configuration to avoid workers errors due to heartbeat or timeout problems. Set the number of cores
    to process the pipelines """
    if not timeout:
        timeout = input('After how long of inactivity do you want to kill your worker (timeout)\n')

    cfg.set({'interface': 'lo'})
    cfg.set({'distributed.scheduler.worker-ttl': None})
    cfg.set({'distributed.comm.timeouts.connect': timeout})
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker)
    client = Client(cluster)
    return client


def process_pipelines(
        config,
        input_type,
        timeout=None,
        n_workers=3,
        threads_per_worker=1,
        dry_run=None,
        diagnostic=False,
        tile_size=(256, 256),
        buffer=None,
        remove_buffer=None,
        bounding_box=None,
        process=False
):
    with open(config, 'r') as c:
        config_file = json.load(c)
        input = config_file.get('input')
        output = config_file.get('output')
        temp = config_file.get('temp')
        pipeline = config_file.get('pipeline')

        # Assertions
        assert type(config) is str
        assert input_type == "single" or input_type == "dir"
        if timeout:
            assert type(timeout) is int
        assert type(n_workers) is int
        assert type(threads_per_worker) is int
        if dry_run:
            assert type(dry_run) is int
        assert type(diagnostic) is bool
        assert type(tile_size) is tuple
        if buffer:
            assert type(buffer) is int
        if remove_buffer:
            assert type(remove_buffer) is bool
        if bounding_box:
            assert type(bounding_box) is tuple
            assert len(bounding_box) == 4

    # If there is some temp file in the temp directory, these are processed
    if len(listdir(temp)) != 0:
        click.echo(
            'Something went wrong during previous execution, there is some temp files in your temp directory.\n'
            'Beginning of the execution\n')
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

    client = config_dask(n_workers=n_workers, threads_per_worker=threads_per_worker, timeout=timeout)

    click.echo('Parallelization started.\n')

    if diagnostic:
        ms = MemorySampler()
        with ms.sample(label='execution', client=client):
            delayed = client.persist(delayed)
            progress(delayed) if process else None
            futures = client.compute(delayed)
            client.gather(futures)
        ms.plot()
        plt.savefig(output + '/memory-usage.png')
    else:
        delayed = client.persist(delayed)
        progress(delayed) if process else None
        futures = client.compute(delayed)
        client.gather(futures)

    # At the end, collect the unmanaged memory for all the workers
    client.run(gc.collect)

    file_manager.getEmptyWeight(output_directory=output)
