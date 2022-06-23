import pickle
import json
import dask
import do
from dask.distributed import Client
from os import listdir
from os.path import join

with open('./config_dev.json') as j:
    config = json.load(j)


def getFiles(input_directory):
    return [join(input_directory, f) for f in listdir(input_directory)]


def getSerializedPipelines(temp_directory):
    pipelines = []
    for tmp in listdir(temp_directory):
        with open(join(temp_directory, tmp), 'rb') as p:
            pipelines.append(pickle.load(p))

    return pipelines


if __name__ == "__main__":
    output_dir = config.get('directories').get('output_dir')

    if len(listdir('./temp')) != 0:
        pipelines = getSerializedPipelines('./temp')
        delayed = do.processPipelines(output_dir=output_dir, pipelines=pipelines)
    else:
        files = getFiles(config.get('directories').get('input_dir'))
        delayed = do.processPipelines(output_dir=output_dir, files=files)
        client = Client(n_workers=2, threads_per_worker=1)

    dask.compute(*delayed)