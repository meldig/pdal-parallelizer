import pickle
import json
import dask
import do
from dask.distributed import Client
from os import listdir
from os.path import join

if __name__ == "__main__":
    with open('./config_dev.json') as j:
        config = json.load(j)

    temp_directory = './temp'
    input_directory = config.get('directories').get('input_dir')
    output_dir = config.get('directories').get('output_dir')
    files = []
    pipelines = []

    if len(listdir(temp_directory)) != 0:
        for f in listdir(temp_directory):
            with open(join(temp_directory, f), 'rb') as p:
                pipelines.append(pickle.load(p))
        delayed = do.processPipelines(output_dir=output_dir, pipelines=pipelines)
    else:
        files = [join(input_directory, f) for f in listdir(input_directory)]
        delayed = do.processPipelines(output_dir=output_dir, files=files)
        client = Client(n_workers=2, threads_per_worker=1)

    dask.compute(*delayed)