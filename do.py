import dask
import tile
import pickle
import os


@dask.delayed
def process(pipeline, temp_file):
    pipeline[0].execute()
    os.remove(temp_file)


def processPipelines(output_dir, files=None, pipelines=None):
    delayedPipelines = []
    if pipelines:
        for p in pipelines:
            temp_file = 'temp/' + str(p[1]) + '.pickle'
            delayedPipelines.append(dask.delayed(process)(p, temp_file))
    else:
        tiles = []
        for file in files:
            tiles.append(tile.Tile(file, output_dir))

        for t in tiles:
            pipeline = t.pipeline()
            temp_file = 'temp/' + str(pipeline[1]) + '.pickle'
            with open(temp_file, 'wb') as outfile:
                pickle.dump(pipeline, outfile)
            delayedPipelines.append(dask.delayed(process)(pipeline, temp_file))

    return delayedPipelines
