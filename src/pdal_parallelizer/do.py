import dask
import tile
import pickle
import os


@dask.delayed
def process(pipeline, temp_dir):
    temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
    pipeline[0].execute()
    try:
        os.remove(temp_file)
        print('File ' + temp_file + ' suppressed.')
    except FileNotFoundError:
        print('File ' + temp_file + ' not found.')


def processPipelines(output_dir, temp_dir, json_pipeline, files=None, pipelines=None):
    delayedPipelines = []
    if pipelines:
        for p in pipelines:
            delayedPipelines.append(dask.delayed(process)(p, temp_dir))
    else:
        tiles = []
        for file in files:
            tiles.append(tile.Tile(file, output_dir, json_pipeline))

        for t in tiles:
            pipeline = t.pipeline()
            temp_file = temp_dir + '/' + str(pipeline[1]) + '.pickle'
            with open(temp_file, 'wb') as outfile:
                pickle.dump(pipeline, outfile)
            delayedPipelines.append(dask.delayed(process)(pipeline, temp_dir))

    return delayedPipelines
