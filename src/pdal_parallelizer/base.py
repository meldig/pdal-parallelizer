import pickle
from os import listdir
from os.path import join


def getFiles(input_directory):
    return [join(input_directory, f) for f in listdir(input_directory)]


def getSerializedPipelines(temp_directory):
    pipelines = []
    for tmp in listdir(temp_directory):
        with open(join(temp_directory, tmp), 'rb') as p:
            pipelines.append(pickle.load(p))

    return pipelines
