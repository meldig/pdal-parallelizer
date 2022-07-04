"""
In the file_manager, we get all the files that we need for the executions. We can get all the files from the input directory, or get all the serialized pipelines of the temp directory
"""

import os.path
import pickle
from os import listdir
from os.path import join


def getFiles(input_directory, nFiles=None):
    files = [join(input_directory, f) for f in listdir(input_directory)]
    if not nFiles:
        return files
    else:
        filesSize = [(join(input_directory, f), os.path.getsize(join(input_directory, f))) for f in files]
        filesSize.sort(key=lambda tup: tup[1], reverse=False)
        sortedFiles = [filesSize[i][0] for i in range(len(filesSize))]
        return sortedFiles[0:nFiles]


def getSerializedPipelines(temp_directory):
    pipelines = []
    for tmp in listdir(temp_directory):
        with open(join(temp_directory, tmp), 'rb') as p:
            pipelines.append(pickle.load(p))

    return pipelines
