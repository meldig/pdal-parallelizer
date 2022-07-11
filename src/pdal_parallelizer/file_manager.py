"""
In the file_manager, we get all the files that we need for the executions. We can get all the files from the input directory, or get all the serialized pipelines of the temp directory
"""

import os.path
import pickle
from os import listdir
from os.path import join


def getFiles(input_directory, nFiles=None):
    if not nFiles:
        for f in listdir(input_directory):
            yield join(input_directory, f)
    else:
        files = [join(input_directory, f) for f in listdir(input_directory)]
        filesSize = [(join(input_directory, f), os.path.getsize(join(input_directory, f))) for f in files]
        filesSize.sort(key=lambda tup: tup[1], reverse=True)
        # sortedFiles = [filesSize[i][0] for i in range(len(filesSize))]
        for i in range(nFiles):
            yield filesSize[i][0]
        # return sortedFiles[0:nFiles]


def getSerializedPipelines(temp_directory):
    for tmp in listdir(temp_directory):
        with open(join(temp_directory, tmp), 'rb') as p:
            pipeline = pickle.load(p)
        yield pipeline
