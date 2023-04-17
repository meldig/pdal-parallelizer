"""
File manager.

Responsible for retrieving all files requested by the user
"""

import os.path
import pickle
import sys
from os import listdir
from os.path import join
import statistics
import subprocess
import json


def get_files(input_directory, nFiles=None):
    """Returns the files of the input directory"""
    files = []
    # If it's not a dry run, all the files are returned
    if not nFiles:
        try:
            for f in listdir(input_directory):
                files.append(join(input_directory, f))
        except NotADirectoryError:
            sys.exit("The input attribute of your configuration file does not designate a directory. Maybe you are "
                     "trying to process a single file ? Check your -it option.")
    # If it's a dry run, only the biggest nFiles of the directory are returned
    else:
        try:
            # Get all the files
            filenames = [join(input_directory, f) for f in listdir(input_directory)]
            # Create tuples (filepath, filesize)
            filesSize = [(join(input_directory, f), os.path.getsize(f)) for f in filenames]
            # Sort files in descending order
            filesSize.sort(key=lambda tup: tup[1], reverse=True)
            # Get the first nFiles
            for i in range(nFiles):
                files.append(filesSize[i][0])
        except NotADirectoryError:
            sys.exit("The input attribute of your configuration file does not designate a directory. Maybe you are "
                     "trying to process a single file ? Check your -it option.")

    return files


def get_serialized_tiles(temp_directory):
    """Returns the pipelines that have been serialized"""
    datas = []
    for tmp in listdir(temp_directory):
        # Open the serialized pipeline
        with open(join(temp_directory, tmp), 'rb') as p:
            # Deserialize it
            data = pickle.load(p)
            datas.append(data)

    return datas
