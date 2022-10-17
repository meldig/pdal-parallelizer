================================================
PDAL-PARALLELIZER
================================================

A simple commandline app to parallelize your pdal pipelines on point clouds

Installation
-----------------------------------------------

Using Pip
................................................

.. code-block::

  pip install pdal-parallelizer
  
GitHub
................................................

The repository of pdal-parallelizer is available at https://github.com/meldig/pdal-parallelizer

Usage
-----------------------------------------------

Config file
................................................

Your configuration file must be like that : 

.. code-block:: json

  {
      "input": "The folder that contains your input files (or a file path)",
      "output": "The folder that will receive your output files",
      "temp": "The folder that will contains your temporary files"
      "pipeline": "Your pipeline path"
  }

Processing pipelines
................................................

.. code-block:: 

  pdal-parallelizer process-pipelines -c <config file> -it dir -nw <n_workers> -tpw <threads_per_worker> -dr <number of files> -d
  pdal-parallelizer process-pipelines -c <config file> -it single -nw <n_workers> -tpw <threads_per_worker> -ts <tiles size> -d -dr <number of tiles> -b <buffer size>

Options
.................................................

- -c (--config) : path of your config file.
- -it (--input_type) : this option indicates whether you are processing a single file or a list of files [single, dir]
- -nw (--n_workers) : number of cores you want for processing [default=3]
- -tpw (--threads_per_worker) : number of threads for each worker [default=1]
- -ts (--tile_size) : size of the tiles [default=(100, 100)] (-ts 100 100) (If a tile does not contain any points, it will be not processed) [default=(256,256)] (optional) (single file only)
- -b (--buffer) : size of the buffer that will be applied to the tiles (in all 4 directions) (optional) (single file only)
- -rb (--remove_buffer) : this flag indicate you want to remove the buffer when your tiles are written. If you choose not to delete the buffer, it will be assigned the withheld flag. (optional) (single file only)
- -bb (--bounding_box) : coordinates of the bounding box you want to process (minx miny maxx maxy) (optional) (single file only)
- -dr (--dry_run) : number of files to execute the test [default=None]
- -d (--diagnostic) : get a graph of the memory usage during the execution (optional)

If you specify ``-it single``, the input value of the config file will contains the path of your file, no longer the path of your directory of inputs.
The -ts, -b, -bb and -rb options are related to single file processing. So if you want to process a list of files, you don't have to specify these.

Dry run
=======

Dry runs are test executions that allows the user to check if it's parameters are good or not.
These dry runs are executed on a subassembly of your input files. You can use it like that :

.. code-block::

  pdal-parallelizer ... -dr 5

With this command, pdal-parallelizer will take the 5 biggest files of your input directory and test your parameters on it.
So you will see if the execution is too slow (if it's the case, you can increase the -nw option) and especially if your
timeout value is high enough.

If everything is good, you can lauch your treatment on your whole input directory (i.e. without specifying -dr option). If not, you can execute a new dry run with other options.

**Advice :** If your dry run is infinite, it's probably because your workers are down. Try to increase the timeout value and re-test.

**PS :** Dry runs or just **test** executions, so it's not serializing your pipelines. Do not launch a dry run on your entire directory, if there is something wrong during it all your pipelines will have to be re-run, even those that have passed. 

**PS2 :** If you want to do a dry run for a single file, pdal-parallelizer will take random tiles in your input file.

Diagnostic
==========

At the end of each execution, you can get a graph of the memory usage. This may be interesting to analyze after a dry run. This graph will be in the output directory you specify in the config file.

Requirements
...........................................

Python 3.9+ (eg conda install -c anaconda python)

PDAL 2.4+ (eg `conda install -c conda-forge pdal`)
