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
      "input": "The folder that contains your input files (or the file path of your copc)",
      "output": "The folder that will receive your output files",
      "temp": "The folder that will contains your temporary files"
      "pipeline": "Your pipeline path"
  }

Processing pipelines
................................................

.. code-block:: 

  process-pipelines -c <config file> -nw <n_workers> -tpw <threads_per_worker> -dr <number of files> -d
  process-pipelines -c <config file> -nw <n_workers> -tpw <threads_per_worker>` --copc -ts <tiles size> -d -dr <number of tiles> -b <buffer size>

Options
.................................................

- -c (--config) : path of your config file.
- -nw (--n_workers) : number of cores you want for processing [default=3]
- -tpw (--threads_per_worker) : number of threads for each worker [default=1]
- --copc : this flag indicate you will process a copc file. (optional)
- -r (--resolution) : resolution of the tiles (optional)
- -ts (--tile_size) : size of the tiles [default=(100, 100)] (-ts 100 100) (If a tile does not contain any points, it will be not processed) (optional)
- -b (--buffer) : size of the buffer that will be applied to the tiles (in all 4 directions) (optional)
- -rb (--remove_buffer) : this flag indicate you want to remove the buffer when your tiles are written (optional)
- -dr (--dry_run) : number of files to execute the test [default=None]
- -d (--diagnostic) : get a graph of the memory usage during the execution (optional)

If you specify the copc flag, you must change a little bit your config file. The input value will contains the path of your copc file, no longer the path of your directory of inputs.
The -r, -ts, -b and -rb options are related to copc processing. So if you want to process las files for example, you don't have to specify these.

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

**PS2 :** If you want to do dry run for copc, pdal-parallelizer will take random tiles in your input file. If this tile does not contain any points, it wil be skipped and pdal-parallelizer will take another.

Diagnostic
==========

At the end of each execution, you can get a graph of the memory usage. This may be interesting to analyze after a dry run. This graph will be in the output directory you specify in the config file.

Requirements
...........................................

PDAL 2.4+ (eg `conda install -c conda-forge pdal`)
