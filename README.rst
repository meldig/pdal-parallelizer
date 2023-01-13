================================================
PDAL-PARALLELIZER
================================================

Some processing on point clouds can be very time consuming, this problem can be solved by using several processes on a machine to run calculations in parallel. The pdal-parallelizer tool will allow you to fully use the power of your machine very simply to put it at the service of your processing.

pdal-parallelizer is a tool that allows you to process your point clouds through pipelines that will be executed on several cores of your machine. This tool uses the flexible open-source Python library Dask for the multiprocess side and allows you to use the power of the Point Data Abstraction Library, PDAL to write your pipelines.

It also protect you from any problem during the execution. Indeed, as the points clouds treatments can be really long, if something goes wrong during the execution you don’t want to restart this from the beginning. So pdal-parallelizer will serialize each pipeline to protect you from this.

Read the documentation for more details : https://pdal-parallelizer.readthedocs.io/

Installation
-----------------------------------------------

Using Pip
................................................

.. code-block::

  pip install pdal-parallelizer

Using Conda
................................................

.. code-block::

  conda install -c clementalba pdal-parallelizer
  
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

Processing pipelines with API
.............................

.. code-block:: python

    from pdal_parallelizer import process_pipelines as process

    process(config="./config.json", input_type="single", timeout=500, n_workers=5, diagnostic=True)

Processing pipelines with CLI
................................................

.. code-block:: 

  pdal-parallelizer process-pipelines -c <config file> -it dir -nw <n_workers> -tpw <threads_per_worker> -dr <number of files> -d
  pdal-parallelizer process-pipelines -c <config file> -it single -nw <n_workers> -tpw <threads_per_worker> -ts <tiles size> -d -dr <number of tiles> -b <buffer size>

Requirements (only for pip installs)
...........................................

Python 3.9+ (eg conda install -c anaconda python)

PDAL 2.4+ (eg `conda install -c conda-forge pdal`)
