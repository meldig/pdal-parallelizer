Usage
=====

Before using pdal-parallelizer you will have to create a configuration file the tool will use to get your input files and write the cloud when processed.

This file look like this :

.. code-block:: json

    {
        "input": "The folder that contains your input files (or a file path)",
        "output": "The folder that will receive your output files",
        "temp": "The folder that will contains your serialized pipelines",
        "pipeline": "Your pipeline path"
    }

**If you want to process a single file, you will have to put the path of it in the input value of the config file.**

pdal-parallelizer dry runs
..........................

There is only one function in pdal-parallelizer called process-pipeline and it have a parameter called "dry run". But what is it ?

Dry runs are test executions that allows the user to check if it's parameters are good or not. These dry runs are executed on a subassembly of your input files.

So, if you specify the dry run parameter is equal to 5, pdal-parallelizer will take the 5 biggest files of your input directory and test your parameters on it. So you will see if the execution is too slow or if the memory blow up.

If everything is good, you can lauch your treatment on your whole input directory (i.e. without specifying -dr option). If not, you can execute a new dry run with other options.

pdal-parallelizer good practices
................................

Sometimes, your executions could fail and you will get an error. But why ?

Your PDAL pipelines will be processed through a Dask Cluster with a defined number of workers. Each worker will have a little part of your machine memory to process their tasks. However, when a PDAL pipeline is processed, it have to open your input file and to write the result of your treatments. These actions can take a lot of memory and make the distributed memory blow.

To avoid this problem, always try to have 1 task per worker. Also, when you process a single file do not cut your data in very small tiles.