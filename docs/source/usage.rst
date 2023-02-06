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