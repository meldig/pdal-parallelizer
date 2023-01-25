Command Line User Guide
================================================

pdal-parallelizer also have a command-line interface.

Processing pipelines with pdal-parallelizer CLI
................................................

To use pdal-parallelizer in the command line you just have to call the executable (pdal-parallelizer) and the function (process-pipelines) and then you can pass all the options needed for your treatment. You can check the options name above.

When you use pdal-parallelizer CLI, you have to specify the timeout like in the API version but it is a bit different. After you enter your command, you will see a message like this :

.. code-block::

    After how long of inactivity do you want to kill your worker (timeout) ?

Here, you just have to enter the desired timeout and press enter, the the treatment will begin.

.. code-block::

    pdal-parallelizer process-pipelines CONFIG INPUT_TYPE [OPTIONS]

**Required arguments**

    -c, --config

Path of your config file.

    -it, --input_type

This option indicates whether you are processing a single file or a list of files : single or dir. If single, please change the input filed of the config file to put the path of your file instead of the path of your input directory.

**Options**

    -nw, --n_workers

Number of cores you want for processing [default=3]

    -tpw, --threads_per_worker

Number of threads for each worker [default=1]

    -dr, --dry_run

Number of files to execute the test [default=None]

    -d, --diagnostic

Get a graph of the memory usage during the execution

**Some options are only related to single file processing**

    -ts, --tile_size

Size of the tiles [default=(256,256)]

    -b, --buffer

Size of the buffer that will be applied to the tiles (in all 4 directions)


    -rb, --remove_buffer

This flag indicate you want to remove the buffer when your tiles are written. If you choose not to delete the buffer, it will be assigned the withheld flag.

    -bb, --bounding_box

Coordinates of the bounding box you want to process (minx miny maxx maxy)

    -mt, --merge_tiles

This flag indicate you want to merge all the tiles at the end of a single cloud treatment