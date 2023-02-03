Python API
================================================

You can use pdal-parallelizer by importing it directly in your Python code.

There is an exemple of the usage of the API:

.. code-block:: python

    from pdal_parallelizer import process_pipelines as process

    process(config="link of my config file",
            input_type="single",
            tile_size=(100, 100),
            n_workers=5,
            threads_per_worker=2,
            diagnostic=True)

There is only one function in pdal-parallelizer:

.. code-block:: python

    process(config, input_type, timeout, n_workers, threads_per_worker, dry_run, diagnostic, tile_size, buffer, remove_buffer, bounding_box, process)

Process points clouds.

Parameters:
...........

**config (*str*)**

    Path of your config file.

**input_type (*str*)**

    This parameter indicates whether you are processing a single file or a list of files. It can take only two values: "single" or "dir". If single, please change the input filed of the config file to put the path of your file instead of the path of your input directory.

**timeout (*int*, *optional*)**

    Time before a worker is killed for inactivity. If you do not specify a timeout, you will need to specify it before the start of the run on the command line.

**n_workers (*int*, *optional*)**

    Number of cores you want for processing. (default=3)

**threads_per_worker (*int*, *optional*)**

    Number of threads for each worker. (default=1)

**dry_run (*int*, *optional*)**

    Number of files to execute the test.

**diagnostic (*bool*, *optional*)**

    Get a graph of the memory usage during the execution. (default=False)

Parameters related to single file processing:
.............................................

**tile_size (*tuple*, *optional*)**

    Size of the tiles. (default=(256,256))

**buffer (*int*, *optional*)**

    Size of the buffer that will be applied to the tiles. (in all 4 directions)

**remove_buffer (*bool*, *optional*)**

    If True, it indicate you want to remove the buffer when your tiles are written. If you choose not to delete the buffer, it will be assigned the withheld flag. (default=False)

**bounding_box (*tuple*, *optional*)**

    Coordinates of the bounding box you want to process. (minx, miny, maxx, maxy)

**merge_tiles (*bool*, *optional*)**

    Indicate you want to merge all the tiles at the end of a single cloud treatment. (default=False)

**remove_tiles (*bool*, *optional*)**

    If you choose to merge the tiles, set remove_tiles to True to remove the merged tiles. (default=False)