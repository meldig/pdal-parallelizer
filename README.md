# PDAL-PARALLELIZER

A simple commandline app to parallelize your pdal pipelines on point clouds

# Installation
### Using Pip
```bash
  $ pip install pdal-parallelizer
```
### Manual
```bash
  $ git clone https://github.com/meldig/pdal-parallelizer
  $ cd pdal-parallelizer
  $ python setup.py install
```
# Usage
```bash
$ pdal-parallelizer
```

### Processing pipelines
`process-pipelines <config file> <n_workers> <threads_per_worker>`

#### Config file

Your configuration file must be like that : 

```json
{
    "directories": {
        "input_dir": "The folder that contains your input files",
        "output_dir": "The folder that will receive your output files",
        "temp_dir": "The folder that will contains your temporary files"
    },
    "pipeline": "Your pipeline path"
}
```

#### Options

- -c (--config) : path of your config file.
- -nw (--n_workers) : number of cores you want for processing [default=3]
- -tpw (--threads_per_worker) : number of threads for each worker [default=1]
- -dr (--dry_run) : number of files to execute the test
- -d (--diagnostic) : get a graph of the memory usage during the execution

###### Dry run

Dry runs are test executions that allows the user to check if it's parameters are good or not.
These dry runs are executed on a subassembly of your input files. You can use it like that :

```bash
$ pdal-parallelizer ... -dr 5
```

With this command, pdal-parallelizer will take the 5 biggest files of your input directory and test your parameters on it.
So you will see if the execution is too slow (if it's the case, you can increase the -nw option) and especially if your
timeout value is high enough.

If everything is good, you can lauch your treatment on your whole input directory (i.e. without specifying -dr option). If not, you can execute a new dry run with other options.

**Advice :** If your dry run is infinite, it's probably because your workers are down. Try to increase the timeout value and re-test.

**PS :** Dry runs or just **test** executions, so it's not serializing your pipelines. Do not launch a dry run on your entire directory, if there is something wrong during it all your pipelines will have to be re-run, even those that have passed. 

###### Diagnostic

At the end of each execution, you can get a graph of the memory usage. This may be interesting to analyze after a dry run. This graph will be in the output directory you specify in the config file.

#### Exemple

```bash
$ pdal-parallelizer process-pipelines -c config.json -nw 3 -tpw 1
```

#### Requirements

PDAL 2.4+ (eg `conda install -c conda-forge pdal`)