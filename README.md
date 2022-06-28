# PDAL-PARALLELIZER

A simple commandline app for parallelize your pdal treatments on point clouds

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
    }
    "pipeline": "Your pipeline path"
}
```

#### Options

- -c (--config) : path of your config file.
- -nw (--n_workers) : number of cores you want for processing [default=3]
- -tpw (--threads_per_worker) : number of threads for each worker [default=1]

```bash
$ pdal-parallelizer process-pipelines -c config.json -nw 3 -tpw 1
```