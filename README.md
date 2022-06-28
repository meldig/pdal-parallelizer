# PDAL-PARALLELIZER

A simple commandline app for parallelize your pdal treatments on point clouds

# Installation
## Using Pip
```bash
  $ pip install pdal-parallelizer
```
## Manual
```bash
  $ git clone https://github.com/meldig/pdal-parallelizer
  $ cd pdal-parallelizer
  $ python setup.py install
```
# Usage
```bash
$ pdal-parallelizer
```
## Processing pipelines
`process-pipelines <config file> <n_workers> <threads_per_worker>`
```bash
$ pdal-parallelizer process-pipelines config.json 3 1
```