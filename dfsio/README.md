# Hadoop DFSIO

This project aims to provide a basic filesystem benchmark tool for Hadoop.

It is based on the tool `TestDFSIO`, but adds support for using other filesystems than the default `hdfs://`.

## Get started

Get the jar file and run a test benchmark:

```bash
$ yarn jar dfsio.jar io.openio.hadoop.DFSIO -write
```

You should get an output similar to this:

```bash
19/12/30 08:25:35 INFO hadoop.DFSIO:   ----- DFSIO ----- : write
19/12/30 08:25:35 INFO hadoop.DFSIO:                 Date: Mon Dec 30 08:25:35 CET 2019
19/12/30 08:25:35 INFO hadoop.DFSIO:      Number of files: 1
19/12/30 08:25:35 INFO hadoop.DFSIO: Total Processed (MB): 10
19/12/30 08:25:35 INFO hadoop.DFSIO:    Throughput (MB/s): 84.75
19/12/30 08:25:35 INFO hadoop.DFSIO:    Average IO (MB/s): 84.75
19/12/30 08:25:35 INFO hadoop.DFSIO:     IO std deviation: 0.01
19/12/30 08:25:35 INFO hadoop.DFSIO:       Total Time (s): 20.5
19/12/30 08:25:35 INFO hadoop.DFSIO:
```


## Usage & Configuration

### Options

__Modes__

The benchmark can be configured with the following modes:

* `-write`: perform a write test
* `-read`: perform a read test (first run a corresponding `-write` benchmark to generate the read files)
* `-clean`: cleanup the base directory

__Options__

* `-nrFiles`: number of files to use (default `1`)
* `-size`: size of file (default `10MB`) (multiples: `B|KB|MB|GB|TB`)
* `-baseDir`: path to the base directory (default `/tmp/dfsio`)


### Examples

_Write benchmark on s3a (10x1GB files)_

```bash
$ yarn jar dfsio.jar io.openio.hadoop.DFSIO \
    -write \
    -nrFiles 10 \
    -size 1GB \
    -baseDir s3a://dfsio/
```

_Read benchmark on hdfs (10x1GB files)_

```bash
$ yarn jar dfsio.jar io.openio.hadoop.DFSIO \
    -read \
    -nrFiles 10 \
    -size 1GB \
    -baseDir hdfs://benchmark/dfsio/
```


### Directory structure

All files are saved in the base directory `-baseDir`.

* `control`: control files for the distributed Hadoop jobs
* `data`: files to read/write during benchmark
* `write`: results for write benchmark
* `read`: results for read benchmark

## Build

Using maven:

```bash
$ mvn clean package
```

## Run tests

Using mvn:

```bash
$ mvn test
```