# Spark Examples

Spark hands-on exercise for the lecture [Distributed Data Analytics](https://hpi.de/naumann/teaching/teaching/ws-1718/distributed-data-analytics-vl-master.html).

## Task

* Discovers all unary inclusion dependencies in a given dataset (within and between all tables).
* The input dataset may consist of multiple tables.
* Evaluate the program using the TPCH dataset provided on this [website](https://hpi.de/naumann/teaching/teaching/ws-1718/distributed-data-analytics-vlmaster.html).
* The inclusion dependency discovery is based on the paper [Scaling Out the Discovery of Inclusion Dependencies (Kruse, Papenbrock, Naumann, 2015)](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/Scaling_out_the_discovery_of_INDs-CR.pdf).

## Usage

1. Build a fatjar using `sbt assembly`
2. Run the [main method](https://github.com/fawind/spark-examples/blob/master/src/main/scala/Main.scala) with the following program arguments:
    * `--path <path to folder>` - Path to the folder containing the dataset csv files. Optional, defaults to `./TPCH`.
    * `--paths <fileA,fileB,fileC>` - Direct path to the dataset files seperated by comma. Optional, defaults to `--path` argument.
    * `--cores <number of cores>` - Number of local cores to use. Optional, defaults to `4`.
