# Parasnake
Distributed number crunching with Python.
(Keywords: numeric computing, scientific computing, HPC, distributed, simulation, parallel)

## Table of contents
- [Features](#features)
- [Introduction](#Introduction)
- [How to start the application](#How_to_start_the_application)
- [How does it compare to x?](#How_does_it_compare_to_x)
- [License](#License)

## Features

- 100% pure Python.
- Easy to use API.
- If one of the nodes crashes the server and all other nodes can still continue with their work. (Heartbeat messages are used internally.)
- While running the user application more nodes can be added dynamically to speed up computation even more.
- The nodes can be a mixture of different OS and hardware architecture.
- Allows to mix different clusters from different locations.

**Note 1:** *It is still in development and the API may change.*

**Note 2:** *The communication between the server and the nodes is encrypted using a secret key.*

## Introduction

Parasnake is a library that allows users to write distributed code easily. The code is organized in two main parts: one for the server and one for the node.

This is reflected in the two classes that must be derived from:

1. The PSServer class: Each node has to register once to the server before it can start computing. After that first initial step the server keeps sending new data to each node as soon as they request it. After processing the data each node send the result back to the server who merges all the processed data together. Once all processing is done the server sends a **quit** message to each node.

    1.1 `ps_run()`: This method must be called by the user to start the server.

    1.2 `ps_get_init_data(node_id)`: This method can be implemented by the user so that each node receives some initial data.

    1.3 `ps_is_job_done()`: This method must be implemented by the user in order to determine if all processing is done.

    1.4 `ps_node_timeout(node_id)`: This method can be implemented by the user. If a node misses a heartbeat message the server can mark the data block as unfinished and other nodes can process it instead.

    1.5 `ps_get_new_data(node_id)`: This method must be implemented by the user. It returns new data that is sent to the node in order to process it.

    1.6 `ps_process_result(node_id, result)`: This method must be implemented by the user. It receives the processed data from a node and can store it internally.

2. The PSNode class: Each node will register to the server and gets a unique node id. Then the nodes request new, unprocessed data from the server. After processing the data the nodes will send the result back to the server and request more data until the server sends the **quit** message.

    2.1 `ps_run()`: This method must be called by the user to start the node.

    2.2 `ps_init(data)`: This method can be implemented by the user if the nodes need some initial data after registration.

    2.3 `ps_process_data(data)`: This method must be implemented by the user. It receives new, unprocessed data from the server and processes it. After that this method must return the processed data that will be sent back to the server.

## How to start the application

### Manually

Usually there is one application for both the server and the node code. You just specify which mode is used (for example with a command line switch).
Since there is only one server and lots of nodes, the node mode should be the default.
For example here we are using the switch "-s" to specify the server mode:

```bash
./myapp.py -s & # option "-s" means run in server mode.

./myapp.py & # start one task in node mode, this is used more often so no need to specify this mode.

./myapp.py & # start another task in node mode.

./myapp.py --ip ip_of_server & # from a different computer.
```

And in the main function a simple check does the job:

```python
def main():
    # Read settings from command line or configuration file
    options = get_option()

    if options.server:
        run_server(options)
    else:
        run_node(options)
```
### Using SLURM / sbatch

If you're using a HPC (high performance cluster) you will run new jobs through a job scheduler.
The mostly used one (at least in the [TOP500](https://www.top500.org/) list) is SLURM (Simple Linux Utility for Resource Management).

First start the server on one computer where the ip address is known (let's call it *ip_of_server*):

```bash
./myapp.py -s & # again here "-s" means run in server mode
```

Then make sure that the application is available on all the compute node in the same folder / path.
Now you can start multiple jobs using the job scheduler, in this case start 8 jobs:

```bash
for i in {1..8}; do sbatch run_single.sbatch; done
```

The batch file "run_single.sbatch" may look like this, we're using the command line option "--ip" to specify the ip address of the server (*ip_of_server* from above):

```bash
#!/bin/bash -l
## Example run script for Parasnake with SLURM

## General configuration options
#SBATCH -J Parasnake
#SBATCH -o Parasnake.%j.%N.out
#SBATCH -e Parasnake.%j.%N_Err.out
#SBATCH --mail-user=my_email@somewhere.com
#SBATCH --mail-type=ALL

## Machine and CPU configuration
## Number of tasks per job:
#SBATCH -n 1
## Number of nodes:
#SBATCH -N 1
## How long will this job run at maximum, here for example 8 hours:
#SBATCH --time=08:00:00
## How much memory will this job consume at maximum, here for example 128 MB
#SLURM --mem-per-cpu=128

# Ensure that all the applications are available on all the cluster nodes at the same place.
# Usually this is done in the cluster setup via NFS or some other distributed
# filesystem already.

# change this to the actual ip address of the system where the server is running.
myapp.py --ip ip_of_server
```

Now let's imagine that your code is running fine but it will take a lot of time before it is finished. What should you do ?
You can log in to the same cluster again (or a different cluster) and just start more nodes to help speed up the computation:

```bash
for i in {1..32}; do sbatch run_single.sbatch; done
```

This time we've just started additional 32 jobs, so we have a total of 8 + 32 = 40 jobs.

### Using PBS / Torque / qsub

Another commonly used job scheduler is PBS (Portable Batch System) or Torque. Again ensure that the application is available on all compute nodes.
Then you can start multiple jobs (8 in this case):

```bash
for i in {1..8}; do qsub run_single.qsub; done
```

Here the file "run_single.qsub" may look like this:

```bash
#!/bin/bash -l
## Example run script for Parasnake with PBS
#PBS -N Parasnake
#PBS -o ${PBS_JOBNAME}.out_${PBS_JOBID}
#PBS -j oe
## Mailing information a(bort),b(egin),e(nd)
#PBS -m abe
#PBS -M my_email@somewhere.com

## Machine and CPU configuration
## Number of tasks per job:
#PBS -l nodes=1:ppn=1
## How long will this job run at maximum, here for example 8 hours:
#PBS -l walltime=8:00:00
## How much memory will this job consume at maximum, here for example 128 MB
#PBS -l pmem=128mb

# Ensure that all the applications are available on all the cluster nodes at the same place.
# Usually this is done in the cluster setup via NFS or some other distributed
# filesystem already.

# change this to the actual ip address of the system where the server is running.
myapp.py --ip ip_of_server
```

And again like with the other job scheduler you can just start additional jobs at any time to speed up the computation.

## Full working examples

Using the two classes looks complicated at first but there are a couple of examples that show how to use it in "real world" applications:

- Distributed [Mandelbrot](examples/mandel/)
- Distributed [Path Tracing](examples/path_tracing/), TODO...
- Add more examples...

## How does it compare to *x* ?

### MPI

[MPI](https://www.open-mpi.org/) (Message Passing Interface) is the gold standard for distributed computing. It's battle tested, highly optimized and has support for C, C++ and Fortran. There are also bindings for other programming languages available.
Parasnake works mainly with Python and is still in development. It is possible to call C / C++ / Fortran code like numpy does it.
Writing correct code in MPI is difficult and if one of the nodes crash all other nodes (including the main node) will be terminated. Parasnake on the other hand doesn't care if one of the nodes crashes. The heartbeat system detect if one of the nodes is no longer available and the server continues with the other nodes. Python is easy to learn and Parasnake takes care of all the communication between the server and the nodes.

The number of nodes in MPI is fixed the whole time once the application has started, whereas with Parasnake you can add more and more nodes while it's running if needed.
Using MPI you are limited what kind of CPU / architecture / operating system you use when you want to run your application on a cluster. With Parasnake you can mix different OS, CPUs and job scheduler as needed. You just have to ensure that the node code run on your hardware.

Another advantage of MPI is that memory can also be distributed across nodes. So if the data is too big to fit into the memory of one nodes it can be split up.
This can also be done with Parasnake, since it doesn't need any specific memory layout. In the mandel example all the data is stored on the server. But the server could just pass a file name and an offset + size to each node and then the nodes would read in that huge file at that offset (which is different for each node) and just send the results back to the server (or write them to disk directly and let the server know the chunk of file was processed successfully). (An example to show how this is done needs to be provided.)

### BOINC

[BOINC](https://boinc.berkeley.edu/) (Berkeley Open Infrastructure for Network Computing) is also used for distributed computing and is well tested and high-performance. Scientists use it to scale their computations massively by providing desktop clients that can run as screen savers on normal PCs, Macs, etc. It utilizes the resources that the computer is currently not using for numeric computations. The most famous (and first) application is the SETI@home client.
Parasnake is a lot smaller but also a lot easier to set up ;-)

## License

This library is licensed under the MIT license.

