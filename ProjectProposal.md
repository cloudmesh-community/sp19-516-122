# Elastic Map Reduce with Amazon Web Services

The goal of this project is to develop a Cloudmesh module that will allow the
user to interface with Amazon Web Service's Elastic Map Reduce (EMR)
architecture followed by experimentation on different Spark configurations and
how the performance changes as the number of workers is scaled up and down and
different servers are utilized. 

## Cloudmesh Module

The cloudmesh module will be developed first and allow for the user to start,
stop, and submit jobs to an EMR cluster. The module will include sensible
defaults in terms of task node setup, datacenter location, etc. and various
utility functions (what clusters are running, what is their status, etc. As
a preliminary listing, these functions will be implemented:

* list - what clusters are running.
* start # - start up a cluster with # nodes.
* stop # - stop a particular cluster.


## Experimentation and Reporting

As the second phase of the project, a suitable machine learning problem (face
detection, fingerprint sensing, etc.) will be developed using Spark. The goal
of this phase isn't to develop a new state-of-the-art solution but, rather, to
explore how Spark's distribute computing enables faster computation of
solutions to complex problems. Exploration will occur along two axes: number
of worker nodes and worker node size (large, extra large, GPU enabled, etc).
A report will describe how the performance changes along each of these axes,
highlight the cost incurred, and derive an optimal setup for the given solution
in terms of time and cost.
