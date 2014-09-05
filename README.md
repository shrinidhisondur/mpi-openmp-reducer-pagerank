mpi-openmp-reducer-pagerank
===========================

Project for Big Data course. Solves 2 problems

(1) Task 1
Write a parallel PageRank program in OpenMP (please see Resources Part to find
the requirement of input Web graph file). Assume that only one of the threads
reads the file. The pagerank values are initialized to a normalized identity vector
by all the threads, and then updated using a matrix vector product. The process
continues until the page ranks do not change significantly (you should explicitly
give the condition or threshold in your report).

(2) Task 2
Write a parallel reducer (from MapReduce) using MPI. Each processor has a table
of key-value pairs. Each key and value is an integer. Also, the size of the table in
each processor is equal. The output should be a partitioned table. In this table,
each key in the input appears only once, and the associated value of this key is the
sum of all values associated with it in the input. You can use a hash or a sort for
your local reduce. At the end of this step each key only appears once. For the
second step, assume that each key is mapped to a processor (e.g. using cyclic or
block distribution). This step will require a bulk communication to ensure that the
number of messages from each processor to every other processor is at most one.
