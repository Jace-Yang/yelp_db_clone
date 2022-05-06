# Spark DataFrame Tutorial

## Introduction

[Apache Spark](https://spark.apache.org/) is a DB engine that executes large-scale data processing jobs on both single-node machines or clusters. Originally, it was developed at the University of California, Berkeley's AMPLab.

The foundation of Spark is Spark Core, which provides distributed data execution on resilient distributed dataset (RDD) through multiple application programming interface (Java, Python, Scala, and R).

<center><img src="https://github.com/Jace-Yang/yelp_db_clone/raw/main/images/spark-architecture-and-ecosystem.png" width="50%"/></center>

Built upon spark core, Spark ecosystem is composed of the several modules. One of them is spark dataframe, which we will focus on its python version in this tutorial!

 In the *problem and solution* section, I will give a self-contained introduction from hadoop -> spark core/rdd -> spark SQL/dataframe. And in the *tutorial* section, I will work through an example of data-mining a 8.6 GB yelp dataset and extracting insight from it!

## The Problem and Solution

* The problems that Spark solves:

    - Since a decade ago, databases have rapidly outgrown the capabilities of a single machine to handle the ever-increasing load with tolerable latency.
    
        When a single node's hardware upgrade (increasing memory, storage, or hiring better CPUs) is too expensive to justify the expense, another option to meet this demand is to utilize more machines!

    - Then came Hadoop, with its HDFS + YARN + MapReduce software framework:
        - `HDFS(Hadoop Distributed File System)` is for distributedly storing data, which has become the industry standard now. <mark style="background-color:#c3dbfc;">#Still in use</mark>

        - `YARN(Yet Another Resource Negotiator)` allocates computational resources to various applications. <mark style="background-color:#c3dbfc;">#Still in use</mark>

        - `MapReduce` is the programming model. It is designed to processes data through map function, implicitly combine and partition, and then the reduce funtion. [A good tutorial of MapReduce](https://www.talend.com/resources/what-is-mapreduce/). 
        
            However, due to lack of abstraction for utilizing distributed memory, the default Hadoop MapReduce will costly run lots of I/O for intermediate data to a stable file system (e.g. HDFS). Furthermore, the Map + Reduce framework cannot define some complex data processing procedures, especially those involving joining. <mark style="background-color:#c3dbfc;">#Not used ver often</mark>
 
    - Then Spark came out, replacing the MapReduce module of Hadoop infrastructure to make it fast! Besides Scala, it also supports Java, python and R, which is important given the typical skillset of today's data engineer. But Spark Core also lacks API for some data transformation processes like built-in aggregation functions.

* How does Spark DataFrame solve the problems:

    - **Spark's in-memory:** Unlike Hadoop MapReduce, Spark stores intermediate results in memory, which highly save cost of I/O to disk and make calculations fast.

    - **Built upon Spark RDD:** Spark RDD is the data structure in Spark Core, which allows developers implicitly store imtermediate data set in the memory, and perform much more flexible MapReduce like data operations.

    - **Spark DataFrame**: Built upon Spark Core, Spark dataframe another powerful data abstraction in Spark. It implements the `table` in the relational database schema, making spark code easier to write when developers are dealing with structureal or semi-structural datasets.

* What are the alternatives, and what are the pros and cons of Spark DataFrame compared with alternatives?  (what makes it unique?)

    |  | Spark DataFrame | Alternatives |
    | :--- | :--- | :--- |
    | Pros | **Compatibility with the Hadoop framework:**, it can run in Hadoop Yarn, Apache Mesos, Kubernetes, clusters, and it can connect to a number of databases such as HBase, HDFS, Hive, and Cassandra. | PostgreSQL, DucksDB, ··· |
    |  | **Unify:** Prior to the advent of Spark, multiple Big Data tools had to be deployed in an organization to perform multiple Big Data analytics tasks, such as Hadoop for offline analytics, MapReduce for querying data, Hive for querying data, Storm for streaming data processing, and Mahout for machine learning. This complicates the development of large data systems and leads to complex system operations and maintenance. <br> Spark instead unify the data format between many system, serving as the "One Stack to Rule Them All.", as AMPLab said. | Use Hadoop + other tools |
    | |**Fast:** Spark runs 100x faster than Hadoop MapReduce for in-memory based operations, and roughly 10x faster in Hard-disk based operations. |Hadoop MapReduce|
    | |**Easier to scale:** Although SQL is expressive and convenient to program, it is harder than Spark to scale up when the data size goes up to TB and even PB per day. |SQL|
    | |**Easier to write:** dealing with large-scale structured data or semi-structured data, which is very common, spark dataframe provides more built-in functions to process data. | Spark RDD|
    | Cons | In the context of large-scale dataset, spark definitely beats pandas/dplyr(one of R's dataframe modules) that can only run in single machine with limited memory. But for small data, spark will encounter lots of waste in computational resource in, e.g, reading/storing small random data. Also, now we have communication cost among machines and launching time! And the lazy commit feature of Spark can make you confused at the first time (will show you in the tutorial!)| Pandas/dplyr··· |
    |  | By default, Hadoop data is not encrypted so there is a risk of data leakage if data is transmitted over Internet between nodes | Single machine
        
* How Spark DataFrame relates to topics from 4111

    - Spark DataFrame is the table object in SparkSQL, which is very similar to the table we learn from this course. They are both two-dimensional with rows and columns and a schema that the developer can specify base on ER model.

    - Fundementally, the order of executing SQL query is the relational algebra. And in PySpark, we directly write data processing query in order of relational algebra!

    - In fact, we can even directly run SQL query upon spark dataframe using API, and applied we learned in this course directly in Spark! 
    
        However, for the purposes of this lesson, I merely use its built-in features. In the following tutorial, you will see that it can accomplish comparable things to what we taught in SQL Basics/SQL Advanced courses (such as join, where, and window function).

## Tutorial & Examples

Please check the ipynb notebook [here](https://github.com/Jace-Yang/yelp_db_clone/blob/main/spark_data_analysis/examples.ipynb).