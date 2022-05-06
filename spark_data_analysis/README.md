# Spark DataFrame Tutorial: data-mining on Yelp dataset

### Introduction

[Apache Spark](https://spark.apache.org/) is a DB engine that executes large-scale data processing jobs on both single-node machines or clusters. Originally, it was developed at the University of California, Berkeley's AMPLab.

The foundation of Spark is Spark Core, which provides distributed data execution on resilient distributed dataset (RDD) through multiple application programming interface (Java, Python, Scala, and R).

<center><img src="https://github.com/Jace-Yang/yelp_db_clone/raw/main/images/spark-architecture-and-ecosystem.png" width="50%"/></center>

Built upon spark core, Spark ecosystem is composed of the several modules. One of them is spark dataframe, which we will focus on its python version in this tutorial!

 In the *problem and solution* section, I will give a self-contained introduction from hadoop -> spark core/rdd -> spark SQL/dataframe. And in the *tutorial* section, I will work through an example of data-mining a 8.6 GB yelp dataset and extracting insight from it!

### The Problem and Solution

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



### Motivations of this tutorial

In my [yelp database/web implementation](https://github.com/Jace-Yang/yelp_db_clone) project, we clone a Yelp full-stack web app using the official website.

<img width = "75%" src="https://github.com/Jace-Yang/yelp_db_clone/raw/main/images/demo/1_filter.gif" />

We spent lots of time on the yelp dataset in order to populate our (fake) web. But we ended up removing over 90% of the 8GB+ data to lighten our PostgreSQL DB. So in this wiki, I am very interested into learning some big data-mining stuff using spark and see what I can do to the entire dataset!

You can find the following ipynb notebook in our [Github](https://github.com/Jace-Yang/yelp_db_clone/blob/main/spark_data_analysis/examples.ipynb), where I also show the omitted environmen setup part.


### **Envionment Setup**

- **Step 0:** Setup a spark cluster. In this tutorial I use [GCP dataproc](https://cloud.google.com/dataproc).

    > Setup Guide: [CSEE 4121 2022S HW2 programming](https://csee-4121-2022.github.io/homeworks/hw2.html)

- **Step 1:** Download dataset from [yelp dataset](https://www.yelp.com/dataset/documentation/main) and upload all json files except the `photos` into a GCP bucket. In this case the bucket name is `coms4111`, and I placed it into a directory that jupyterlab can directly access it through `GCS` folder.

    ---

    Below step needs to be done every time you create a new cluster

    ---

- **Step 2:** Clone the repository to the cluster's local disk


```python
!git clone https://github.com/Jace-Yang/yelp_db_clone
```

- **Step 3:** Download external package in order to parse XML files: spark-xml with version 2.12-0.14.0 to support Spark 3.1.2 and Scala 2.12.


```python
!sudo hdfs dfs -get gs://csee4121/homework2/spark-xml_2.12-0.14.0.jar /usr/lib/spark/jars/
    # Reference: https://csee-4121-2022.github.io/homeworks/hw2.html
```

> Note: if you are using multiple GCP dataproc nodes, run `sudo hdfs dfs -get gs://csee4121/homework2/spark-xml_2.12-0.14.0.jar /usr/lib/spark/jars/` on every worker VM machines by SSH them.

- **Step 4:** Move data from GS into a HDFS directory every time you create a new cluster. We do this by moving data into the local disk first, then to HDFS!


```python
# Gs -> Local
!mkdir yelp_db_clone/data/
!gsutil cp gs://coms4111/notebooks/jupyter/data/*.json file:///yelp_db_clone/data/
```

    Copying gs://coms4111/notebooks/jupyter/data/yelp_academic_dataset_business.json...
    Copying gs://coms4111/notebooks/jupyter/data/yelp_academic_dataset_checkin.json...
    Copying gs://coms4111/notebooks/jupyter/data/yelp_academic_dataset_review.json...
    Copying gs://coms4111/notebooks/jupyter/data/yelp_academic_dataset_tip.json...  
    | [4 files][  5.5 GiB/  5.5 GiB]   74.2 MiB/s                                   
    ==> NOTE: You are performing a sequence of gsutil operations that may
    run significantly faster if you instead use gsutil -m cp ... Please
    see the -m section under "gsutil help options" for further information
    about when gsutil -m can be advantageous.
    
    Copying gs://coms4111/notebooks/jupyter/data/yelp_academic_dataset_user.json...
    / [5 files][  8.6 GiB/  8.6 GiB]  101.8 MiB/s                                   
    Operation completed over 5 objects/8.6 GiB.                                      



```python
# Local -> HDFS
!hdfs dfs -cp -f file:///yelp_db_clone/data/* hdfs:///user/dataproc/
```


```python
# Check whether data is now in HDFS!
!hdfs dfs -ls hdfs:///user/dataproc/
```

    Found 5 items
    -rw-r--r--   1 root hadoop  118863795 2022-05-05 14:28 hdfs:///user/dataproc/yelp_academic_dataset_business.json
    -rw-r--r--   1 root hadoop  286958945 2022-05-05 14:28 hdfs:///user/dataproc/yelp_academic_dataset_checkin.json
    -rw-r--r--   1 root hadoop 5341868833 2022-05-05 14:28 hdfs:///user/dataproc/yelp_academic_dataset_review.json
    -rw-r--r--   1 root hadoop  180604475 2022-05-05 14:28 hdfs:///user/dataproc/yelp_academic_dataset_tip.json
    -rw-r--r--   1 root hadoop 3363329011 2022-05-05 14:29 hdfs:///user/dataproc/yelp_academic_dataset_user.json


### Tutorial

#### Examples


```python
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
```

- Sometimes I get tired and use `from pyspark.sql.functions import *`

##### Get a spark session


```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    22/05/05 22:17:30 INFO org.apache.spark.SparkEnv: Registering MapOutputTracker
    22/05/05 22:17:30 INFO org.apache.spark.SparkEnv: Registering BlockManagerMaster
    22/05/05 22:17:31 INFO org.apache.spark.SparkEnv: Registering BlockManagerMasterHeartbeat
    22/05/05 22:17:31 INFO org.apache.spark.SparkEnv: Registering OutputCommitCoordinator


##### Read Data & Print Schema


```python
review = spark.read.json('hdfs:///user/dataproc/yelp_academic_dataset_review.json')
review.printSchema()
```

    [Stage 0:======================================================>  (38 + 2) / 40]

    root
     |-- business_id: string (nullable = true)
     |-- cool: long (nullable = true)
     |-- date: string (nullable = true)
     |-- funny: long (nullable = true)
     |-- review_id: string (nullable = true)
     |-- stars: double (nullable = true)
     |-- text: string (nullable = true)
     |-- useful: long (nullable = true)
     |-- user_id: string (nullable = true)
    


                                                                                    


```python
business = spark.read.json('hdfs:///user/dataproc/yelp_academic_dataset_business.json')
business.printSchema()
```

    root
     |-- address: string (nullable = true)
     |-- attributes: struct (nullable = true)
     |    |-- AcceptsInsurance: string (nullable = true)
     |    |-- AgesAllowed: string (nullable = true)
     |    |-- Alcohol: string (nullable = true)
     |    |-- Ambience: string (nullable = true)
     |    |-- BYOB: string (nullable = true)
     |    |-- BYOBCorkage: string (nullable = true)
     |    |-- BestNights: string (nullable = true)
     |    |-- BikeParking: string (nullable = true)
     |    |-- BusinessAcceptsBitcoin: string (nullable = true)
     |    |-- BusinessAcceptsCreditCards: string (nullable = true)
     |    |-- BusinessParking: string (nullable = true)
     |    |-- ByAppointmentOnly: string (nullable = true)
     |    |-- Caters: string (nullable = true)
     |    |-- CoatCheck: string (nullable = true)
     |    |-- Corkage: string (nullable = true)
     |    |-- DietaryRestrictions: string (nullable = true)
     |    |-- DogsAllowed: string (nullable = true)
     |    |-- DriveThru: string (nullable = true)
     |    |-- GoodForDancing: string (nullable = true)
     |    |-- GoodForKids: string (nullable = true)
     |    |-- GoodForMeal: string (nullable = true)
     |    |-- HairSpecializesIn: string (nullable = true)
     |    |-- HappyHour: string (nullable = true)
     |    |-- HasTV: string (nullable = true)
     |    |-- Music: string (nullable = true)
     |    |-- NoiseLevel: string (nullable = true)
     |    |-- Open24Hours: string (nullable = true)
     |    |-- OutdoorSeating: string (nullable = true)
     |    |-- RestaurantsAttire: string (nullable = true)
     |    |-- RestaurantsCounterService: string (nullable = true)
     |    |-- RestaurantsDelivery: string (nullable = true)
     |    |-- RestaurantsGoodForGroups: string (nullable = true)
     |    |-- RestaurantsPriceRange2: string (nullable = true)
     |    |-- RestaurantsReservations: string (nullable = true)
     |    |-- RestaurantsTableService: string (nullable = true)
     |    |-- RestaurantsTakeOut: string (nullable = true)
     |    |-- Smoking: string (nullable = true)
     |    |-- WheelchairAccessible: string (nullable = true)
     |    |-- WiFi: string (nullable = true)
     |-- business_id: string (nullable = true)
     |-- categories: string (nullable = true)
     |-- city: string (nullable = true)
     |-- hours: struct (nullable = true)
     |    |-- Friday: string (nullable = true)
     |    |-- Monday: string (nullable = true)
     |    |-- Saturday: string (nullable = true)
     |    |-- Sunday: string (nullable = true)
     |    |-- Thursday: string (nullable = true)
     |    |-- Tuesday: string (nullable = true)
     |    |-- Wednesday: string (nullable = true)
     |-- is_open: long (nullable = true)
     |-- latitude: double (nullable = true)
     |-- longitude: double (nullable = true)
     |-- name: string (nullable = true)
     |-- postal_code: string (nullable = true)
     |-- review_count: long (nullable = true)
     |-- stars: double (nullable = true)
     |-- state: string (nullable = true)
    


    22/05/05 22:17:49 WARN org.apache.spark.sql.catalyst.util.package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.


- Here we see that spark allows semi-structure! The data like
    ```
    "hours": {
        "Monday": "10:00-21:00",
        "Tuesday": "10:00-21:00",
        "Friday": "10:00-21:00",
        "Wednesday": "10:00-21:00",
        "Thursday": "10:00-21:00",
        "Sunday": "11:00-18:00",
        "Saturday": "10:00-21:00"
    }
    ```
    has now been converted to a structure type automatically!


```python
user = spark.read.json('hdfs:///user/dataproc/yelp_academic_dataset_user.json')
user.printSchema()
```

    [Stage 2:=======================================================> (39 + 1) / 40]

    root
     |-- average_stars: double (nullable = true)
     |-- compliment_cool: long (nullable = true)
     |-- compliment_cute: long (nullable = true)
     |-- compliment_funny: long (nullable = true)
     |-- compliment_hot: long (nullable = true)
     |-- compliment_list: long (nullable = true)
     |-- compliment_more: long (nullable = true)
     |-- compliment_note: long (nullable = true)
     |-- compliment_photos: long (nullable = true)
     |-- compliment_plain: long (nullable = true)
     |-- compliment_profile: long (nullable = true)
     |-- compliment_writer: long (nullable = true)
     |-- cool: long (nullable = true)
     |-- elite: string (nullable = true)
     |-- fans: long (nullable = true)
     |-- friends: string (nullable = true)
     |-- funny: long (nullable = true)
     |-- name: string (nullable = true)
     |-- review_count: long (nullable = true)
     |-- useful: long (nullable = true)
     |-- user_id: string (nullable = true)
     |-- yelping_since: string (nullable = true)
    


                                                                                    

##### Filtering / Group By


We only care about those restaurants that are still open! So let's do a filtering first.


```python
business = business.filter(col('is_open')==1)
business.count()
```




    119698




```python
business.groupby('state').count().show()
```

    +-----+-----+
    |state|count|
    +-----+-----+
    |   AZ| 8108|
    |   LA| 7676|
    |   NJ| 7031|
    |   MI|    1|
    |   NV| 6277|
    |   ID| 3783|
    |   CA| 4065|
    |   VT|    1|
    |   DE| 1894|
    |   MO| 8363|
    |   IL| 1765|
    |   WA|    2|
    |  XMS|    1|
    |   IN| 8946|
    |   TN| 9600|
    |   PA|26289|
    |   SD|    1|
    |   AB| 4346|
    |   TX|    4|
    |   MA|    2|
    +-----+-----+
    only showing top 20 rows
    



```python
business = business \
    .groupby('state') \
    .count() \
    .filter(col('count') > 10).drop('count') \
    .join(business, on='state') 
```

##### Dealing with semi-stuctural data

The way we select those semi-stuctural columns that have been automatically infered is easy:


```python
business.select('name', 'attributes.RestaurantsDelivery', 'attributes.Wifi', 'attributes.BusinessAcceptsBitcoin').show(10)
```

    +--------------------+-------------------+-------+----------------------+
    |                name|RestaurantsDelivery|   Wifi|BusinessAcceptsBitcoin|
    +--------------------+-------------------+-------+----------------------+
    |       The UPS Store|               null|   null|                  null|
    |  St Honore Pastries|              False|u'free'|                  null|
    |Perkiomen Valley ...|               null|   null|                  null|
    |      Sonic Drive-In|               True|  u'no'|                  null|
    |     Famous Footwear|               null|   null|                  null|
    |      Temple Beth-El|               null|   null|                  null|
    |      Sonic Drive-In|               True|  u'no'|                  null|
    |           Marshalls|               null|   null|                  null|
    |Vietnamese Food T...|               null|   null|                  null|
    |             Denny's|               True|  u'no'|                  null|
    +--------------------+-------------------+-------+----------------------+
    only showing top 10 rows
    


- (But the way, what is wrong with bitcoin!

However we also have a column that is not automatically infered:


```python
business.select('business_id', 'categories').show(truncate = 100)
```

    +----------------------+------------------------------------------------------------------------------+
    |           business_id|                                                                    categories|
    +----------------------+------------------------------------------------------------------------------+
    |mpf3x-BjTdTEA3yCZrAYPw|Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services|
    |MTSW4McQd7CbVtyjqoe9mw|                         Restaurants, Food, Bubble Tea, Coffee & Tea, Bakeries|
    |mWMc6_wTdE0EUBKIGXDVfA|                                                     Brewpubs, Breweries, Food|
    |CF33F8-E6oudUQ46HnavjQ|  Burgers, Fast Food, Sandwiches, Food, Ice Cream & Frozen Yogurt, Restaurants|
    |n_0UpQx1hsNbnPUSlodU8w|      Sporting Goods, Fashion, Shoe Stores, Shopping, Sports Wear, Accessories|
    |qkRM_2X51Yqxk3btlwAQIg|                                           Synagogues, Religious Organizations|
    |bBDDEgkFA1Otx9Lfe7BZUQ|              Ice Cream & Frozen Yogurt, Fast Food, Burgers, Restaurants, Food|
    |UJsufbvfyfONHeWdvAHKjA|                                          Department Stores, Shopping, Fashion|
    |eEOYSgkmpB90uNA7lDOMRA|                                    Vietnamese, Food, Restaurants, Food Trucks|
    |il_Ro8jwPlHresjw9EGmBg|               American (Traditional), Restaurants, Diners, Breakfast & Brunch|
    |jaxMSoInw8Poo3XeMJt8lQ|              General Dentistry, Dentists, Health & Medical, Cosmetic Dentists|
    |MUTTqe8uqyMdBl186RmNeA|                                             Sushi Bars, Restaurants, Japanese|
    |rBmpy_Y1UbBx8ggHlyb7hA|                         Automotive, Auto Parts & Supplies, Auto Customization|
    |M0XSSHqrASOnhgbWDJIpQA|Vape Shops, Tobacco Shops, Personal Shopping, Vitamins & Supplements, Shopping|
    |8wGISYjYkE2tSqn3cDMu8A|                         Automotive, Car Rental, Hotels & Travel, Truck Rental|
    |ROeacJQwBeh05Rqg7F6TCg|                                                           Korean, Restaurants|
    |qhDdDeI3K4jy2KyzwFN53w|                              Shopping, Books, Mags, Music & Video, Bookstores|
    |kfNv-JZpuN6TVNSO6hHdkw|                                        Steakhouses, Asian Fusion, Restaurants|
    |9OG5YkX1g2GReZM0AskizA|                                                          Restaurants, Italian|
    |PSo_C1Sfa13JHjzVNW6ziQ|                               Pet Services, Pet Groomers, Pets, Veterinarians|
    +----------------------+------------------------------------------------------------------------------+
    only showing top 20 rows
    



```python
# Split the categories
def split_trim(categories_strings):
    '''
    Examples
    --------
    >>> split_trim('Ice Cream & Frozen Yogurt, Fast Food, Burgers, Restaurants, Food')
    ['Ice Cream & Frozen Yogurt', 'Fast Food', 'Burgers', 'Restaurants', 'Food']
    '''
    categories = categories_strings.split(', ')
    return categories
business.select('business_id', 'categories') \
    .withColumn('category', F.explode(F.split(col("categories"), ", "))) \
    .show(10, truncate = 200)
```

    +----------------------+------------------------------------------------------------------------------+-----------------+
    |           business_id|                                                                    categories|         category|
    +----------------------+------------------------------------------------------------------------------+-----------------+
    |mpf3x-BjTdTEA3yCZrAYPw|Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services| Shipping Centers|
    |mpf3x-BjTdTEA3yCZrAYPw|Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services|   Local Services|
    |mpf3x-BjTdTEA3yCZrAYPw|Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services|         Notaries|
    |mpf3x-BjTdTEA3yCZrAYPw|Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services|  Mailbox Centers|
    |mpf3x-BjTdTEA3yCZrAYPw|Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services|Printing Services|
    |MTSW4McQd7CbVtyjqoe9mw|                         Restaurants, Food, Bubble Tea, Coffee & Tea, Bakeries|      Restaurants|
    |MTSW4McQd7CbVtyjqoe9mw|                         Restaurants, Food, Bubble Tea, Coffee & Tea, Bakeries|             Food|
    |MTSW4McQd7CbVtyjqoe9mw|                         Restaurants, Food, Bubble Tea, Coffee & Tea, Bakeries|       Bubble Tea|
    |MTSW4McQd7CbVtyjqoe9mw|                         Restaurants, Food, Bubble Tea, Coffee & Tea, Bakeries|     Coffee & Tea|
    |MTSW4McQd7CbVtyjqoe9mw|                         Restaurants, Food, Bubble Tea, Coffee & Tea, Bakeries|         Bakeries|
    +----------------------+------------------------------------------------------------------------------+-----------------+
    only showing top 10 rows
    



```python
# Keep a mapping table
business_category = business.select('business_id', 'categories') \
    .withColumn('category', F.explode(F.split(col("categories"), ", "))) \
    .drop('categories')
business_category.groupby('category') \
    .count() \
    .sort(col('count').desc()) \
    .show(20, truncate = 50)
```

    [Stage 28:================================>                       (17 + 8) / 29]

    +-------------------------+-----+
    |                 category|count|
    +-------------------------+-----+
    |              Restaurants|34985|
    |                     Food|20418|
    |                 Shopping|20184|
    |            Home Services|13319|
    |            Beauty & Spas|12259|
    |         Health & Medical|11044|
    |           Local Services|10137|
    |               Automotive| 9876|
    |                Nightlife| 8379|
    |Event Planning & Services| 8171|
    |                     Bars| 7528|
    |              Active Life| 6495|
    |               Sandwiches| 6075|
    |   American (Traditional)| 5531|
    |                Fast Food| 5516|
    |          Hotels & Travel| 5123|
    |                    Pizza| 5089|
    |            Home & Garden| 5021|
    |              Auto Repair| 5001|
    |             Coffee & Tea| 4954|
    +-------------------------+-----+
    only showing top 20 rows
    


                                                                                    

##### Join

Sometimes, you want to join those columns with functional dependency like this:


```python
%%time 
review_wide = review.join(business.select('business_id', 
                                          col('name').alias('biz_name'), 
                                          'attributes.RestaurantsTakeOut', 
                                          'categories',
                                          'is_open'),
                          on='business_id',
                          how='inner') \
                     .join(user.select('user_id', 
                                      col('name').alias('user_name'), 
                                      'fans', 
                                      'yelping_since'),
                          on='user_id',
                          how='inner')
```

    CPU times: user 3.5 ms, sys: 1.63 ms, total: 5.14 ms
    Wall time: 39.5 ms


- Wait! That fast for this huge join??


```python
%%time 
review_wide.count()
```

    [Stage 45:>                                                       (0 + 37) / 40]

    CPU times: user 11.5 ms, sys: 1.63 ms, total: 13.1 ms
    Wall time: 7.52 s


    5790989



- But why so slow for counting! Aha, this is because they need to execute the delayed join as well due to the lazy commit of Spark!


```python
%%time
# But we can explictly tell DB to store it in memory
review_wide = review_wide.persist(pyspark.StorageLevel.MEMORY_ONLY)
```

    CPU times: user 1.72 ms, sys: 540 µs, total: 2.26 ms
    Wall time: 73.6 ms


- But again, the DB is doing nothing..


```python
%%time 
# Lets count!
review_wide.count()
```

    [Stage 56:================================================>    (184 + 16) / 200]

    CPU times: user 23.4 ms, sys: 13 ms, total: 36.4 ms
    Wall time: 18.8 s


    5790989



- This command is still even longer! Because this time it runs the Join and keep it in the memory!


```python
%%time 
# Now, this command is getting much more faster!
review_wide.count()
```

    CPU times: user 1.38 ms, sys: 642 µs, total: 2.02 ms
    Wall time: 384 ms





    5790989



- And finally!! Counting 6m+ rows in 1~1.5 seconds! Totally fine to me!

- Now there are 2 shotcuts I figured out to do caching:

    - `df = df.rdd.toDF()` will enforce df to because the transformation need to run through all the pending query
    
    - `df = df.cache(); df.count()` will perform `.persist(pyspark.StorageLevel.MEMORY_OR_DISK)` caching and then actually cached it


```python
review_wide.show(5)
```

    +--------------------+--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+------------------+--------------------+-------+---------+----+-------------------+
    |             user_id|         business_id|cool|               date|funny|           review_id|stars|                text|useful|            biz_name|RestaurantsTakeOut|          categories|is_open|user_name|fans|      yelping_since|
    +--------------------+--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+------------------+--------------------+-------+---------+----+-------------------+
    |--RJK834fiQXm21Vp...|aIoUwpy5ZFQXUDxWM...|   0|2019-08-25 23:17:52|    0|QPF7spAqCc-D81GeX...|  1.0|There are new own...|     0|     Pete & Shorty's|              True|Seafood, Diners, ...|      1|    Renee|   0|2018-02-04 20:34:16|
    |--UhENQdbuWEh0mU5...|K_s-9Wd6vXSfnxYFz...|   1|2017-08-06 02:42:02|    1|dghJt1TSuyFkmLddu...|  5.0|When im first arr...|     0|Kei Sushi Restaurant|              True|Sushi Bars, Resta...|      1|    Sonny|   0|2017-06-19 18:37:56|
    |--cxdcv_b9uhAKsKT...|oD3zBLplcYefdMHo5...|   0|2019-11-01 22:04:32|    0|VNT1ymOYUuWC-qxdI...|  4.0|This is definitel...|     0|  Garden Farm Market|              True|Fruits & Veggies,...|      1|   Cheryl|   0|2014-02-08 01:13:06|
    |-0EzgKMI9ZakqLiWR...|P0BB_HeVN-M8D31yt...|   0|2019-03-28 21:38:52|    0|ILPE_Jjrqu_DICC7R...|  5.0|Dr. Exelby and Mi...|     0|Rainbow Veterinar...|              null|Pet Services, Vet...|      1|   Joseph|   0|2018-01-03 21:31:49|
    |-0EzgKMI9ZakqLiWR...|6aPXOXi8h1m58hihg...|   0|2018-07-18 13:04:59|    0|C74fnh5gpnH2cgbHT...|  5.0|If you're looking...|     0| Old Northeast Pizza|              True|Pizza, Restaurant...|      1|   Joseph|   0|2018-01-03 21:31:49|
    +--------------------+--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+------------------+--------------------+-------+---------+----+-------------------+
    only showing top 5 rows
    


#### Tutorials

To better help you know how spark works. I intentially come up with some super complex query (hope it makes sense to you guys)!

##### Top 5 categories in each state?


```python
temp = business_category \
    .join(business.select('business_id', 'state'), on='business_id')  \
    .join(review.select('review_id', 'business_id'), on='business_id')  \
    .groupby(['state', 'category']) \
    .agg(F.count('review_id').alias('# of reviews')).rdd.toDF()
temp.show(10)
```

                                                                                    

    +-----+--------------------+------------+
    |state|            category|# of reviews|
    +-----+--------------------+------------+
    |   IL|             Fashion|         832|
    |   IN|       Home & Garden|        8173|
    |   FL|     Mailbox Centers|        1122|
    |   NJ|                Mags|         666|
    |   TN|Landmarks & Histo...|        4209|
    |   IN|        Pet Groomers|        2327|
    |   NV|Real Estate Services|        1744|
    |   FL|Funeral Services ...|         290|
    |   NJ|              Waxing|        3383|
    |   NV|             Tanning|        1173|
    +-----+--------------------+------------+
    only showing top 10 rows
    



```python
temp.withColumn('cate_rank_in_state', 
                F.row_number().over(Window.partitionBy("state").orderBy(col("# of reviews").desc()))) \
    .filter(col('cate_rank_in_state') <= 5) \
    .withColumn('category_info', F.concat(col("category"), F.lit(' ('), col("# of reviews"), F.lit(')'))) \
    .withColumn('cate_rank_in_state', F.concat(F.lit('NO.'), col("cate_rank_in_state"), F.lit(' reviewed category'))) \
    .groupby('state') \
    .pivot("cate_rank_in_state").agg(F.first("category_info")).show(50, truncate = 100)
```

    +-----+----------------------+----------------------+-----------------------------+------------------------------+-------------------------------+
    |state|NO.1 reviewed category|NO.2 reviewed category|       NO.3 reviewed category|        NO.4 reviewed category|         NO.5 reviewed category|
    +-----+----------------------+----------------------+-----------------------------+------------------------------+-------------------------------+
    |   AZ|  Restaurants (216409)|          Food (83123)|            Nightlife (66048)|                  Bars (63785)|                Mexican (45785)|
    |   LA|  Restaurants (461434)| Cajun/Creole (177231)|                Food (160307)|            Nightlife (156503)|               Seafood (149792)|
    |   NJ|  Restaurants (139961)|          Food (52769)|            Nightlife (29784)|American (Traditional) (29206)|                   Bars (28009)|
    |   NV|  Restaurants (196156)|          Food (85363)|            Nightlife (58038)|                  Bars (50745)| American (Traditional) (42102)|
    |   ID|   Restaurants (86798)|          Food (38380)|            Nightlife (26733)|                  Bars (25700)| American (Traditional) (18715)|
    |   CA|  Restaurants (167698)|          Food (80123)|            Nightlife (60262)|                  Bars (57473)|     Breakfast & Brunch (42943)|
    |   DE|   Restaurants (40103)|          Food (12848)|            Nightlife (11455)|                  Bars (11300)|  American (Traditional) (7936)|
    |   MO|  Restaurants (273446)|         Food (117941)|            Nightlife (99756)|                  Bars (92158)| American (Traditional) (60164)|
    |   IL|   Restaurants (30350)|          Food (10197)|American (Traditional) (9799)|              Nightlife (8129)|                    Bars (7998)|
    |   IN|  Restaurants (263387)|         Food (105585)|            Nightlife (94689)|                  Bars (89811)| American (Traditional) (66912)|
    |   TN|  Restaurants (356984)|    Nightlife (154097)|                Bars (142820)|                 Food (135582)|American (Traditional) (115791)|
    |   PA|  Restaurants (836680)|         Food (316944)|           Nightlife (261678)|                 Bars (247768)|        American (New) (167425)|
    |   AB|   Restaurants (54256)|          Food (20484)|            Nightlife (13849)|                  Bars (12905)|               Shopping (10407)|
    |   FL|  Restaurants (650084)|         Food (250857)|           Nightlife (211828)|                 Bars (204929)|American (Traditional) (161324)|
    +-----+----------------------+----------------------+-----------------------------+------------------------------+-------------------------------+
    


##### Top 5 longest consecutive reviewing users and reviewed business

Let us find out who keep sending reviews every day:


```python
# Fetch the user's id with its
user_review = review \
    .sort('date') \
    .select('review_id', 'user_id', 'text', F.to_date(F.date_format(col("date"), "yyyy-MM-dd kk:hh:ss")).alias("date")) \
    .groupby('user_id', 'date').agg(
        F.last('text').alias('latest_review')) 
    # For processing timestamp: https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/
result = user_review \
    .withColumn('date_id', 
                F.row_number().over(Window.partitionBy("user_id").orderBy(col("date")))) \
    .withColumn('consecutive_id', col('date') - col('date_id')) \
    .groupby('user_id', 'consecutive_id') \
    .agg(F.count('date_id').alias('# of consecutive days'),
         F.first('date').alias('start'),
         F.last('date').alias('end')
        ) \
    .drop('consecutive_id') \
    .sort(col('# of consecutive days').desc()) \
    .limit(5).rdd.toDF()
```

                                                                                    


```python
user.select('user_id', 'name', 'yelping_since') \
    .join(result, on='user_id') \
    .join(user_review.withColumnRenamed('date', 'end'), on=['user_id', 'end']) \
    .sort(col('# of consecutive days').desc()) \
    .select('name', 'yelping_since', '# of consecutive days', 'start', 'end', 'latest_review') \
    .show(truncate = 60)
```

    [Stage 169:=================================>                    (42 + 16) / 67]

    +---------+-------------------+---------------------+----------+----------+------------------------------------------------------------+
    |     name|      yelping_since|# of consecutive days|     start|       end|                                               latest_review|
    +---------+-------------------+---------------------+----------+----------+------------------------------------------------------------+
    |Christina|2012-10-08 01:25:31|                   51|2017-05-28|2017-07-17|You can purchase products and support a better world at t...|
    |    Danan|2010-04-09 05:28:01|                   31|2012-03-24|2012-04-23|Stopped in to grab a dozen bagels this morning and was ki...|
    |    Brett|2012-01-11 17:19:55|                   29|2019-05-09|2019-06-06|Why 5 stars? Because you need to go straight for the Big ...|
    |     Niki|2014-12-15 03:33:23|                   29|2018-03-11|2018-04-08|Bazbeaux is the perfect mix of specialty pizza, while als...|
    |     Abby|2008-05-09 13:48:14|                   28|2010-02-11|2010-03-10|St. Louis is lucky to have such a in-depth publication on...|
    +---------+-------------------+---------------------+----------+----------+------------------------------------------------------------+
    


                                                                                    

And.. what are those businesses being reviewed!


```python
# Fetch the business's id with its
business_review = review \
    .sort('date') \
    .select('review_id', 'business_id', 'text', F.to_date(F.date_format(col("date"), "yyyy-MM-dd kk:hh:ss")).alias("date")) \
    .groupby('business_id', 'date').agg(
        F.last('text').alias('latest_review')) 
```


```python
result_biz = business_review \
    .withColumn('date_id', 
                F.row_number().over(Window.partitionBy("business_id").orderBy(col("date")))) \
    .withColumn('consecutive_id', col('date') - col('date_id')) \
    .groupby('business_id', 'consecutive_id') \
    .agg(F.count('date_id').alias('# of consecutive days'),
         F.first('date').alias('start'),
         F.last('date').alias('end')
        ) \
    .drop('consecutive_id') \
    .sort(col('# of consecutive days').desc()) \
    .limit(5).rdd.toDF()
```

                                                                                    


```python
business.select('business_id', 'name', 'address', 'city', 'state') \
    .join(result_biz, on='business_id') \
    .join(business_review.withColumnRenamed('date', 'end'), on=['business_id', 'end']) \
    .sort(col('# of consecutive days').desc()) \
    .select('name', 'address', 'city', 'state', '# of consecutive days', 'start', 'end', 'latest_review') \
    .show(truncate = 50)
```

    [Stage 198:=============================================>        (56 + 11) / 67]

    +----------------------------------+----------------+-----------+-----+---------------------+----------+----------+--------------------------------------------------+
    |                              name|         address|       city|state|# of consecutive days|     start|       end|                                     latest_review|
    +----------------------------------+----------------+-----------+-----+---------------------+----------+----------+--------------------------------------------------+
    |                      Oceana Grill|    739 Conti St|New Orleans|   LA|                  139|2018-12-12|2019-04-29|A MAZ ING!  Recommended by cab driver and could...|
    |Hattie B’s Hot Chicken - Nashville|  112 19th Ave S|  Nashville|   TN|                  111|2017-07-27|2017-11-14|Hattie B's is a fun restaurant to visit when yo...|
    |                 Acme Oyster House|724 Iberville St|New Orleans|   LA|                   94|2018-05-04|2018-08-05|Oysters on oysters on oysters. Line is long so ...|
    |                      Oceana Grill|    739 Conti St|New Orleans|   LA|                   90|2019-08-23|2019-11-20|Great food, pasta was fresh and food came out q...|
    |                     Café Du Monde|  800 Decatur St|New Orleans|   LA|                   89|2019-12-19|2020-03-16|I know this is touristy thing to do, but we had...|
    +----------------------------------+----------------+-----------+-----+---------------------+----------+----------+--------------------------------------------------+
    


                                                                                    

##### Popular VS Unpopular!

Inspired by the question 5 of our [Project 2](https://github.com/w4111/project2-s22/blob/main/project2.ipynb), lets define 4 categories of businesses! For a given business B, we will use the number of reviews on B as the first metric, and the average stars those reviews give to the B as the second metric. Then we can classify each user as follows:

- High stars, high amount of reviews   (**popular businesses**)
- High stars, low amount of reviews
- Low stars, high amount of reviews
- Low stars, low amount of reviews  (**unpopular businesses**)

We define the stars and amount of reviews to be high or low based on the rules below:
   
- 1) If `avg stars of valid review < avg(avg stars of valid review of all business in its state)` -> low for the user, else it is considered high.
    
    -  A `valid` review are reviews sent by `active` users. An `active` user is user that (1) registered in 2021 or later (2) registered before 2021 but sent at least 1 review every year since he/she registered.
    
    - A `active` restaurants are those restaurant with at least 1 valid review.
    
- 2) Similarlly for high but `>=`

Then, we also calculate the `unpopular_popular` ratio for each states!


```python
# Check data distribution across all years
review.select(F.year(F.date_format(col("date"), "yyyy-MM-dd kk:hh:ss")).alias("year")) \
    .groupby('year') \
    .count() \
    .sort('year') \
    .show()
```

    [Stage 199:=================================================>     (58 + 6) / 64]

    +----+------+
    |year| count|
    +----+------+
    |2005|   854|
    |2006|  3853|
    |2007| 15363|
    |2008| 48226|
    |2009| 74387|
    |2010|138587|
    |2011|230813|
    |2012|286570|
    |2013|383950|
    |2014|522275|
    |2015|688415|
    |2016|758882|
    |2017|820048|
    |2018|906362|
    |2019|907284|
    |2020|554557|
    |2021|618189|
    |2022| 31665|
    +----+------+
    


                                                                                    


```python
# Calculate 'age' of users
user_age = user.select('user_id', (2021 - F.year(F.date_format(col("yelping_since"), "yyyy-MM-dd kk:hh:ss"))).alias("user_age"))
user_age.show()
```

    +--------------------+--------+
    |             user_id|user_age|
    +--------------------+--------+
    |qVc8ODYU5SZjKXVBg...|      14|
    |j14WgRoU_-2ZE1aw1...|      12|
    |2WnXYQFK0hXEoTxPt...|      13|
    |SZDeASXq7o05mMNLs...|      16|
    |hA5lMy-EnncsH4JoR...|      14|
    |q_QQ5kBBwlCcbL1s4...|      16|
    |cxuxXkcihfCbqt5By...|      12|
    |E9kcWJdJUHuTKfQur...|      13|
    |lO1iq-f75hnPNZkTy...|      13|
    |AUi8MPWJ0mLkMfwbu...|      11|
    |iYzhPPqnrjJkg1JHZ...|      11|
    |xoZvMJPDW6Q9pDAXI...|      12|
    |vVukUtqoLF5BvH_Vt...|      10|
    |_crIokUeTCHVK_JVO...|      12|
    |1McG5Rn_UDkmlkZOr...|      12|
    |SgiBkhXeqIKl1PlFp...|      15|
    |fJZO_skqpnhk1kvom...|      13|
    |x7YtLnBW2dUnrrpwa...|      11|
    |QF1Kuhs8iwLWANNZx...|      12|
    |VcLRGCG_VbAo8MxOm...|      12|
    +--------------------+--------+
    only showing top 20 rows
    


- For those registered in 2021 and 2022, their `age` will be 0 and 1! Therefore if they have reviewed a restaurant since then, they are active user because `1 >= 1` and `1 >= 0`!


```python
# Fetch activate users!
active_user = review \
    .select('user_id', F.year(F.date_format(col("date"), "yyyy-MM-dd kk:hh:ss")).alias("year")) \
    .groupby('user_id') \
    .agg(F.countDistinct('year').alias('n_reviewed_year')) \
    .join(user_age, on='user_id') \
    .filter(col('n_reviewed_year') >= col('user_age')).rdd.toDF()
active_user.show()
```

    [Stage 213:>                                                        (0 + 1) / 1]

    +--------------------+---------------+--------+
    |             user_id|n_reviewed_year|user_age|
    +--------------------+---------------+--------+
    |FlXBpK_YZxLo27jcM...|             11|      10|
    |s7cUp9ma9h9FYN-fa...|             12|      11|
    |OrnQ04JCGie8Q2ymF...|             11|      11|
    |Lrk7Q6eJcu1nyDdW0...|             10|      10|
    |wnrHhjDpQk5uLUK7I...|             12|      12|
    |fD14WAdRrqOnGJQBg...|             11|      11|
    |8M4_nA8e9VacrirvC...|             15|      13|
    |nzfx0ElyVk7dq4vdT...|              9|       9|
    |pou3BbKsIozfH50rx...|             10|       9|
    |gFVLJRSTeHIQs97vI...|             10|      10|
    |jv0KSBZkHoojy6RkI...|             14|      13|
    |Hxx8FmhpxiugIrGRh...|             12|      11|
    |wzyGNvArcpUjZauRW...|             11|      10|
    |2iS1vg5TYpV_iEiNC...|             12|      12|
    |hBgbpWZJHS-y28xfg...|             11|      10|
    |mKBl4fAqTfNts7B78...|             13|      12|
    |upQwDazroow3rjNMW...|             11|      10|
    |Vc4QSYKAOf8NXriGX...|             12|      10|
    |UXTsDU3DJLW380wgc...|             15|      13|
    |kHmXUEOAsIbguUrPo...|             13|      12|
    +--------------------+---------------+--------+
    only showing top 20 rows
    


                                                                                    


```python
valid_reviews = review.join(active_user, on='user_id').join(business.select('business_id', 'state'), on='business_id').rdd.toDF()
state_benchmark = valid_reviews \
    .groupby('state') \
    .agg((F.count('review_id') / F.countDistinct('business_id')).alias("benchmark # of valid reviews"),
          F.mean('stars').alias("benchmark average Stars")).rdd.toDF()
state_benchmark.show()
```

                                                                                    

    +-----+----------------------------+-----------------------+
    |state|benchmark # of valid reviews|benchmark average Stars|
    +-----+----------------------------+-----------------------+
    |   AZ|           9.371335041275263|     3.6280698024087603|
    |   LA|          10.678437265214125|      3.834362907197636|
    |   NJ|          7.3678122934567085|      3.575016819914779|
    |   NV|           12.06864530225783|      3.723359635491317|
    |   ID|           6.330405405405405|      3.618475824527698|
    |   CA|           7.552803129074316|     3.8847315725876057|
    |   DE|            6.63151207115629|      3.496455259628281|
    |   MO|          10.214642956376771|     3.7917715062903774|
    |   IL|           5.996662216288384|      3.475342313258377|
    |   IN|          11.881699426199487|      3.895101877254092|
    |   TN|            8.61311475409836|     3.7037861263213374|
    |   PA|           9.597779866612953|     3.7065487072342647|
    |   AB|           4.683073557560842|     3.6973607380590914|
    |   FL|          10.705234448756949|      3.749492910546481|
    +-----+----------------------------+-----------------------+
    


- The benchmark to determine low and high!


```python
business_scored = valid_reviews.groupby('business_id', 'state') \
    .agg((F.count('review_id')).alias("# of valid reviews"),
          F.mean('stars').alias("average Stars")) \
    .join(state_benchmark, on='state').rdd.toDF()
```

                                                                                    


```python
pop_biz_by_state = business_scored \
    .filter((col('# of valid reviews') >= col('benchmark # of valid reviews')) &
            (col('average Stars') >= col('benchmark average stars'))) \
    .groupby('state') \
    .agg(F.count('business_id').alias('n_pop_biz')).rdd.toDF()
```


```python
unpop_biz_by_state = business_scored \
    .filter((col('# of valid reviews') < col('benchmark # of valid reviews')) &
            (col('average Stars') < col('benchmark average stars'))) \
    .groupby('state') \
    .agg(F.count('business_id').alias('n_unpop_biz')).rdd.toDF()
```


```python
pop_biz_by_state \
    .join(unpop_biz_by_state, on='state') \
    .withColumn('Ratio (%)', F.lit(100) * col('n_pop_biz') / (col('n_pop_biz') + col('n_unpop_biz'))) \
    .withColumn('Ratio (%)', F.format_number(col('Ratio (%)'), 2)) \
    .sort(col('Ratio (%)').desc()) \
    .show()
```

    +-----+---------+-----------+---------+
    |state|n_pop_biz|n_unpop_biz|Ratio (%)|
    +-----+---------+-----------+---------+
    |   AB|      676|       1235|    35.37|
    |   CA|      423|        805|    34.45|
    |   IL|      250|        533|    31.93|
    |   DE|      239|        572|    29.47|
    |   ID|      416|       1036|    28.65|
    |   NV|      714|       1792|    28.49|
    |   LA|      943|       2373|    28.44|
    |   IN|     1179|       3029|    28.02|
    |   NJ|      883|       2294|    27.79|
    |   PA|     3009|       8374|    26.43|
    |   FL|     2522|       7082|    26.26|
    |   TN|     1086|       3062|    26.18|
    |   AZ|      873|       2472|    26.10|
    |   MO|      938|       2793|    25.14|
    +-----+---------+-----------+---------+
    


- Looks like `AB` and `CA` have a larger proportion of unpopular businesses!

##### Uniquely frequent work for each rating

What are those unique word in each rating? Like 'worse' is most likely to have it in 0 star. We will implement this use a simplified tf-idf.

**Warm-up question: how many words people write?**


```python
review_words = review \
    .select('review_id', 'stars', 'text') \
    .withColumn('raw_word', F.explode(F.split("text", " |[\n\t]"))) \
    .withColumn('word', F.regexp_replace(col('raw_word'), r'[^a-zA-Z0-9]', '')) \
    .filter(col('word') != '') \
    .withColumn('word', F.lower('word')).cache()
review_words.count()
```

    729344595



- Yes!! The dataset has 729 million words.. Image working with it in pandas...


```python
%%time
review_words \
    .groupby('review_id', 'stars') \
    .agg(F.count(F.lit('1')).alias('# of words')) \
    .groupby('stars') \
    .agg(F.countDistinct(col('review_id')).alias('# of reviews'),
         F.mean(col('# of words')).alias('Average # of words')) \
    .sort(col('stars').desc()) \
    .show()
```

    [Stage 431:=====================================================> (62 + 2) / 64]

    +-----+------------+------------------+
    |stars|# of reviews|Average # of words|
    +-----+------------+------------------+
    |  5.0|     3231567| 84.12486109679917|
    |  4.0|     1452867|107.55910072979839|
    |  3.0|      691910|123.70078044832421|
    |  2.0|      544225|133.84700996830355|
    |  1.0|     1069530|133.50509663123054|
    +-----+------------+------------------+
    
    CPU times: user 8.77 ms, sys: 0 ns, total: 8.77 ms
    Wall time: 3.62 s


                                                                                    

- This is easy and fast! And isn't that interesting people tend to write more words when they are emotionally give 1/2 rates!

**Actually working on it!**

In the every beginning, I cound each word by 

```python
document_frequency = review_words \
    .groupby('word') \
    .agg(F.count('word').alias('doc_freq')).rdd.toDF()
```

But this won't work because of the data skew! The Spark is assigning 1636982 different words of the entire 729344595 words to each group!

This leads to huge data skew!! I didn't realize this until I request a 100 CPU quota and set up an extremly powerful cluster, but get nothing for quite a while... Some links that help me realize that:

- https://stackoverflow.com/questions/63611463/pyspark-groupby-on-large-dataframe/63616739#63616739.
- https://nealanalytics.com/blog/databricks-spark-jobs-optimization-techniques-shuffle-partition-technique-part-1/
- https://stackoverflow.com/questions/65413065/pyspark-groupby-multiple-columns-count-performance

Then I break the group by stages into two, first `.groupby('review_id', 'stars')` then `.groupby('word')` This way data is added up per review before grouping on each single words. And this time the review_id is sorted when we explode those rows! After that summing words count in each review is actually workable.


```python
spark.conf.set("spark.sql.shuffle.partitions", 50)
```


```python
term_freq_by_review = review_words \
    .repartition('review_id') \
    .groupby('review_id', 'stars', 'word') \
    .agg(F.count(F.lit('1')).alias('term_freq_by_review')).cache()
term_freq_by_review.count()
```

    488914278




```python
document_frequency = term_freq_by_review \
    .repartition('word') \
    .groupby('word') \
    .agg(F.sum('term_freq_by_review').alias('doc_freq')).cache()
document_frequency.count()
```

    1636982




```python
document_frequency.sort(col('doc_freq')).show(5, truncate=100)
```

    +-------------+--------+
    |         word|doc_freq|
    +-------------+--------+
    |   japanneeds|       1|
    |        21611|       1|
    |monthsceiling|       1|
    |lackedseveral|       1|
    |    peterstry|       1|
    +-------------+--------+
    only showing top 5 rows
    


- I don't know what they are... Maybe it is because writers forget to put in space. We definitely do not want them!


```python
review_words.filter(col('word')=='levelplus').show()
```

    [Stage 327:=====================================================> (38 + 1) / 39]

    +--------------------+-----+--------------------+------------+---------+
    |           review_id|stars|                text|    raw_word|     word|
    +--------------------+-----+--------------------+------------+---------+
    |edVshZCpGlyRT9lXw...|  2.0|I stayed here rec...|level...plus|levelplus|
    +--------------------+-----+--------------------+------------+---------+
    


                                                                                    


```python
document_frequency.sort(col('doc_freq').desc()).show(5)
```

    +----+--------+
    |word|doc_freq|
    +----+--------+
    | the|36709968|
    | and|26129544|
    |   i|18982270|
    |   a|18798244|
    |  to|17717798|
    +----+--------+
    only showing top 5 rows
    


- We don't want these words either!


```python
term_frequency = review_words \
    .groupby('stars', 'word') \
    .agg(F.count('word').alias('term_freq')).cache()
term_frequency.count()
```

    2596782




```python
result_long = term_frequency.join(document_frequency.filter(col('doc_freq')>50000), on='word') \
    .withColumn('tf-idf', col('term_freq') / col('doc_freq')) \
    .sort(col('tf-idf').desc()) \
    .withColumn('tf-idf_rank', 
                F.row_number().over(Window.partitionBy("stars").orderBy(col("tf-idf").desc()))) \
    .filter(col('tf-idf_rank') <= 10).rdd.toDF()
result_long.show(10)
```

    +--------+-----+---------+--------+------------------+-----------+
    |    word|stars|term_freq|doc_freq|            tf-idf|tf-idf_rank|
    +--------+-----+---------+--------+------------------+-----------+
    |  refund|  1.0|    46836|   56663|0.8265711310731871|          1|
    |   worst|  1.0|   141195|  184455| 0.765471253151175|          2|
    |horrible|  1.0|   114310|  158146|0.7228130967586913|          3|
    |    rude|  1.0|   145212|  206809|0.7021551286452717|          4|
    |    zero|  1.0|    37294|   53880|0.6921677802524128|          5|
    |   waste|  1.0|    48875|   72891|0.6705217379374683|          6|
    |response|  1.0|    34436|   53734|0.6408605352290915|          7|
    |terrible|  1.0|   103234|  164012|0.6294295539350779|          8|
    |   awful|  1.0|    54318|   87650|0.6197147746719909|          9|
    | charged|  1.0|    56419|   96012|0.5876244636087156|         10|
    +--------+-----+---------+--------+------------------+-----------+
    only showing top 10 rows
    



```python
# Get wide table
result_wide = result_long \
    .withColumn('td-idf_info', 
                F.concat(col("word"), 
                         F.lit(' ('), 
                         F.format_number(col("tf-idf") * 100, 1), 
                         F.lit('%)'))) \
    .groupby('stars') \
    .pivot("tf-idf_rank").agg(F.first("td-idf_info")) \
    .sort(col('stars').desc()) 
# Rename the columns
result_wide = result_wide.select([col('stars')] + [col(rank).alias(f'NO.{rank} td-idf') for rank in result_wide.columns[1:]])
result_wide.show(50, truncate = 100)
```

    +-----+------------------+--------------+---------------------+---------------------+------------------+---------------------+----------------+--------------------+-----------------+----------------+
    |stars|       NO.1 td-idf|   NO.2 td-idf|          NO.3 td-idf|          NO.4 td-idf|       NO.5 td-idf|          NO.6 td-idf|     NO.7 td-idf|         NO.8 td-idf|      NO.9 td-idf|    NO.10 td-idf|
    +-----+------------------+--------------+---------------------+---------------------+------------------+---------------------+----------------+--------------------+-----------------+----------------+
    |  5.0|incredible (79.2%)|highly (79.0%)|          gem (78.5%)|knowledgeable (77.0%)|phenomenal (76.5%)|      amazing (76.2%)|   thank (74.1%)|professional (73.5%)|wonderful (72.4%)|fabulous (71.8%)|
    |  4.0|     solid (45.0%)|pricey (40.0%)|    complaint (39.6%)|        tasty (38.2%)|    casual (37.6%)|      enjoyed (37.5%)|     bit (37.2%)|       liked (36.8%)|      fan (36.4%)| crowded (35.9%)|
    |  3.0|   average (34.0%)|  okay (31.5%)|       decent (30.6%)|           ok (30.2%)|        35 (27.9%)|        bland (26.0%)|mediocre (24.9%)|       kinda (24.2%)|   rating (23.0%)| however (22.9%)|
    |  2.0|  mediocre (34.7%)| bland (31.7%)|disappointing (30.9%)|   overpriced (27.0%)|    barely (24.5%)|unfortunately (22.2%)|      ok (22.0%)|        poor (21.9%)|      dry (21.1%)|     sad (20.9%)|
    |  1.0|    refund (82.7%)| worst (76.5%)|     horrible (72.3%)|         rude (70.2%)|      zero (69.2%)|        waste (67.1%)|response (64.1%)|    terrible (62.9%)|    awful (62.0%)| charged (58.8%)|
    +-----+------------------+--------------+---------------------+---------------------+------------------+---------------------+----------------+--------------------+-----------------+----------------+
    


- It works!! incredible is difinitely only for 5 stars and awful is for 1. And like the 35 is because we remove `/` from `3/5`
