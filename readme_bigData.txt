1.Word Count Program Using PySpark ?

spark=SparkSession.builder.master("local").appName("wordCount").getOrCreate()
sc=spark.sparkContext
text_file=sc.textFile("abc.txt")
counts=textfile.flatMap(lamda line:line.split(" ")).map(lamda word:(word,1)).reduceByKey(lamda x,y:x+y)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))


---------------------------------------------------------------------------------------------
2.Determine if a string is a permutation of another string

'Nib', 'bin' -> False
'act', 'cat' -> True
'a ct', 'ca t' -> True
'Abhishek', 'bhAishek' ->True

def anagrams(inp1, inp2):
    if len(inp1) != len(inp2):
        return False

    counts = {}
    for c1,c2 in zip(inp1,inp2):
             if c1 in counts.keys():
                 counts[c1]+=1
             else:
                 counts[c1]=1
             if c2 in counts.keys():
                 counts[c2] -= 1
             else:
                 counts[c2] = -1



    for count in counts.values():
             if count!=0:
                 return False


    return True


def main():
    inp1 = "silents"
    inp2 = "listens"
    if anagrams(inp1,inp2):
        print("equal")
    else:
        print("not equal")



if  __name__ =="__main__":
    main()
------------------------------------------------------------------
3.advantage of immutable ?

Immutable data is definitely safe to share across processes.
Immutable data can as easily live in memory as on disk.
and Immutable allow to makes recreating the RDD parts possible at any given instance
------------------------------------------------------------------------
4. Logical plan for  below two query ?

a.spark.sql("select count(*) from table_name").show()
b.spark.sql("select * from titanic_csv").show()
Answer: count(*) will require Exchange of data between the Executors.
-----------------------------------------------------------------------
5.what is garbage collector name in spark and how to tune it ?

Answer: the Garbage-First GC (G1 GC). the G1 collector aims to achieve both high
throughput and low latency. Try the G1GC garbage collector with -XX:+UseG1GC

---------------------------------------------------------------------------

6.what is managed and external table in hive ?

Answer: Managed tables are Hive tables where the entire lifecycle of the tables data are managed
and controlled by Hive.
External tables are tables where Hive has loose coupling with the data.

-----------------------------------------------------------------------------
paypal

python
1.difference  between list and tuple

The primary difference between tuples and lists is that tuples are immutable as opposed to
 lists which are mutable.

2.how to connect db with python code.

import mysql.connector

mydb = mysql.connector.connect(
    host = "localhost",
    user = "yourusername",0
    password = "your_password"
)
cursor = mydb.cursor()
cursor.execute("CREATE DATABASE geeksforgeeks")



3.how to merge to different dictionary data in python
merged_dict = dict_one | dict_two | dict_three


4.few values were assigned to a variable list a, after assinging it to another and appenindg few values into it,what will be the final o/p

sql===
two tables of employee having employee id as column and coupon code as
column.need to assign each employee code them and how to use row_number()



---------------------
digit88

1.Performance issue in the spark application and how you can improve it.
2.salting in spark
  In Spark, SALT is a technique that adds random values to push Spark partition data evenly

3.Increase or decrease spark partitions dynamically

4.DAG AND LINEAGE GRAPH

5.ipl database table design
 player
 Team
 session
 ball_by_ball
 match

-------------------------------------------
citi

1.how many difference database connection is going to create for n different and n partitions executor ?
  As per  shashank only 1 db connection only
  numPartitions
  The maximum number of partitions that can be used for parallelism in table reading and writing.
  This also determines the maximum number of concurrent JDBC connections.
  If the number of partitions to write exceeds this limit,
  we decrease it to this limit by calling coalesce(numPartitions) before writing.

2.how the broadcast is working and who require the memory ?
  Broadcast variables do occupy memory on all executors.
  Broadcast variables allow the programmer to keep a
  read-only variable cached on each machine rather than shipping a copy of it with tasks
  The data broadcasted this way is cached in serialized form and deserialized before running each task

3.case class in scala ?
  Scala case classes are just regular classes which are immutable by default and
  decomposable through pattern matching.
  It uses equal method to compare instance structurally. It does not use new keyword to instantiate object.
  All the parameters listed in the case class are public and immutable by default
  case class className(parameters)

4.difference between RDD & DATAFRAME & DATASET.


5.how you can identify the skew data.
  Usually, in Apache Spark,
  data skewness is caused by transformations that change data partitioning like join, groupBy, and orderBy.
  For example, joining on a key that is not evenly distributed across the cluster,
  causing some partitions to be very large and not allowing Spark to process data in parallel

6.define your own schema in spark-->
  Avro is an open source project that provides data serialization and
  data exchange services for Apache Hadoop
  use json , or structType struct field

7.What is the concept of application, job, stage and task in spark?
   When you invoke an action on an RDD, a "job" is created. Jobs are work submitted to Spark.
   Jobs are divided into "stages" based on the shuffle boundary. This can help you understand.
   Each stage is further divided into tasks based on the number of partitions in the RDD.
   So tasks are the smallest units of work for Spark.
----------------------------------
Deloitte
1.How you can delete duplicate record
  We use a SQL ROW_NUMBER function, and it adds a unique sequential row number for the row.

In the following CTE, it partitions the data using the PARTITION BY clause for the
[Firstname], [Lastname] and [Country] column and generates a row number for each row.

WITH CTE([FirstName],
    [LastName],
    [Country],
    DuplicateCount)
AS (SELECT [FirstName],
           [LastName],
           [Country],
           ROW_NUMBER() OVER(PARTITION BY [FirstName],
                                          [LastName],
                                          [Country]
           ORDER BY ID) AS DuplicateCount
    FROM [SampleDB].[dbo].[Employee])
DELETE FROM CTE
WHERE DuplicateCount > 1;
----------------------------------------------
digit88

id date  amount
A 2022-11-30 2000
A 2011-11-22 5000
A 2022-11-28 1000000
---------------------------
A  2022-10-28 1000000
A 2022-10-28 1000000
A 2022- 10 28 400000
-------------------------
A 2022-10-27 50000
A 2022-10-29 400000
A 2022-11-27 400000
------------------------------

for date of 28th every month ,
i want to cmpare the difference of transcation
from previous date to 28th
and
from 28th to 29th
and flafg it as an anomoly if the percentage change is higher than 50

use window function
------------------------------
what is lead and lag functions in sql
lag-->to get the previous record
lead-->to get the next record 
----------------------------------
happiestminds

1.how to call udf function in dataframe
  val random =udf(()=>Math.random())
  spark.udf.register("random",random.asNondetermistic());
  df.withColumn("rand",random())

2.how you can find city wise population max

df==>city,pupulation,year
rdd.map(lamda reg:(reg[0],reg[1])).reduceByKey(lamda v1,v2:(v1 if v1[1]>=v2[1] else v2))
df.groupBy('city').max(population).show()

3.how you can change the datatype of the column
df===>salary
df.withColumn("salary",col("salary").cast("IntegerType"))

4.lead function example

select orderQ ,LEAD(orderQ) OVER(ORDER BY orderQ) DESC FROM SALES.ORDERDETAIL

5.monthly salary of the employee.
SELECT EMP_name,(emp_An_salry/12)as 'monthly_salary',emp_An_salry as 'anual salary' from emp_data

------------------------------------------------------------------
1.remove the duplicate

def remove(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)

    return final_list

2.5th higest salary
  val byDeptOrderByDesc=Window.partitionBy(col("department")).orderBy(col("department"),desc)
    df.withColumn("col5",dense_rank() over byDeptOrderByDesc).filter("col5=5")

3.join two large table then how you can optimize ?
   instead of default shuffle join you can use Sort-merge Join -->Mappers take advantage of co-location of keys
   to do efficient join.

4.List Comprehension
    fruits = ["apple", "banana", "cherry", "kiwi", "mango"]
    newlist = [x for x in fruits if "a" in x]
-----------------------

Identify the max transaction amount done every month per customer without dropping any existing column.


After Identifying the max transaction amount done every month derive a column(prev_transaction)
that shows the previous month transaction for a cutomer  with current month.


---------------------------------------------------------------------------------------


id    dpt_id    salary
-------------------
1    hr        1000
2    fin        1400
3    hr        1800
4    fin        500
5    adm        3000
===================
Write a query to get salary payout from a dept more than 2,000.
select dept_id,sum(salary) from employee_department group by dept_id  having sum(salary)>2000
==============================
scope in build tool
provided and compile
you can use provided scope to remove the dependency in prod for e.g test-
--------------------------------------------------
vowel count

#regex import
import re
df.flatMap()
mapOp=df.flatMap(lambda x:x.split(" ")).map(lamda l:(l,len(re.findall('[aeiou]',1)))).collect()
[('vikram',2),('is',1),('best',1),('umbrella',3)]

TABLE T1 MINUS TABLE T2==>left_semi
select * from (
select distinct(t1.somevalue)
from table t1,t2
where t1.c1=t2.c1
)x
LEFT JOIN
(
select distinct(t1.somevalue)
from table t1,t2
where t1.c1=t2.c1
)y
on x.somevalue=y.somevalue
where y.somevalue is null
--------------------------------------------------
JIO
Plus One
Input: digits = [4,3,2,1]
Output: [4,3,2,2]
Explanation: The array represents the integer 4321.
Incrementing by one gives 4321 + 1 = 4322.
Thus, the result should be [4,3,2,2].
def plus_one(digits):
    n = len(digits)

    while n > 0:
        if digits[n - 1] == 9:
            if (n - 1) == 0:
                digits[n - 1] = 0
                temp = [1] + digits
                return temp
            else:
                digits[n - 1] = 0
                n = n - 1
        else:
            digits[n - 1] += 1
            return digits

    return digits



Problem 1 :
List down all the duplicates character in the string

from collections import Counter
def find_dup_char(input):
        WC = Counter(input)

        for letter, count in WC.items():
            if (count > 1):
                print(letter,count)

if  __name__ =="__main__":
   find_dup_char("This is the reliance industries, we welcome you")


--------------------------------------------------























































