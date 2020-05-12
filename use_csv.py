# -*- coding: utf-8 -*-
"""
Created on Tue May 12 15:03:43 2020

@author: metec
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 12 13:48:11 2020

@author: metec
"""
import findspark
findspark.init(spark_home = "C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('SPARK SQL').getOrCreate()

def read_csv():
    df = spark.read.csv("datasets/goodbooks-10k/ratings.csv", header = True)
    
    df = df.withColumn("rating", df["rating"].cast(IntegerType()))
    
    return df
    
if __name__ == "__main__":
    df = read_csv()
    
    print("investigate the data by showing top 20 rows")
    df.show()
    
    print("Show the number of ratings per book and order by count higher to lower")
    rating_count_per_book = df.groupBy(df["book_id"]).count().orderBy("count", ascending = False)
    rating_count_per_book.show()
    
    print("Show the number of ratings per book which have book_id less than 500")
    rating_count_per_bookid_lr_500 = df.filter(df["book_id"] < 500).groupBy("book_id").count()\
    .orderBy(["book_id", "count"], ascending = False)
    rating_count_per_bookid_lr_500.show()
    
    print("Show the number of ratings per book which have total num. of ratings less than 50")
    rating_count_per_book_sm_10 = df.groupBy(df["book_id"]).count()
    rating_count_per_book_sm_10 = rating_count_per_book_sm_10.filter(rating_count_per_book_sm_10["count"] < 50)\
    .orderBy("count", ascending = False)
    rating_count_per_book_sm_10.show()
    
    print("Show the count of the above resulting df rating_count_per_book_sm_10")
    count_rat_count_per_book_sm_10 = rating_count_per_book_sm_10.count()
    print(str(count_rat_count_per_book_sm_10))
    
    
    print("For each book id get the total sum of the ratings then order by sum(rating) and book_id")
    df.groupBy("book_id").sum("rating").orderBy(["sum(rating)", "book_id"], ascending = False).show()
    
    
    