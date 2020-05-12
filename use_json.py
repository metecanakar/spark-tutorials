# -*- coding: utf-8 -*-
"""
Created on Tue May 12 13:48:11 2020

@author: metec
"""
import findspark
findspark.init(spark_home = "C:\spark")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example app').getOrCreate()




def read_json():
    df = spark.read.json("datasets/insanlar.json")
    return df


if __name__ == "__main__":
    df = read_json()
    
    print("see how data looks like by showing the first 5 rows")
    df.show(5)
    
    print("select only the yas column")
    df.select('yas').show(5)
    print("same output as above but using a different way to select the column")
    df.select(df['yas']).show(5)
    
    print("select colums isim and yas while adding 1 to the values of the yas column")
    df.select(df['isim'], df['yas'] + 1).show(5)
    
    print("filter yas smaller than 22 and order by yas")
    df.filter(df['yas'] < 22).orderBy('yas', ascending = False).show(50)
    
    print("Find the number of people for each age and show them ascending by count (lower to higher)")
    df.groupBy("yas").count().orderBy("count").show(30)
    
    print("Find the number of people for each age and show them descending by count (higher to lower)")
    df.groupBy("yas").count().orderBy("count", ascending = False).show(50)
    


