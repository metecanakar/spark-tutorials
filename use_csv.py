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
from pyspark import SparkContext, SparkConf

from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

#just to get the version
sc_conf = SparkConf()
sc = SparkContext(conf=sc_conf)
print(sc.version)

#create a SparkSession
spark = SparkSession.builder.appName('SPARK SQL').getOrCreate()

def read_csvs():
    df_ratings = spark.read.csv("datasets/goodbooks-10k/ratings.csv", header = True)
    
    df_books = spark.read.csv("datasets/goodbooks-10k/books.csv", header = True)
    
    #convert the column rating's data type to integer from string
    df_ratings = df_ratings.withColumn("rating", df_ratings["rating"].cast(IntegerType()))
    
    #show the schema
    df_ratings.printSchema()
    
    return df_ratings, df_books

   
if __name__ == "__main__":
    df_ratings, df_books = read_csvs()
    """
    print("investigate the data by showing top 20 rows")
    df_ratings.show()
    
    print("Case When example: if rating == 5 then output 'rating is 5', otherwise output 'NOT 5'")
    df_ratings.select(df_ratings.book_id, df_ratings.rating, when(df_ratings["rating"] == 5, "rating is 5").otherwise("NOT 5").alias("added_col")).show()
    
    print("Case When example: if rating == 4 OR 5 then output 'rating is 4 OR 5', otherwise output 'NEITHER 4 NOR 5'")
    df_ratings.select(df_ratings.book_id, df_ratings.rating, when((df_ratings["rating"] == 5) | (df_ratings["rating"] == 4), "rating is 4 or 5")
                      .otherwise("NEITHER 4 NOR 5").alias("added_col")).show()
    
    
    print("Show the number of ratings per book and order by count higher to lower")
    rating_count_per_book = df_ratings.groupBy(df_ratings["book_id"]).count().orderBy("count", ascending = False)
    rating_count_per_book.show()
    
    print("Show the number of ratings per book which have book_id less than 500")
    rating_count_per_bookid_lr_500 = df_ratings.filter(df_ratings["book_id"] < 500).groupBy("book_id").count()\
    .orderBy(["book_id", "count"], ascending = False)
    rating_count_per_bookid_lr_500.show()
    
    print("Show the number of ratings per book which have total num. of ratings less than 50")
    rating_count_per_book_sm_10 = df_ratings.groupBy(df_ratings["book_id"]).count()
    rating_count_per_book_sm_10 = rating_count_per_book_sm_10.filter(rating_count_per_book_sm_10["count"] < 50)\
    .orderBy("count", ascending = False)
    rating_count_per_book_sm_10.show()
    
    print("Show the count of the above resulting df rating_count_per_book_sm_10")
    count_rat_count_per_book_sm_10 = rating_count_per_book_sm_10.count()
    print(str(count_rat_count_per_book_sm_10))
    
    print("For each book id get the total sum of the ratings then order by sum(rating) and book_id")
    df_ratings.groupBy("book_id").sum("rating").orderBy(["sum(rating)", "book_id"], ascending = False).show()
    
    print("After filtering books with id smaller than 500 \
          Join 2 dataframes using inner join on book_id columns in both dataframes.\
           Prior to the join filter them via book_id")
    #if there was join on multiple columns: df = df1.join(df2, (df1.x1 == df2.x1) & (df1.x2 == df2.x2))
    joined_book_rating_df = df_ratings.filter(df_ratings["book_id"] < 500)\
    .join(df_books, df_ratings.book_id == df_books.book_id, 'inner').show(1)    
    
    print("After join select only some columns")
    joined_book_rating_df_selected = df_ratings.filter(df_ratings["book_id"] < 500)\
    .join(df_books, df_ratings.book_id == df_books.book_id, 'inner').select(df_ratings["book_id"],
                                                                            "original_title",
                                                                            "user_id",
                                                                            "rating")
    joined_book_rating_df_selected.show(50)
    
    print("group by book_id joined_book_rating_df_selected and sum ratings")
    joined_book_rating_df_selected.groupBy("book_id").sum("rating").\
    orderBy(["sum(rating)", "book_id"], ascending = False).show()
    
    print("group by book_id and user_id joined_book_rating_df_selected and count ratings")
    joined_book_rating_df_selected.groupBy(["book_id", "user_id"]).count().\
    orderBy(["count", "book_id"], ascending = False).show()
    
    
    print("group by book_id and user_id joined_book_rating_df_selected and sum ratings")
    joined_book_rating_df_selected.groupBy(["book_id", "original_title", "user_id"]).sum("rating").\
    orderBy(["sum(rating)", "book_id"], ascending = False).show()
    
    
    #GROUP BY VS WINDOW FUNCTION
    print("GROUP BY:")
    per_book_id_ratings = df_ratings.groupBy("book_id").sum("rating")
    per_book_id_ratings_joined = per_book_id_ratings.join(df_ratings, df_ratings.book_id == per_book_id_ratings.book_id)\
        .orderBy(["sum(rating)", df_ratings["book_id"]], ascending = True)
    per_book_id_ratings_joined.show(100)
    
    """
    
    """
    #this was just to get the book title but not necessary for the window function example
    joined_per_book_id_ratings = per_book_id_ratings.join(df_books, df_books.book_id == per_book_id_ratings.book_id, "inner").\
        select(df_ratings["book_id"],
               "original_title",
               "sum(rating)")
    joined_per_book_id_ratings.show()

    
    print("WINDOW FUNCTION:")
    window_spec = Window.partitionBy("book_id")
    df_ratings.withColumn("sum(rating)", sum("rating").over(window_spec))\
        .orderBy(["sum(rating)", df_ratings["book_id"]], ascending = True).show(100)
        
    
    """
    print("FIRST FILTER 2 tables (book_id = 1 and rating = 1),\
          then join, finally select: ")
    
    
    filtered_books = df_books.filter(df_books["book_id"] == 1)
    filtered_ratings = df_ratings.filter(df_ratings["rating"] == 1)
    joined_data = filtered_books.join(filtered_ratings, filtered_books.book_id == filtered_ratings.book_id, 'left')   
    
    print("Select the column names with old dataframes not the joined dataframe")
    selected_data_old_col_names = joined_data.select(
        filtered_books["book_id"],
        filtered_books["title"],
        filtered_ratings["rating"])
    selected_data_old_col_names.show()
    
    print("Select the column names with the joined dataframe")
    selected_data_new_col_names = joined_data.select(
        filtered_books["book_id"],
        filtered_books["title"],
        filtered_ratings["rating"])
    
    selected_data_new_col_names.show()
    #basically the results are the same
    
    
    
