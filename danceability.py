
# coding: utf-8

# In[1]:


#General imports and file-reading support.
from hdf5_getters import * 
import os
import sys
import glob
import numpy as np

##JustSparkThings.
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Row

def dir_explorer(root): 
    file_paths = []
    walker = os.walk(root)
    for (path, dirnames, filenames) in walker:
        for filename in filenames:
            file_paths.append(os.path.join(path, filename))
    return file_paths

def get_title_year_danceability(filename):
    f = open_h5_file_read(filename)
    title = get_title(f)
    danceability = get_danceability(f)
    year = get_year(f)
    return (title.decode('utf-8'), year, danceability)
    
    
conf = SparkConf().setMaster('local').setAppName('Danceability')
sc = SparkContext(conf=conf)
spark = SparkSession     .builder     .master("local[2]")     .appName("Danceability")     .getOrCreate()


# In[2]:


root_directory = "/home/falapen/Skola/LDSA/Project/Data/A"
file_paths = dir_explorer(root_directory)


# In[3]:


filepath_rdd = sc.parallelize(file_paths)
song_rdd = filepath_rdd.map(get_title_year_danceability)


# In[4]:


fields = [StructField("title", StringType(), True), StructField("year", IntegerType(), True), StructField("danceability", FloatType(), True)]
schema = StructType(fields)
songs = song_rdd.map(lambda x: Row(title=x[0], year=int(x[1]), danceability=float(x[2])))
df = spark.createDataFrame(songs, schema).cache()


# In[ ]:


df.take(2)

