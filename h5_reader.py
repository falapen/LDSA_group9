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

#Taken from the Million song dataset tutorial. 
def apply_to_all_files(basedir,func=lambda x, y: x,ext='.h5', extra_arg=None):
    """
    From a base directory, go through all subdirectories,
    find all files with the given extension, apply the
    given function 'func' to all of them.
    If no 'func' is passed, we do nothing except counting.
    INPUT
       basedir  - base directory of the dataset
       func     - function to apply to all filenames
       ext      - extension, .h5 by default
    RETURN
       number of files
    """
    cnt = 0
    # iterate over all files in all subdirectories
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        # count files
        cnt += len(files)
        # apply function to all files
        for f in files :
            func(f, extra_arg)       
    return cnt
###############################################


def print_songs(filename): 
    """
    Print all song-names from a file. 
    """
    f = open_h5_file_read(filename)
    num_songs = get_num_songs(f)
    #song_names = []
    for i in range(num_songs):
        print(get_title(f, i).decode("utf-8"))
    f.close()

def print_artists(filename): 
    """
    Print all artist from a file. 
    """
    f = open_h5_file_read(filename)
    num_songs = get_num_songs(f)
    previous_artist = ""
    song_count = 0
    #song_names = []
    for i in range(num_songs):
        song_count+=1
        current_artist = get_artist_name(f, i)
        if current_artist != previous_artist:
            print(current_artist.decode("utf-8"), " - ", song_count)
            song_count = 0
        previous_artist = current_artist
    f.close()

def import_song_to_rdd(filename): 
    
    
    
    pass

#Constants
DATA_ROOT = '/home/falapen/Skola/LDSA/Project/Data'

#Initialize some spark things 

spark = SparkSession.builder.master("local[2]").appName("Song analysis").getOrCreate()
df = 


#Actual program
apply_to_all_files(DATA_ROOT, print_artists)