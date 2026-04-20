import polars as pl 
import matplotlib as plt 
from pathlib import Path 

# file path absolute 
al_path = Path("data/access_logs") 
# get all csv files from folder path 
al_files = [file for file in folderPath.rglob("*") if p.is_file()]

# read in csv 
for file in al_files: 
    pl.read_csv(file)
