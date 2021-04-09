# DLBench

This project provides two scripts to generate data for experiments need, especially in a context of data lakes.

The first script, "DocumentDataGen.py" allows to download (long) textual documents from the HAL repository. 

The second script, "TabularDataGen.py" allows to extract tabular files from a SQLite database.

## 1- "DocumentDataGen.py"

### Dependencies
The script needs the following python packages to run effectively: tqdm, pandas, argopt, joblib, unidecode and requests

### Usage
To generate documents, type 'python DocumentDataGen.py -S <scale> -J <nb_jobs>', with "scale" the scale factor and "nb_jobs the number of processor cores to use for distributed run. 
The scale factor goes from 1 to 5, with 10000 documents generate for S=1, and 50000 for S=5.
For example, to generate 30000 documents with a 5-core parallel run, enter "python DocumentDataGen.py -S 3 -J 5'. 

The script automatically creates a 'data' folder where all generated data are stored. It also provides a CSV catalogue of basic metadata (title, language, path, institution)  on all generated files. 

## 2- "TabularDataGen.py"

### Dependencies
The script needs the following python packages to run effectively: tqdm, pandas, argopt and sqlalchemy

In addition to these python packages, you have to put in the same folder the SQLite database from which tabular files are extrected. Such database must be downloaded from https://storage.googleapis.com/table-union-benchmark/large/benchmark.sqlite .

### Usage
To generate tabular files, type 'python TabularDataGen.py -S <scale>', with "scale" the scale factor. The scale factor goes from 1 to 5, with 1000 raw tabular files extracted for S=1, and 5000 for S=5.
  
For example, to generate 2000 tabular files, you have to enter "python TabularDataGen.py -S 2". 

The script automatically creates a 'data' folder where all generated data are stored.  It also provides a CSV catalogue that contains basic metadata descriptions for each extracted file.

