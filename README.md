# DLDataGen

This project provides scripts two scripts to generate data for experiments need, especially in a context of data lakes.
The first script, "DocumentDataGen.py" allows to download (long) textual documents from the HAL repository. 
The second script, "TabularDataGen.py" allows to extract tabular files from a base SQLite database.

## "DocumentDataGen.py"

### Dependencies
The script needs the following python packages to run effectively: tqdm, pandas, argopt, joblib, unidecode and requests

### Usage
To generate documents, type 'python DocumentDataGen.py -l <limit> -j <n_jobs>', with limit the number of thousands of documents to generate, and n_jobs the number
of processor cores to use. 
  
For example, to generate 10000 documents using 5 cores, you have to enter 'python -l 10 -j 5'. 

The script automatically creates a 'data' folder where all generated data are stored. It also provides a CSV catalogue of basic metadata (title, language, path, institution) 
for on all generated files. 

## "TabularDataGen.py"

### Dependencies

### Usage
