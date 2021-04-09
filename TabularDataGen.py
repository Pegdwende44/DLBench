#Created by Pegdwende Sawadogo,
# sawadogonicholas44@gmail.com




import sys, getopt
import os
import subprocess
from tqdm import tqdm
import pandas as pd
from argopt import argopt
import re
from time import time, ctime
import glob
import shutil
import sqlalchemy
from sqlalchemy import inspect





def build_tables_list(scalefactor=1):
	tables = []
	output_folder = os.getcwd()
	benchmark_uri = 'sqlite:///'+output_folder+'/benchmark.sqlite' 
	data_folder = output_folder+"/data"

	#Preprocessing
	try:
		if not os.path.exists(output_folder+"/data"): #create folder if not exists
			os.makedirs(output_folder+"/data")
		else: #empty folder if exists
			shutil.rmtree(output_folder+"/data")
			os.makedirs(output_folder+"/data")
	except:
		print(f"Could not create or empty output folder {output_folder}/data")
		sys.exit()
		
	#Get list of tables
	try:
		benchmark_eng = sqlalchemy.create_engine(benchmark_uri, echo = False)
		benchmark_insp = inspect(benchmark_eng)
		temp = benchmark_insp.get_table_names()
		liste_catalogue = []
		for i, t in enumerate(temp):
			if (i % 5) < scalefactor and (len(tables)< scalefactor*1000):
				tables.append(t)
				liste_catalogue.append([data_folder+'/'+t+'.csv',t, 2010 + i%11, 1+i%12])
		df_catalogue = pd.DataFrame(liste_catalogue, columns=["path","name","year","month"])
		df_catalogue.to_csv(output_folder+'/catalogue.csv', index=False) 
		print("FINISHED WITH %d TABLES FOUND" % len(tables))
		return tables, benchmark_eng, data_folder
	except Exception as e:
		print("Error while loading tables from benchmark")
		print(e)
		sys.exit()
		




def download_file(table, benchmark_eng, data_folder):
	print('-'*15)
	
	table_name = table
	try:
		print(f"Exporting table with name {table_name}")
		#load table
		table_df = pd.read_sql_table(table_name, con=benchmark_eng) 
		#Update (rename) columns
		col_olds = table_df.columns
		col_news = []
		for col_old in col_olds:
			col_news.append(col_old.replace("\\","_").replace("\n","_").replace("é", "e").replace("\i", "i").replace('�','e').replace("'","_"))
		table_df.columns = col_news
		# save base table
		table_df.to_csv(data_folder+'/'+table_name+'.csv', index=False) 
		return 1
	except KeyboardInterrupt:
		print("Keyboard interrupt exception caught")
		sys.exit()
	except:
		print(f"Could not extract table with name {table_name}")
		print(ValueError)
		return 0




def extract_tabular_files(tables, benchmark_eng, data_folder):
	
	
	succes_downloaded = []
	for table in tqdm(tables):
		succes_downloaded.append(download_file(table, benchmark_eng, data_folder))

	print(f"Successfully extracted {sum(succes_downloaded)} of {len(succes_downloaded)} files")








def main(argv):
	#default parameters
	LIMIT = 1


	#get custom parameters
	try:
		opts, args = getopt.getopt(argv,"S:")
	except getopt.GetoptError:
		print('TabularDataGen.py -S <scale> ')
		sys.exit()
	try:
		for opt, arg in opts:
			if opt == '-S':
				LIMIT = int(arg)
				#LIMIT OKAY
			else:
				print('TabularDataGen.py -S <scale>')
				sys.exit()
	except:
		print('TabularDataGen.py -S <scale>')
		sys.exit()
	
	if (LIMIT > 5 or LIMIT < 1) :
		print("Invalid parameter: Must have 'S' parameter between 1 and 5.")
		sys.exit()
	start = time()



	#build catalogue
	tables, benchmark_eng, data_folder = build_tables_list(scalefactor=LIMIT)
	print("LIST BUILT...")
	#download data
	extract_tabular_files(tables, benchmark_eng, data_folder)

	done = time()
	elapsed = done - start
	print("*"*20)
	print("STARTED : "+ctime(start) )
	print("END : "+ctime(done) )
	print("TIME ELAPSED:" + str(elapsed/60) + " MINUTES" )

if __name__ == "__main__":
	main(sys.argv[1:])
