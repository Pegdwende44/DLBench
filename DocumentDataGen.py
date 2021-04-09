#Created by Pegdwende Sawadogo,
# sawadogonicholas44@gmail.com




import sys, getopt
import os
import subprocess
from tqdm import tqdm
import pandas as pd
from argopt import argopt
from joblib import Parallel, delayed
import unidecode
import re
import requests
from time import time, ctime
import glob
import shutil



def get_valid_filename(s):
    s = str(s).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', s)



def build_hal_docs_catalogue(limit=1000):
	output_folder = os.getcwd()
	#val = URLValidator(verify_exists=True)
	try:
		if not os.path.exists(output_folder+"/data"): #create folder if not exists
			os.makedirs(output_folder+"/data")
		else: #empty folder if exists
			shutil.rmtree(output_folder+"/data")
			os.makedirs(output_folder+"/data")
	except:
		print(f"Could not create or empty output folder {output_folder}/data")
		sys.exit()
		return 0
	try:
		consecutive_errors = 0
		current_cursor = ""
		next_cursor="*"
		remaining = limit
		col_names =  ['docid', 'domain', 'submitted_date', 'title', 'file_url', 'inst_country', 'publisher', 'language', 'filepath']
		df  = pd.DataFrame(columns = col_names)
		#Query parameters
		request_url = "https://api.archives-ouvertes.fr/search/"
		request_params = {}
		request_params['q'] = "*:*" #tall possible results"
		request_params['fq'] = ["docType_s:(ART)","submitType_s:file","submittedDateY_i:[2000 TO 2020]","language_s:(en OR fr)" ] #filtering
		request_params['wt'] = "json" #return json results
		request_params['sort'] = "docid asc" #sort
		request_params['fl'] = ['docid',  'domain_s', 'submittedDate_s', 'title_s', 'fileMain_s', 'instStructCountry_s', 'language_s']
		request_params['rows'] = "1000"
        
		print("START MAKING CATALOGUE...")
		while (next_cursor != current_cursor and remaining > 0 and consecutive_errors<10):
			S = requests.Session()
			current_cursor = next_cursor
			request_params['cursorMark'] = current_cursor #cursor
			#query execution
			print("REQUEST...")
			result_temp = S.get(url=request_url, params=request_params)
			result = result_temp.json()
			next_cursor = result.get('nextCursorMark')
			
			#manage results
			if next_cursor == None:
				print("Error while %d docs found" % (limit-remaining))
				return 0
			else:
				docs = result['response']['docs'] 
				for doc in docs: 
					if(remaining > 0):
                       
						doc_dict = {}
						#docid
						doc_dict['docid'] = doc.get('docid')
						#domain_s
						if len(doc.get('domain_s', '')) > 0 :
							doc_dict['domain'] = doc.get('domain_s')[0]
						else:
							doc_dict['domain'] = doc.get('domain_s')
							#submittedDate_s        
						doc_dict['submitted_date'] = doc.get('submittedDate_s')
						#title_s
						if len(doc.get('title_s', '')) > 0:
							doc_dict['title'] = doc.get('title_s')[0]
						else:
							doc_dict['title'] = doc.get('title_s', '')
						#fileMain_s (url)   
						doc_dict['file_url'] = doc.get('fileMain_s', '')
						#structure country
						if len(doc.get('instStructCountry_s', '')) > 0:
							doc_dict['inst_country'] = doc.get('instStructCountry_s')[0]
						else:
							doc_dict['inst_country'] = doc.get('instStructCountry_s', '')
						#language
						if len(doc.get('language_s', '')) > 0:
							doc_dict['language'] = doc.get('language_s')[0]
						else:
							doc_dict['language'] = doc.get('language_s', '')
                        #filepath    
						doc_dict['filepath'] = output_folder+'/data/'+get_valid_filename(doc_dict['domain'])+'/'+ str(doc_dict['docid']) + '.pdf' 

						#Check if file really exists
						try:
							r = requests.head(doc_dict['file_url'])
							if r.status_code == 200: #OK
								df.loc[len(df)] = doc_dict 
								remaining = remaining - 1
								consecutive_errors = 0
							else:
								consecutive_errors = consecutive_errors+1
								print("Error for file with id %d!" %doc_dict['docid'])
						except KeyboardInterrupt:
							print("Keyboard interrupt exception caught")
							sys.exit()
						except:
							consecutive_errors = consecutive_errors+1
							print("File not existing in path  ", doc_dict['file_url'], " ! \n  Lets try next...")
						
				print("REQUEST...OK!")
				print("NOW %d DOCS IN THE CATALOGUE..." % (limit-remaining))
                #export catalogue
		df.to_csv(output_folder+"/catalogue.csv", sep=";", index=False)
		print("FINISHED WITH %d DOCS FOUND" % len(df))
		return 1
	except KeyboardInterrupt:
		print("Keyboard interrupt exception caught")
		sys.exit()
	except:
		print("Error while %d docs found" % (limit-remaining))
		return 0




def download_file(url, id, path):

    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    try:
        print(f"Downloading file with id {id}")
        if os.path.exists(f"{path}"):
            print(f"\tFile with id {id} already exists!")
            return 0
        p = subprocess.Popen(
            ["wget", "--timeout", "10", "--tries", "3", "-O", "{0}".format(path),
             url])
        p.communicate()  # now wait plus that you can send commands to process
        if not p.returncode:
            return 1
        else:
            return 0
    except KeyboardInterrupt:
        print("Keyboard interrupt exception caught")
        sys.exit()
    except:
        # if file is created, delete it
        if os.path.exists(f"{path}"): 
            try: 
            #    #file_size = os.path.getsize(path)
                os.remove(f"{path}")
            except:
             print(f"Unable to delete file with id {id}")
        print(f"Could not download file with id {id}")
        return 0




def download_text_files(limit=-1, n_jobs=1):
	catalogue_path = os.getcwd()+"/catalogue.csv"
	catalogue = pd.read_csv(catalogue_path, sep=";").sample(frac=1) #in any order
	# limit number of files to download 
	if limit > 0:
		if limit < len(catalogue):
			catalogue = catalogue.head(limit)
 
	doc_urls = catalogue["file_url"].values
	doc_ids = catalogue["docid"].values
	doc_paths = catalogue["filepath"].values
    
	if n_jobs > 1:
		succes_downloaded = Parallel(n_jobs=n_jobs)(
			delayed(download_file)(url, id, path) for url, id, path in
			tqdm(list(zip(doc_urls, doc_ids, doc_paths))))
	else:
		succes_downloaded = []
		for url, id, path in tqdm(list(zip(doc_urls, doc_ids, doc_paths))):
			succes_downloaded.append(download_file(url, id,path))

	print(f"Successfully downloaded {sum(succes_downloaded)} of {len(succes_downloaded)} files")








def main(argv):
	#default parameters
	LIMIT = 0
	N_JOBS = 1

	#get custom parameters
	try:
		opts, args = getopt.getopt(argv,"S:J:")
	except getopt.GetoptError:
		print('DocumentDataGen.py -S <scale> -J <n_jobs>')
		sys.exit()
	try:
		for opt, arg in opts:
			if opt == '-S':
				LIMIT = int(arg)
				#LIMIT OKAY
			elif opt == '-J':
				N_JOBS = int(arg)
			else:
				print('DocumentDataGen.py -S <scale> -J <n_jobs>')
				sys.exit()
	except:
		print('DocumentDataGen.py -S <scale> -J <n_jobs>')
		sys.exit()
	
	if (LIMIT > 5 or LIMIT < 1 or N_JOBS < 1) :
		print("Invalid or undefined parameters: Must have S parameter between 1 and 5, and J at least 1.")
		print('DocumentDataGen.py -S <scale> -J <n_jobs>')
		sys.exit()
	start = time()



	#build catalogue
	build_hal_docs_catalogue(limit=LIMIT*10000)
	print("CATALOGUE BUILT...")
	#download data
	
	download_text_files(n_jobs=N_JOBS)
	
	done = time()
	elapsed = done - start
	print("*"*20)
	print("STARTED : "+ctime(start) )
	print("END : "+ctime(done) )
	print("TIME ELAPSED:" + str(elapsed/60) + " MINUTES" )

if __name__ == "__main__":
	main(sys.argv[1:])
