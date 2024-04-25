import os
import pandas as pd
import psycopg2
import logging
import dask.dataframe as dd
import dask
from dask.distributed import Client, LocalCluster, as_completed
import time
from dotenv import load_dotenv

load_dotenv() 
HOST = os.environ.get("DB_LC_HOST")
DB_USER = os.environ.get("DB_LC_USER")
DB_NAME = os.environ.get("DB_LC")
DB_PSSWD = os.environ.get("DB_LC_PSSWD")

def drug_info(drug):
   print(drug)
   connection = psycopg2.connect(
       host=HOST,
       database=DB_NAME,
       user=DB_USER,
       password=DB_PSSWD
   )
   cursor = connection.cursor()
   query = f"""SELECT * FROM DRUG 
         INNER JOIN THER USING(primaryid)
       WHERE prod_ai ILIKE '{drug}%' 
         AND DRUG.drug_seq = THER.dsg_drug_seq """
   cursor.execute(query)
   data = cursor.fetchall()
   len_ther.append(len(data))
   column_names = [desc[0] for desc in cursor.description]  # Get column names
   df = pd.DataFrame(data, columns=column_names)   
   df.to_csv(f"./drugs/{drug}/DRUG_INDI.csv")
   primaryids = df["primaryid"].unique()
   df_id = pd.DataFrame(primaryids)
   df_id.to_csv(f"./drugs/{drug}/primaryid.csv")
   patients.append(len(primaryids))
   total_reac = 0
   for primaryid in primaryids: 
      query = f"""SELECT * FROM DEMO  
      INNER JOIN REAC using(primaryid) 
      WHERE DEMO.primaryid = '{primaryid}' 
      """
      cursor.execute(query)
      data = cursor.fetchall()
      total_reac += len(data)
      column_names = [desc[0] for desc in cursor.description]  # Get column names
      df2 = pd.DataFrame(data, columns=column_names)  
      if not os.path.isfile(f"./drugs/{drug}/DEMO_REAC.csv"):   
               df2.to_csv(f"./drugs/{drug}/DEMO_REAC.csv")
      else: 
               df2.to_csv(f"./drugs/{drug}/DEMO_REAC.csv", mode='a', header=False, na_rep='NULL') 
   len_reac.append(total_reac)
if __name__ == '__main__':
   start_time = time.time()
   cluster = LocalCluster(n_workers=8, threads_per_worker=2)
   client = Client(cluster)
   study = pd.read_csv("./Antidepressives.csv", low_memory=False, on_bad_lines='skip', encoding_errors='ignore')
   drugs = study["Drug"].to_list()
   len_ther = []
   len_reac = []
   patients = []
   results = [drug_info(drug) for drug in drugs]

# Asynchronously compute the results
   futures = client.compute(results)

   # Wait for all computations to finish
   for future in as_completed(futures):
       result = future.result()
       print("Result:", result)

   summary = {
               'drug' : drugs,
               'therapies' : len_ther,
               'reactions' : len_reac,
               'patients' : patients
              }
   df2 = pd.DataFrame(summary)
   df2.to_csv("./summary.csv")
   cluster.close()
   end_time = time.time()
   print(start_time - end_time)

