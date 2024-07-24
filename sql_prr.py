import numpy as np
import re
import os

import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import concurrent.futures as futures
import dask.dataframe as dd
import glob
from dotenv import load_dotenv

load_dotenv() 
SSH_USER = os.environ.get("SSH_USER")
SSH_ADDRESS = os.environ.get("SSH_ADDRESS")
SSH_PSSWD = os.environ.get("SSH_PSSWD")
SSH_PORT = int(os.environ.get("SSH_PORT_REMOTE"))

DB_IP = os.environ.get("DB_IP")
DB_PORT = int(os.environ.get("DB_PORT"))
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PSSWD = os.environ.get("DB_PSSWD")

def reac_data(drug, server):
   print(f"Starting {drug}")
   
   cursor = connection.cursor()
   query = f"""SELECT pt, prod_ai, count(primaryid) AS no_reactions FROM DRUG 
               INNER JOIN REAC USING(primaryid)
               WHERE prod_ai ILIKE '{drug}%' 
               GROUP BY pt, prod_ai
               """
   cursor.execute(query)
   data = cursor.fetchall()
   if len(data) > 0: 
      column_names = [desc[0] for desc in cursor.description]
      df = pd.DataFrame(data, columns=column_names)
      df["prod_ai"] = drug
      df = df.groupby(["pt", "prod_ai"])["no_reactions"].sum().reset_index()
      file_path = f"./drugs/{drug}/adr.csv"
      df.to_csv(file_path, index=False)
   cursor.close()
   
   return f"Finish {drug}"

try:

    with SSHTunnelForwarder(
         (SSH_ADDRESS, SSH_PORT),
         ssh_username=SSH_USER,
         ssh_password=SSH_PSSWD, 
         remote_bind_address=("localhost", 5432)) as server:
         
         server.start()
         print ("server connected")
         connection = psycopg2.connect(
                        host=DB_IP,
                        database=DB_NAME,
                        user=DB_USER,
                        password=DB_PSSWD,
                        port=server.local_bind_port
                     )
         if __name__ == '__main__':
            pattern = "./drugs/*/adr.csv"  
            files_to_delete = glob.glob(pattern)
            for file_path in files_to_delete:
                try:
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")
            study = pd.read_csv("./Antidepressives.csv", low_memory=False, on_bad_lines='skip', encoding_errors='ignore')
            drugs = study["Drug"].to_list()

            with futures.ThreadPoolExecutor() as e:
               f = [e.submit(reac_data, drug, server) for drug in drugs]
               for r in futures.as_completed(f):
                  print(r.result())
            
            pattern = "./drugs/*/adr.csv"  
            df = dd.read_csv(pattern)
            df['no_reactions'].astype(int)

            # Perform groupby and sum aggregation
            result = df.groupby(['pt'])["no_reactions"].sum().reset_index().compute()
            print(result.head())
            result = result.rename(columns={'no_reactions': 'total'})

            # Write result to a CSV file
            result.to_csv("./drugs/adr_summary.csv", index=False) 
            df_prr = dd.merge(df, result, on='pt', how='inner')
            df_prr["prr"] = df_prr["no_reactions"] / df_prr["total"]
            df_prr.to_csv("prr.csv", index=False)
            connection.close()
except Exception as error:
    print ("Connection Failed")
    print(error)

