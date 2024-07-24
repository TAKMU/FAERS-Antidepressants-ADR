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

# load all environment variables (considering if DB is using SSH Tunneling)
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
def reac_data(drug):
   # Obtain data from DB (drug name, adverse drug effects)
   # Count the unique ADRs for each drug 
   # return drug and number of unique ADRs
   cursor = connection.cursor()
   query = f"""SELECT pt, prod_ai FROM DRUG 
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
      df = df.drop_duplicates(subset=['pt'])
      return {"name": drug, "adrs" : len(df)}
   cursor.close()
   return {"name": drug, "adrs" : 0}

try:
   #Use SSH to connect to the DB
   with SSHTunnelForwarder(
         (SSH_ADDRESS, SSH_PORT),
         ssh_username=SSH_USER,
         ssh_password=SSH_PSSWD, 
         remote_bind_address=(DB_IP, DB_PORT)) as server:
         
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
        #use main to allow the use of futures
        #obtain the Antidepressives from ATC code. 
        #use function reac_data to obtain the quantity of unique ADRs for each drug
        #save result on csv (drug_no_reactions.csv)
         study = pd.read_csv("./Antidepressives.csv", low_memory=False, on_bad_lines='skip', encoding_errors='ignore')
         drugs = study["Drug"].to_list()
         drug_adr_dict = {"prod_ai": [], "adrs" : []}
         with futures.ThreadPoolExecutor() as e:
            f = [e.submit(reac_data, drug) for drug in drugs]
            for r in futures.as_completed(f):
               if(r.result() != None):
                  drug_adr_dict["prod_ai"].append(r.result()["name"])
                  drug_adr_dict["adrs"].append(r.result()["adrs"])
         result = pd.DataFrame(drug_adr_dict)
         result = result.sort_values(by="adrs")
         labels = pd.read_csv("./data/cluster_label.csv")
         result = pd.merge(result, labels, on="prod_ai")
         result.to_csv("./data/drug_no_reactions.csv")
         connection.close()


except Exception as error:
    print ("Connection Failed")
    print(error)

