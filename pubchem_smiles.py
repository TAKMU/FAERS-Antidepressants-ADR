import pubchempy as pcp
import pandas as pd
import time
import concurrent.futures as futures
import dask.dataframe as dd

special_characters = '@#$%^&*()_+={}[]|\\:;"\'<>,.?/~`'

def smiles_data(drug):
    df = pcp.get_properties(['IsomericSMILES', 'CanonicalSMILES'], drug, "name")
    return df[0]

if __name__ == '__main__':
    study = pd.read_csv("./Antidepressives.csv", low_memory=False, on_bad_lines='skip', encoding_errors='ignore')
    drugs = study["Drug"].to_list()
    syns = []
    dfs = []
    smiles = {
        "Name" : [],
        "Isomeric" : [],
        "Canonical" : []
    }
    for drug in drugs:
        smiles["Name"].append(drug)
        result = smiles_data(drug)
        smiles["Isomeric"].append(result['IsomericSMILES'])
        smiles["Canonical"].append(result['CanonicalSMILES'])
        
    smile_df = pd.DataFrame(smiles)
    smile_df.to_csv("./data/smiles.csv", index=False)
   