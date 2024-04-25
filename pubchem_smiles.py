import pubchempy as pcp
import pandas as pd
import time
import concurrent.futures as futures
import dask.dataframe as dd

special_characters = '@#$%^&*()_+={}[]|\\:;"\'<>,.?/~`'

def smiles_data(drug):
    df = pcp.get_properties(['IsomericSMILES', 'CanonicalSMILES'], drug, "name")
    return df[0]

def synonyms_data(drug):
    syn_list = pcp.get_synonyms(drug, 'name', 'compound')[0]
    return syn_list["Synonym"]

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
        syns = pd.DataFrame({'synonyms': synonyms_data(drug)})
        contains_special_char = syns.apply(lambda row: any(char in special_characters for char in row.synonyms), axis=1)
        syns = syns[~contains_special_char]
        syns.to_csv(f"./drugs/{drug}/synonyms.csv", index=False)
        smiles["Name"].append(drug)
        result = smiles_data(drug)
        smiles["Isomeric"].append(result['IsomericSMILES'])
        smiles["Canonical"].append(result['CanonicalSMILES'])
        
    smile_df = pd.DataFrame(smiles)
    smile_df.to_csv("smiles.csv", index=False)
   