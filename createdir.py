import os
import pandas as pd

df = pd.read_csv("./Antidepressives.csv", index_col="ATC code")

for drug in df["Drug"]:
    if not os.path.exists(f"./drugs/{drug}"):
        os.makedirs(f"./drugs/{drug}")
