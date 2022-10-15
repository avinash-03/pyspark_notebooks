# %%
import pandas as pd 

# %%
df = pd.read_csv('data/OfficeData.csv')

# %%
#print schema
print(df)

# %%
# show departmentwise sum of salary
df2 = df.groupby('department').sum().reset_index()

# %%
print(df2)

# %%



