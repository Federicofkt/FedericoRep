#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas
import random

nome="green_tripdata_2016-06.csv"
n=sum(1 for line in open(nome))-1
s=30000
skip=sorted(random.sample(range(1,n+1),n-s))
df=pandas.read_csv(nome,skiprows=skip)
df.to_csv("verde.csv", index=False)


# In[ ]:





# In[ ]:




