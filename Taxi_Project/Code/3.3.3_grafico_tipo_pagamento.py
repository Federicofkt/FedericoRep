
# coding: utf-8

# In[43]:


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv('payment_verdi.csv')


# In[44]:


tipo_1 = np.array(df['1'])
tipo_2 = np.array(df['2'])


# In[50]:


years = ['2013','2014','2015','2016','2017','2018']
barWidth = 0.6
r1 = np.arange(len(years))*1.5
r2 = [x + barWidth for x in r1]
plt.bar(r1, tipo_1, width = barWidth, color = 'blue', label = 'CC')
plt.bar(r2, tipo_2, width = barWidth, color = 'orange', label ='CSH')
plt.xticks(ticks = r1 + 0.29, labels = years)
plt.xlabel("Year")
plt.ylabel("Number of payments")
plt.legend()
plt.figure(figsize=(6,7))

