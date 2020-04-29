
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv('media_distanza_mesi_verdi.csv')
df2 = pd.read_csv('media_distanza_mesi_gialli.csv')
df2


# In[82]:


total_verdi = np.array(df['avg(trip_distance)'])
total_gialli = np.array(df2['avg(trip_distance)'])
for i in range(89):
    if total_gialli[i] > 20:
        total_gialli[i] = (total_gialli[i - 1] + total_gialli[i + 1])/2


# In[ ]:


total_verdi = np.delete(total_verdi, 0)
total_verdi


# In[84]:


x = [x for x in range(31,90)]
years = ['2011','2012','2013','2014','2015','2016','2017','2018']
plt.plot(x, total_verdi,  color = 'green', label = 'green fare')
plt.plot(total_gialli,  color = 'yellow', label = 'yellow fare')
plt.xticks(ticks = [0,12,24,36,48,60,72,84], labels = years)
plt.legend()

