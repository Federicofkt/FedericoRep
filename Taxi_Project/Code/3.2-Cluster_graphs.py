#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd


import networkx as nx
from sciprog import draw_nx
prova=pd.read_csv("/Users/federicosparapan/desktop/media_mance.csv")

import pydot as py
from pydot import Edge

g=py.Dot(graph_name="Trip", graph_type="digraph")
g.set_node_defaults(color="blue", fontcolor="red")
g.set_node_defaults(bla=3, arrowhead="vee",arrowsize='10.6', splines='curved', fontcolor='brown')
g.set_node_defaults(scale=3)
for index, row in prova.iterrows():
    
    g.add_edge(Edge(row["zona_prelievo"], row["zona_scarico"], color="brown", label=row["avg(tip_amount)"]))
g.write_png("media_mance.png")


# In[ ]:




