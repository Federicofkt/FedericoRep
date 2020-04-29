# Databricks notebook source
#unione dataframe
verdi_2013 = spark.read.format('csv').load('/FileStore/tables/2013_verdi.csv', inferSchema = True, header=True)
verdi_2014 = spark.read.format('csv').load('/FileStore/tables/2014_verdi.csv', inferSchema = True, header=True)
verdi_2015 = spark.read.format('csv').load('/FileStore/tables/2015_verdi.csv', inferSchema = True, header=True)
verdi_2016 = spark.read.format('csv').load('/FileStore/tables/2016_verdi.csv', inferSchema = True, header=True)
verdi_2017 = spark.read.format('csv').load('/FileStore/tables/2017_verdi.csv', inferSchema = True, header=True)
verdi_2018 = spark.read.format('csv').load('/FileStore/tables/2018_verdi.csv', inferSchema = True, header=True)

# COMMAND ----------

verdi_2013 = verdi_2013.drop(verdi_2013['Ehail_fee'])

# COMMAND ----------

verdi_2014 = verdi_2014.drop(verdi_2014['Ehail_fee'])

# COMMAND ----------

verdi_2015 = verdi_2015.drop(verdi_2015['improvement_surcharge'])
verdi_2016 = verdi_2016.drop(verdi_2016['improvement_surcharge'])
verdi_2017 = verdi_2017.drop(verdi_2017['improvement_surcharge'])
verdi_2018 = verdi_2018.drop(verdi_2018['improvement_surcharge'])

# COMMAND ----------

#aggiungo la colonna anno
from pyspark.sql.functions import *

verdi_2013 = verdi_2013.withColumn('anno', year('pickup_datetime'))
verdi_2014 = verdi_2014.withColumn('anno', year('pickup_datetime'))
verdi_2015 = verdi_2015.withColumn('anno', year('pickup_datetime'))
verdi_2016 = verdi_2016.withColumn('anno', year('pickup_datetime'))
verdi_2017 = verdi_2017.withColumn('anno', year('pickup_datetime'))
verdi_2018 = verdi_2018.withColumn('anno', year('pickup_datetime'))

# COMMAND ----------

verdi_2013 = verdi_2013.select('VendorID','pickup_datetime','dropoff_datetime','Store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','Extra','mta_tax','tip_amount','tolls_amount','total_amount','Payment_type','Trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
verdi_2014 = verdi_2014.select('VendorID','pickup_datetime','dropoff_datetime','Store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','Extra','mta_tax','tip_amount','tolls_amount','total_amount','Payment_type','Trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
verdi_2015 = verdi_2015.select('VendorID','pickup_datetime','dropoff_datetime','Store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','Extra','mta_tax','tip_amount','tolls_amount','total_amount','Payment_type','Trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
verdi_2016 = verdi_2016.select('VendorID','pickup_datetime','dropoff_datetime','Store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','Extra','mta_tax','tip_amount','tolls_amount','total_amount','Payment_type','Trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
verdi_2017 = verdi_2017.select('VendorID','pickup_datetime','dropoff_datetime','Store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','Extra','mta_tax','tip_amount','tolls_amount','total_amount','Payment_type','Trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
verdi_2018 = verdi_2018.select('VendorID','pickup_datetime','dropoff_datetime','Store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','Extra','mta_tax','tip_amount','tolls_amount','total_amount','Payment_type','Trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')

# COMMAND ----------

total_verdi = verdi_2013.union(verdi_2014).union(verdi_2015).union(verdi_2016).union(verdi_2017).union(verdi_2018)

# COMMAND ----------

#clustering
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import *

#pulizia del dataframe per avere solo float per utilizzare gli algoritmi di machine learning
dizio = {'N': '0', 'Y':'1'}
total_verdi = total_verdi.na.replace(dizio,'store_and_fwd_flag')
total_verdi = total_verdi.withColumn('store_and_fwd_flag', total_verdi['store_and_fwd_flag'].cast(IntegerType()))
vecAssembler = VectorAssembler(inputCols=['VendorID', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'Payment_type', 'fare_amount', 'Extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'giorno', 'mese', 'ora', 'anno'], outputCol='features').transform(total_verdi)

#scelta di possibili valori di k
cost = np.zeros(20)
for k in range(2,20):
  kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol('features')
  model = kmeans.fit(vecAssembler)
  cost[k] = model.computeCost(vecAssembler)
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.plot(range(2,20),cost[2:20])
ax.set_xlabel('k')
ax.set_ylabel('cost')
display(fig)

# COMMAND ----------

#valutazione del valore di K migliore
kmeans = KMeans().setK(10).setSeed(1).setFeaturesCol('features')
model = kmeans.fit(vecAssembler)

predictions_10 = model.transform(vecAssembler)
evaluator = ClusteringEvaluator()
silhoutte = evaluator.evaluate(predictions_10)
print('valutazione: ' + str(silhoutte))
kmeans = KMeans().setK(11).setSeed(1).setFeaturesCol('features')
model = kmeans.fit(vecAssembler)

predictions_11 = model.transform(vecAssembler)
evaluator = ClusteringEvaluator()
silhoutte = evaluator.evaluate(predictions_11)
print('valutazione: ' + str(silhoutte))

kmeans = KMeans().setK(12).setSeed(1).setFeaturesCol('features')
model = kmeans.fit(vecAssembler)

predictions_12 = model.transform(vecAssembler)
evaluator = ClusteringEvaluator()
silhoutte = evaluator.evaluate(predictions_12)
print('valutazione: ' + str(silhoutte))


# COMMAND ----------

kmeans = KMeans().setK(3).setSeed(1).setFeaturesCol('features')
model = kmeans.fit(vecAssembler)

predictions = model.transform(vecAssembler)
evaluator = ClusteringEvaluator()
silhoutte = evaluator.evaluate(predictions)
print('valutazione: ' + str(silhoutte))


# COMMAND ----------

display(predictions_11.groupBy('prediction').count())

# COMMAND ----------

#conteggio per zona_prelievo, zona_scarico
cont = predictions_11.where(predictions_11['prediction'] == 0).groupBy('zona_prelievo','zona_scarico').count()
display(cont)

# COMMAND ----------

#avg fare_amount per zona_prelievo, zona_scarico
cont = predictions_11.where(predictions_11['prediction'] == 0).groupBy('zona_prelievo','zona_scarico').avg('fare_amount')
cont = cont.withColumn('avg(fare_amount)', round(cont['avg(fare_amount)'], 2))
display(cont)

# COMMAND ----------

#media mance per zona_prelievo, zona_scarico
cont = predictions_11.where(predictions_11['prediction'] == 0).where(predictions_11['payment_type'] == 1).groupBy('zona_prelievo','zona_scarico').avg('tip_amount')
cont = cont.withColumn('avg(tip_amount)', round(cont['avg(tip_amount)'], 2))
display(cont)

# COMMAND ----------

#codice per il grafo utilizzanfo l'export del primo cont (conteggio corse tra varie macroaree)
prova=pd.read_csv("export.csv")
g=py.Dot(graph_name="Trip", graph_type="digraph")
g.set_node_defaults(color="blue", fontcolor="red")
g.set_node_defaults(bla=3, arrowhead="vee",arrowsize='10.6', splines='curved', fontcolor='brown')
g.set_node_defaults(scale=3)
for index, row in prova.iterrows():
    
    g.add_edge(Edge(row["zona_prelievo"], row["zona_scarico"], color="brown", label=row["count"]))
g.write_png("dfg.png")

# COMMAND ----------


