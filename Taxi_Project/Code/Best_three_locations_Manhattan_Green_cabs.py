from pyspark.sql.functions import *

locazioni=spark.read.format('csv').load('/FileStore/tables/taxi__zone_lookup-b0a97.csv', inferSchema = True, header = True)

unito = total_verdi.join(locazioni, total_verdi.PULocationID == locazioni.LocationID)
unito = unito.withColumnRenamed("Borough", "zona_prelievo_1")
unito = unito.drop("LocationID","Zone","service_zone")
unito2 = unito.join(locazioni, unito.DOLocationID == locazioni.LocationID)
unito2 = unito2.withColumnRenamed("Borough", "zona_scarico")
unito2 = unito2.drop("Zone","service_zone")

dummy_green = unito2.filter(unito2["zona_prelievo"]=="Manhattan")
dummy_green = dummy_green.groupBy("PULocationID").count()
dummy_green = dummy_green.orderBy(dummy_green["count"], ascending=False).limit(3)
dummy_green = dummy_green.join(locazioni, dummy_green.PULocationID == locazioni.LocationID)
top_3_Mnht_green = dummy_green.select("Count","Borough","Zone").orderBy(dummy_green["count"], ascending=False)
display(top_3_Mnht_green)