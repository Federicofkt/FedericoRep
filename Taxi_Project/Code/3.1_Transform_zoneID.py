#!/usr/bin/env python
# coding: utf-8

# In[1]:


import bokeh, bokeh.plotting, bokeh.models
from bokeh.io import output_notebook, show
output_notebook()

import geopandas as gpd
from shapely.geometry import Point
import urllib
import dask.dataframe as dd
import dask.distributed
import numpy as np

import sklearn.preprocessing

client = dask.distributed.Client()

coord_system = {'init': 'epsg:4326'}
df = gpd.read_file('taxi_zones.shp').to_crs(coord_system)
df = df.drop(['Shape_Area', 'Shape_Leng', 'OBJECTID'], axis=1)
display(df)


# In[84]:


import pandas
dati=pandas.read_csv("miocsv.csv")
dato=dd.read_csv("miocsv.csv")


# In[31]:


def assign_taxi_zones(df, lon_var, lat_var, locid_var):
    """Joins DataFrame with Taxi Zones shapefile.
    This function takes longitude values provided by `lon_var`, and latitude
    values provided by `lat_var` in DataFrame `df`, and performs a spatial join
    with the NYC taxi_zones shapefile. 
    The shapefile is hard coded in, as this function makes a hard assumption of
    latitude and longitude coordinates. It also assumes latitude=0 and 
    longitude=0 is not a datapoint that can exist in your dataset. Which is 
    reasonable for a dataset of New York, but bad for a global dataset.
    Only rows where `df.lon_var`, `df.lat_var` are reasonably near New York,
    and `df.locid_var` is set to np.nan are updated. 
    Parameters
    ----------
    df : pandas.DataFrame or dask.DataFrame
        DataFrame containing latitudes, longitudes, and location_id columns.
    lon_var : string
        Name of column in `df` containing longitude values. Invalid values 
        should be np.nan.
    lat_var : string
        Name of column in `df` containing latitude values. Invalid values 
        should be np.nan
    locid_var : string
        Name of series to return. 
    """

    import geopandas
    from shapely.geometry import Point


    # make a copy since we will modify lats and lons
    localdf = df[[lon_var, lat_var]].copy()
    
    # missing lat lon info is indicated by nan. Fill with zero
    # which is outside New York shapefile. 
    localdf[lon_var] = localdf[lon_var].fillna(value=0.)
    localdf[lat_var] = localdf[lat_var].fillna(value=0.)
    

    shape_df = geopandas.read_file('taxi_zones.shp')
    shape_df.drop(['OBJECTID', "Shape_Area", "Shape_Leng", "borough", "zone"],
                  axis=1, inplace=True)
    shape_df = shape_df.to_crs({'init': 'epsg:4326'})

    try:
        local_gdf = geopandas.GeoDataFrame(
            localdf, crs={'init': 'epsg:4326'},
            geometry=[Point(xy) for xy in
                      zip(localdf[lon_var], localdf[lat_var])])

        local_gdf = geopandas.sjoin(
            local_gdf, shape_df, how='left', op='within')

        return local_gdf.LocationID.rename(locid_var)
    except ValueError as ve:
        print(ve)
        print(ve.stacktrace())
        series = localdf[lon_var]
        series = np.nan
        return series


# In[86]:


assign_taxi_zones(dati,"Dropoff_longitude","Dropoff_latitude","vediamo")


# In[87]:


dati['PULocationID'] = dato.map_partitions(
    assign_taxi_zones, "Pickup_longitude", "Pickup_latitude",
    "PULocationID", meta=('PULocationID', np.float64))
dati['DOLocationID'] = dato.map_partitions(
    assign_taxi_zones, "Dropoff_longitude", "Dropoff_latitude",
    "DOLocationID", meta=('DOLocationID', np.float64))
dati[['PULocationID', 'DOLocationID']].head()


# In[88]:


display(dati)


# In[89]:


dati.to_csv("2015_verdi.csv", index=False)


# In[ ]:




