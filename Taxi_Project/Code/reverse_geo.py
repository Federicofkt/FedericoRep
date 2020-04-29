from geopy.geocoders import Nominatim
geolocator = Nominatim()

borough = []
loc = ['0.5, -73.951857'] #0.5 and -73.951857 are just examples 
for l in loc:
    sub = str(geolocator.reverse(l))
    borough.append(sub.split(', ')[2])
print(borough)
