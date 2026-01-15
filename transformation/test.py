import os
import pandas_gbq
from skyfield.api import load, wgs84
from skyfield.sgp4lib import EarthSatellite


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'orbital-telemetry-pipeline-4a0629de017b.json'

df = pandas_gbq.read_gbq("SELECT * FROM `orbital_satellites_dataset.orbital_satellites_data`")

df.columns = df.columns.str.upper()

sat_data = df.iloc[1].to_dict()

ts = load.timescale()

satellite = EarthSatellite.from_omm(ts, sat_data)

t = ts.now()

geocentric = satellite.at(t)

geo_pos = wgs84.geographic_position_of(geocentric)

dic = {'lat': geo_pos.latitude.degrees, 'lon': geo_pos.longitude.degrees, 'alt': geo_pos.elevation.km }

print(dic)