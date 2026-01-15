from skyfield.api import load, wgs84
from skyfield.sgp4lib import EarthSatellite
import numpy as np
from datetime import datetime, timezone

ts = load.timescale()

now = datetime.now(timezone.utc)

midnight = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

start_time = ts.from_datetime(midnight)

minutes = np.arange(0, 1440, 10)

timeVector = start_time + (minutes / 1440.0)

def get_position(sat_data):

    satellite = EarthSatellite.from_omm(ts, sat_data)

    geocentric = satellite.at(timeVector)
    
    geo_pos = wgs84.geographic_position_of(geocentric)

    lats = geo_pos.latitude.degrees

    lons = geo_pos.longitude.degrees

    alts = geo_pos.elevation.km

    path = []

    for t_obj, lat, lon, alt in zip(timeVector, lats, lons):
        path.append({
            'timestamp': str(t_obj.utc_datetime()), 
            'lat': round(float(lat), 2),
            'lon': round(float(lon), 2),
        })

    return path, np.mean(alts)