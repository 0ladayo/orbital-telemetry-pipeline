import os
import pandas_gbq
import logging
import numpy as np
import pandas as pd
from physics import get_position
from sat_utils import orbit_classifier, launch_year, get_owner
import base64
import functions_framework


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@functions_framework.cloud_event
def main(cloud_event): 
    try:

        pubsub_message = cloud_event.data['message']['data']

        message_str = base64.b64decode(pubsub_message).decode('utf-8')

        logging.info(f"Received Pub/Sub trigger: {message_str}")

        if message_str == 'Ingestion logic completed':
            
            logging.info('Starting Transformation Logic...')

            project_id = os.environ['PROJECT__ID']

            bigquery_dataset = os.environ['BIGQUERY__DATASET']
            
            logging.info('Ingesting data from BigQuery Table...')

            df = pandas_gbq.read_gbq(f"SELECT * FROM `{bigquery_dataset}.orbital_satellites_data`", project_id = project_id,)

            df.columns = df.columns.str.upper()

            logging.info(f'Filtering for active PAYLOADS')

            df = df[df['TYPE'] == 'Active'].copy()

            records = df.to_dict('records')

            logging.info(f'Propagating trajectories for payloads (24h horizon)...')

            trajectoryPath= [get_position(row) for row in records]

            trajectoryList, altitudeList = zip(*trajectoryPath)

            logging.info('Physics engine complete')

            logging.info('Generating metadata attributes...')

            orbitClassifierLogic = np.vectorize(orbit_classifier)

            orbitClassifier = orbitClassifierLogic(df['MEAN_MOTION'], df['ECCENTRICITY'])

            launchYearLogic = np.vectorize(launch_year)

            launchYear = launchYearLogic(df['OBJECT_ID'])

            getOwnerLogic = np.vectorize(get_owner)

            getOwner = getOwnerLogic(df['OBJECT_NAME'])

            logging.info("Assembling final dataset...")

            dfTransformed = df[['NORAD_CAT_ID', 'OBJECT_NAME', 'TYPE', 'INCLINATION']].rename(columns = {
                'NORAD_CAT_ID': 'Norad_Id', 'OBJECT_NAME': 'Object_Name', 'TYPE': 'Type', 'INCLINATION': 'Inclination'
            })

            dfTransformed['Orbit'] = orbitClassifier

            dfTransformed['Owner'] = getOwner
            
            dfTransformed['Launch_Year'] = launchYear

            dfTransformed['Avg_Altitude'] = altitudeList

            dfTransformed['Trajectory'] = trajectoryList

            logging.info('Uploading to BigQuery (transformed_orbital_satellites_data)...')

            pandas_gbq.to_gbq(dfTransformed, f'{bigquery_dataset}.transformed_orbital_satellites_data', project_id = project_id, if_exists = 'replace')

            logging.info('Transformation Script finished successfully')
        
        else:

            logging.info('Ignoring irrelevant message')

    except Exception:

        logging.exception('Transformation Script failed')