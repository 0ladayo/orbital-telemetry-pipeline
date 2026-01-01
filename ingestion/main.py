import dlt
import pandas as pd
import logging
import functions_framework

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info('script started ...')


@dlt.resource(table_name = 'orbital_satellites_data', write_disposition = 'replace', file_format = 'parquet')
def load_satellites_data():
    url = 'https://celestrak.com/NORAD/elements/gp.php?GROUP=active&FORMAT=csv'
    dtypes = {
        'MEAN_MOTION_DDOT': float, 
        'MEAN_MOTION_DOT': float,  
        'BSTAR': float,           
        'ECCENTRICITY': float,     
        'MEAN_MOTION': float,
        'INCLINATION': float,
        'RA_OF_ASC_NODE': float,
        'ARG_OF_PERICENTER': float,
        'MEAN_ANOMALY': float,
    }
    logging.info(f'fetching data from {url}...')
    for chunk in pd.read_csv(url, chunksize = 100, dtype = dtypes):
        yield chunk

@functions_framework.http
def main(request):
    try:
        pipeline = dlt.pipeline(pipeline_name = 'orbital_telemetry_pipeline', destination = 'bigquery', dataset_name = "orbital_satellites_dataset",
        staging = 'filesystem')
        load_info = pipeline.run(load_satellites_data)
        logging.info(f'pipeline finished successfully. Info: {load_info}')
        return 'Success', 200

    except Exception:
        logging.exception('pipeline crashed')
        return 'Failed', 500