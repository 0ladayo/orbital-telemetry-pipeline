import dash
from dash import dcc, html, Input, Output, callback, State, ctx
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import pandas_gbq
import json
import os
import logging
import io
import numpy as np
import threading
import gc
from datetime import datetime, timezone
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


PROJECT_ID = os.environ.get('PROJECT_ID')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')

logging.info(f"Config: PROJECT_ID={PROJECT_ID}, BIGQUERY_DATASET={BIGQUERY_DATASET}, GCS_BUCKET_NAME={GCS_BUCKET_NAME}")
CACHE_FILENAME = 'orbital_data_cache_v2.parquet'
KPI_CACHE_FILENAME = 'orbital_kpi_cache.parquet'


df_main = None
kpi_data = None
timestamps = None
search_options = []
_traj_lat = None
_traj_lon = None
_cached_figures = {}

data_lock = threading.Lock()

def optimize_dataframe_memory(df):
    for col in ['Object_Name', 'Owner', 'Orbit', 'Object_Type']:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        if col not in ['Trajectory_Lat', 'Trajectory_Lon']:
            df[col] = df[col].astype('float32')
            
    int_cols = df.select_dtypes(include=['int64']).columns
    for col in int_cols:
        if df[col].max() < 2147483647 and df[col].min() > -2147483648:
            df[col] = df[col].astype('int32')
    return df

def extract_trajectories_to_arrays(df):
    n_rows = len(df)
    n_timestamps = 144 
    
    lat_array = np.zeros((n_rows, n_timestamps), dtype=np.float32)
    lon_array = np.zeros((n_rows, n_timestamps), dtype=np.float32)
    
    for i, traj in enumerate(df['Trajectory']):
        if isinstance(traj, str):
            traj = json.loads(traj)
        steps = min(len(traj), n_timestamps)
        for j in range(steps):
            lat_array[i, j] = traj[j]['lat']
            lon_array[i, j] = traj[j]['lon']
            
    df = df.drop(columns=['Trajectory'])
    df['_traj_idx'] = np.arange(n_rows, dtype=np.int32)
    return df, lat_array, lon_array

def _precompute_static_figures():
    global _cached_figures
    if df_main is None: return
    
    owner_counts = df_main['Owner'].value_counts().head(10).reset_index()
    owner_counts.columns = ['Owner', 'Count']
    fig_owners = px.bar(owner_counts, x='Count', y='Owner', orientation='h', text_auto=True)
    fig_owners.update_layout(template="plotly_white", title="<b>Owner Distribution</b>", height=600, margin={"r":20,"t":60,"l":20,"b":20})
    fig_owners.update_traces(marker_color='#3b82f6')
    _cached_figures['owners'] = fig_owners
    
    df_alt = df_main[df_main['Avg_Altitude'] < 40000]
    fig_alt = px.histogram(df_alt, x="Avg_Altitude", nbins=50)
    fig_alt.update_layout(template="plotly_white", title="<b>Altitude Distribution (km)</b>", height=350, margin={"r":20,"t":50,"l":20,"b":20})
    fig_alt.update_traces(marker_color='#3b82f6')
    _cached_figures['altitude'] = fig_alt
    
    fig_inc = px.histogram(df_main, x="Inclination", nbins=50)
    fig_inc.update_layout(template="plotly_white", title="<b>Inclination Distribution (°)</b>", height=350, margin={"r":20,"t":50,"l":20,"b":20})
    fig_inc.update_traces(marker_color='#3b82f6')
    _cached_figures['inclination'] = fig_inc
    
    del df_alt
    gc.collect()

def load_data_smart():
    global df_main, kpi_data, timestamps, search_options, _traj_lat, _traj_lon, _cached_figures
    
    if df_main is not None: return 

    with data_lock:
        if df_main is not None: return 

        today_utc = datetime.now(timezone.utc).date()
        storage_client = storage.Client()
        loaded_from_gcs = False


        if GCS_BUCKET_NAME:
            try:
                bucket = storage_client.bucket(GCS_BUCKET_NAME)
                blob_main = bucket.blob(CACHE_FILENAME)
                blob_kpi = bucket.blob(KPI_CACHE_FILENAME)

                if blob_main.exists() and blob_kpi.exists():
                    logging.info(f"Downloading Parquet cache...")
                    
                    parquet_bytes = blob_main.download_as_bytes()
                    df_temp = pd.read_parquet(io.BytesIO(parquet_bytes))
                    del parquet_bytes
                    
                    cache_valid = True
                    if '_cache_date' in df_temp.columns:
                        cache_date = pd.to_datetime(df_temp['_cache_date'].iloc[0]).date()
                        if cache_date != today_utc:
                            logging.warning("Cache is stale. Refreshing from BigQuery...")
                            cache_valid = False
                    
                    if cache_valid:
                        df_main = df_temp
                        
                        kpi_bytes = blob_kpi.download_as_bytes()
                        kpi_temp = pd.read_parquet(io.BytesIO(kpi_bytes))
                        kpi_data = kpi_temp.iloc[0].to_dict()
                        del kpi_bytes, kpi_temp

                        n_ts = 144
                        if 'lat_0' in df_main.columns:
                            lat_cols = [f'lat_{i}' for i in range(n_ts)]
                            lon_cols = [f'lon_{i}' for i in range(n_ts)]
                            _traj_lat = df_main[lat_cols].values.astype(np.float32)
                            _traj_lon = df_main[lon_cols].values.astype(np.float32)
                            df_main = df_main.drop(columns=lat_cols + lon_cols)
                        
                        df_main['_traj_idx'] = np.arange(len(df_main), dtype=np.int32)
                        
                        df_main = optimize_dataframe_memory(df_main)
                        
                        timestamps = pd.date_range(start=pd.Timestamp(today_utc, tz='UTC'), periods=144, freq='10min')
                        
                        loaded_from_gcs = True
                        logging.info(f"GCS Parquet Load Complete. Loaded {len(df_main)} objects, trajectory shape: {_traj_lat.shape}")
            except Exception as e:
                logging.error(f"GCS Load Failed: {e}")
                import traceback
                logging.error(traceback.format_exc())
                loaded_from_gcs = False

        if not loaded_from_gcs:
            logging.info("Downloading from BigQuery (Fallback)...")
            try:
                q1 = f"SELECT * FROM `{BIGQUERY_DATASET}.transformed_orbital_satellites_data`"
                df_raw = pandas_gbq.read_gbq(q1, project_id=PROJECT_ID)
                
                df_raw['Avg_Altitude'] = pd.to_numeric(df_raw['Avg_Altitude'], errors='coerce')
                
                if isinstance(df_raw['Trajectory'].iloc[0], str):
                    first_traj = json.loads(df_raw['Trajectory'].iloc[0])
                else:
                    first_traj = df_raw['Trajectory'].iloc[0]
                timestamps = pd.to_datetime([x['timestamp'] for x in first_traj], utc=True)
                
                df_main, _traj_lat, _traj_lon = extract_trajectories_to_arrays(df_raw)
                del df_raw
                gc.collect()
                
                df_main = optimize_dataframe_memory(df_main)
                df_main['_cache_date'] = str(today_utc)
                
                q2 = f"SELECT * FROM `{BIGQUERY_DATASET}.orbital_kpis_view`"
                df_kpi_temp = pandas_gbq.read_gbq(q2, project_id=PROJECT_ID)
                kpi_data = df_kpi_temp.iloc[0].to_dict()

                if GCS_BUCKET_NAME:
                    try:
                        logging.info("Saving to GCS Parquet...")
                        df_save = df_main.copy()
                        for i in range(144):
                            df_save[f'lat_{i}'] = _traj_lat[:, i]
                            df_save[f'lon_{i}'] = _traj_lon[:, i]
                        
                        buffer = io.BytesIO()
                        df_save.to_parquet(buffer, engine='pyarrow', compression='snappy')
                        buffer.seek(0)
                        bucket = storage_client.bucket(GCS_BUCKET_NAME)
                        bucket.blob(CACHE_FILENAME).upload_from_file(buffer, content_type='application/octet-stream')
                        
                        buffer_kpi = io.BytesIO()
                        df_kpi_temp.to_parquet(buffer_kpi, engine='pyarrow', compression='snappy')
                        buffer_kpi.seek(0)
                        bucket.blob(KPI_CACHE_FILENAME).upload_from_file(buffer_kpi, content_type='application/octet-stream')
                        
                        del df_save, buffer, buffer_kpi
                        gc.collect()
                        logging.info("GCS Upload Complete.")
                    except Exception as e:
                        logging.error(f"Save to GCS failed: {e}")

            except Exception as e:
                logging.critical(f"BigQuery Load Failed: {e}")
                logging.critical(f"PROJECT_ID={PROJECT_ID}, BIGQUERY_DATASET={BIGQUERY_DATASET}, GCS_BUCKET_NAME={GCS_BUCKET_NAME}")
                import traceback
                logging.critical(traceback.format_exc())
                df_main = pd.DataFrame({'Object_Name': ['No Data'], 'Owner': ['N/A'], 'Avg_Altitude': [0], 'Inclination': [0], '_traj_idx': [0], 'Orbit': ['LEO']})
                _traj_lat = np.zeros((1, 144), dtype=np.float32)
                _traj_lon = np.zeros((1, 144), dtype=np.float32)
                kpi_data = {'Total_Objects': 0, 'Payload_Count': 0, 'Debris_Count': 0, 'Debris_Ratio_Pct': 0}
                timestamps = pd.date_range(start=pd.Timestamp.now(tz='UTC'), periods=144, freq='10min')

        unique_names = df_main['Object_Name'].cat.categories if hasattr(df_main['Object_Name'], 'cat') else df_main['Object_Name'].unique()
        search_options = [{'label': str(name), 'value': str(name)} for name in sorted(unique_names)]
        _precompute_static_figures()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
server = app.server

@server.route('/_ah/warmup')
def warmup():
    try:
        load_data_smart()
        return "Warmup successful", 200
    except Exception as e:
        return str(e), 500

def get_current_time_index():
    if timestamps is None or len(timestamps) == 0: return 0
    now_utc = pd.Timestamp.now(tz='UTC')
    try:
        time_diffs = np.abs((timestamps - now_utc).total_seconds())
        return int(np.argmin(time_diffs))
    except:
        return 0

def serve_layout():
    if df_main is None: load_data_smart()
    
    if kpi_data is None: 
        return html.Div("Loading or Error...")

    default_time_index = get_current_time_index()
    
    sidebar = html.Div([
        html.H2("🛰️", className="display-4 text-center"),
        html.H4("Analytics Controls", className="text-center mb-4"),
        html.Hr(),
        html.Label("Playback Time (UTC)", className="lead"),
        html.Div(id='time-display-sidebar', className="h4 text-primary text-center mb-3"),
        dbc.Button("↺ Sync to Now", id="reset-time-btn", color="secondary", size="sm", className="mb-3 w-100"),
        dcc.Slider(
            id='time-slider', min=0, max=143, step=1, value=default_time_index, 
            marks={0:'00:00', 36:'06:00', 72:'12:00', 108:'18:00', 143:'23:50'}, vertical=False
        ),
        html.Hr(className="my-4"),
        html.Label("Find Satellite", className="lead"),
        dcc.Dropdown(id='satellite-search', options=search_options, placeholder="Search...", className="mb-3"),
        html.Hr(className="mt-4"),
        html.P(f"Objects Tracked: {len(df_main):,}", className="text-muted text-center small")
    ], style={"position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "20rem", "padding": "2rem 1rem", "backgroundColor": "#f8f9fa", "overflowY": "auto", "borderRight": "1px solid #dee2e6"})

    content = html.Div([
        dbc.Row([dbc.Col(html.H1("Space Traffic Analytics", className="text-dark"), width=12)]),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Total Objects", className="text-muted"), html.H3(f"{kpi_data.get('Total_Objects',0):,}", className="text-dark")]), className="shadow-sm border-0"), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Active Payloads", className="text-muted"), html.H3(f"{kpi_data.get('Payload_Count',0):,}", className="text-success")]), className="shadow-sm border-0"), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Debris Count", className="text-muted"), html.H3(f"{kpi_data.get('Debris_Count',0):,}", className="text-danger")]), className="shadow-sm border-0"), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Debris Ratio", className="text-muted"), html.H3(f"{kpi_data.get('Debris_Ratio_Pct',0):.1f}%", className="text-dark")]), className="shadow-sm border-0"), width=3),
        ], className="mb-4"),
        dbc.Row([
            dbc.Col([dbc.Card([dbc.CardBody(dcc.Graph(id='globe-map', style={"height": "600px"}), className="p-0")], className="shadow-sm border-0")], width=7),
            dbc.Col([dbc.Card([dbc.CardBody(dcc.Graph(figure=_cached_figures.get('owners', go.Figure()), style={"height": "600px"}), className="p-0")], className="shadow-sm border-0")], width=5)
        ], className="mb-4"),
        dbc.Row([
            dbc.Col(dbc.Card(dcc.Graph(figure=_cached_figures.get('altitude', go.Figure())), className="shadow-sm border-0 p-1"), width=6),
            dbc.Col(dbc.Card(dcc.Graph(figure=_cached_figures.get('inclination', go.Figure())), className="shadow-sm border-0 p-1"), width=6),
        ])
    ], style={"marginLeft": "22rem", "marginRight": "2rem", "paddingBottom": "4rem"})

    return html.Div([sidebar, content], style={"backgroundColor": "#f4f6f8", "minHeight": "100vh", "position": "absolute", "top": 0, "left": 0, "width": "100%"})

app.layout = serve_layout

@callback(Output('time-slider', 'value'), [Input('reset-time-btn', 'n_clicks')], [State('time-slider', 'value')])
def reset_slider(n, c): 
    return get_current_time_index() if ctx.triggered_id == 'reset-time-btn' else c

@callback(
    [Output('globe-map', 'figure'), Output('time-display-sidebar', 'children')], 
    [Input('time-slider', 'value'), Input('satellite-search', 'value')]
)
def update_map(time_index, search_name):
    if time_index is None: 
        time_index = get_current_time_index()
    if df_main is None: 
        load_data_smart()
    
    time_index = max(0, min(time_index, 143))
    time_str = timestamps[time_index].strftime("%H:%M UTC")

    if search_name:
        mask = (df_main['Object_Name'].astype(str) == str(search_name))
        matching_indices = np.where(mask)[0]
        
        if len(matching_indices) > 0:
            lats = _traj_lat[matching_indices, time_index].tolist()
            lons = _traj_lon[matching_indices, time_index].tolist()
            texts = [f"{search_name}"]
            colors = ['#FFD700']
            sizes = [20]
            opacities = [1.0]
        else:
            lats, lons, texts, colors, sizes, opacities = [], [], [], [], [], []
    else:
        lats = _traj_lat[:, time_index].tolist()
        lons = _traj_lon[:, time_index].tolist()
        
        orbit_values = df_main['Orbit'].astype(str).values
        colors = []
        for orbit in orbit_values:
            if orbit == 'LEO':
                colors.append('#10b981')
            elif orbit == 'MEO':
                colors.append('#3b82f6')
            elif orbit == 'GEO':
                colors.append('#ef4444')
            else:
                colors.append('#eab308')
        
        texts = (df_main['Object_Name'].astype(str) + ' (' + df_main['Owner'].astype(str) + ')').tolist()
        sizes = [2] * len(lats)
        opacities = [0.7] * len(lats)

    fig = go.Figure(go.Scattergeo(
        lon=lons, 
        lat=lats, 
        text=texts, 
        mode='markers', 
        marker=dict(size=sizes, color=colors, opacity=opacities)
    ))
    fig.update_layout(
        geo=dict(
            projection_type="orthographic", 
            showland=True, landcolor="#111111", 
            showocean=True, oceancolor="#222222", 
            showcountries=True, countrycolor="#444444",
            bgcolor="rgba(0,0,0,0)"
        ), 
        title=dict(text="<b>Globe Overview</b>", x=0.02, y=0.95, font=dict(color="black", size=20)),
        margin={"r":0,"t":50,"l":0,"b":0}, 
        paper_bgcolor="rgba(0,0,0,0)", 
        template="plotly_dark"
    )
    return fig, time_str

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8050))
    app.run(host='0.0.0.0', port=port, debug=False)