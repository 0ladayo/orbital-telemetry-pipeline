import dash
from dash import dcc, html, Input, Output, callback, State, ctx
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import pandas_gbq
import json
import os
import sys
import logging
import numpy as np
from datetime import datetime, timezone


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


PROJECT_ID = os.environ['PROJECT_ID']
BIGQUERY_DATASET = os.environ['BIGQUERY_DATASET']
if os.path.exists('/tmp'):
    CACHE_FILE = '/tmp/orbital_data_cache.pkl'
else:
    CACHE_FILE = 'orbital_data_cache.pkl'

df_main = None
df_kpi = None
timestamps = []
search_options = []

def load_data_smart():
    global df_main, df_kpi, timestamps, search_options
    
    if df_main is not None: return 

    today_utc = datetime.now(timezone.utc).date()
    
    if os.path.exists(CACHE_FILE):
        logging.info(f"Checking disk cache: {CACHE_FILE}")
        try:
            cached_data = pd.read_pickle(CACHE_FILE)
            cached_ts = cached_data['timestamps']
            
            if len(cached_ts) > 0 and cached_ts[0].date() == today_utc:
                logging.info("Cache is fresh. Loading into RAM.")
                df_main = cached_data['main']
                df_kpi = cached_data['kpi']
                timestamps = cached_ts
                unique_names = sorted(df_main['Object_Name'].astype(str).unique())
                search_options = [{'label': name, 'value': name} for name in unique_names]
                return
            else:
                logging.warning("Cache is stale. Refreshing...")
                os.remove(CACHE_FILE)
        except Exception as e:
            logging.error(f"Cache read error: {e}")
            if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)

    logging.info("Downloading fresh data from BigQuery...")
    
    try:
        q1 = f"SELECT * FROM `{BIGQUERY_DATASET}.transformed_orbital_satellites_data`"
        df_main = pandas_gbq.read_gbq(q1, project_id=PROJECT_ID)
        
        if isinstance(df_main['Trajectory'].iloc[0], str):
            df_main['Trajectory'] = df_main['Trajectory'].apply(json.loads)
        
        df_main['Avg_Altitude'] = pd.to_numeric(df_main['Avg_Altitude'], errors='coerce')

        q2 = f"SELECT * FROM `{BIGQUERY_DATASET}.orbital_kpis_view`"
        df_kpi = pandas_gbq.read_gbq(q2, project_id=PROJECT_ID)

        timestamps_raw = [x['timestamp'] for x in df_main['Trajectory'].iloc[0]]
        timestamps = pd.to_datetime(timestamps_raw, utc=True)
        
        pd.to_pickle({'main': df_main, 'kpi': df_kpi, 'timestamps': timestamps}, CACHE_FILE)
        logging.info("Download complete and saved to cache.")
        
        unique_names = sorted(df_main['Object_Name'].astype(str).unique())
        search_options = [{'label': name, 'value': name} for name in unique_names]
        
    except Exception as e:
        logging.critical(f"Critical Data Load Failure: {e}")
        sys.exit(1)

load_data_smart()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
server = app.server

owner_counts = df_main['Owner'].value_counts().head(10).reset_index()
owner_counts.columns = ['Owner', 'Count']
fig_owners = px.bar(owner_counts, x='Count', y='Owner', orientation='h', text_auto=True)
fig_owners.update_layout(
    template="plotly_white",
    title=dict(text="<b>Owner Distribution</b>", x=0.02, y=0.98, font=dict(size=20, color="black")),
    yaxis={'categoryorder':'total ascending'},
    margin={"r":20,"t":60,"l":20,"b":20}, 
    height=600 
)
fig_owners.update_traces(marker_color='#3b82f6') 

df_alt = df_main[df_main['Avg_Altitude'] < 40000]
fig_alt = px.histogram(df_alt, x="Avg_Altitude", nbins=50)
fig_alt.update_layout(
    template="plotly_white",
    title=dict(text="<b>Altitude Distribution (km)</b>", x=0.02, font=dict(size=16, color="black")),
    margin={"r":20,"t":50,"l":20,"b":20},
    height=350
)
fig_alt.update_traces(marker_color='#3b82f6') 

fig_inc = px.histogram(df_main, x="Inclination", nbins=50)
fig_inc.update_layout(
    template="plotly_white",
    title=dict(text="<b>Inclination Distribution (°)</b>", x=0.02, font=dict(size=16, color="black")),
    margin={"r":20,"t":50,"l":20,"b":20},
    height=350
)
fig_inc.update_traces(marker_color='#3b82f6')

def get_current_time_index():
    if len(timestamps) == 0: return 0
    now_utc = pd.Timestamp.now(tz='UTC')
    time_diffs = [abs((ts - now_utc).total_seconds()) for ts in timestamps]
    return time_diffs.index(min(time_diffs))

def serve_layout():
    default_time_index = get_current_time_index()
    kpi = df_kpi.iloc[0]

    sidebar = html.Div(
        [
            html.H2("🛰️", className="display-4 text-center"),
            html.H4("Analytics Controls", className="text-center mb-4"),
            html.Hr(),
            
            html.Label("Playback Time (UTC)", className="lead"),
            html.Div(id='time-display-sidebar', className="h4 text-primary text-center mb-3"),
            
            dbc.Button("↺ Sync to Now", id="reset-time-btn", color="secondary", size="sm", className="mb-3 w-100"),
            
            dcc.Slider(
                id='time-slider',
                min=0, max=143, step=1, 
                value=default_time_index, 
                marks={0:'00:00', 36:'06:00', 72:'12:00', 108:'18:00', 143:'23:50'},
                vertical=False,
            ),
            
            html.Hr(className="my-4"),
            
            html.Label("Find Satellite", className="lead"),
            dcc.Dropdown(
                id='satellite-search',
                options=search_options,
                placeholder="Search Object Name...",
                className="mb-3",
            ),
            
            html.Hr(className="mt-4"),
            html.P(f"Objects Tracked: {len(df_main):,}", className="text-muted text-center small")
        ],
        style={
            "position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "20rem",
            "padding": "2rem 1rem", 
            "backgroundColor": "#f8f9fa", 
            "overflowY": "auto",
            "borderRight": "1px solid #dee2e6"
        }
    )

    content = html.Div(
        [
            dbc.Row([
                dbc.Col(html.H1("Space Traffic Analytics", className="text-dark"), width=12),
                dbc.Col(html.P("Daily Orbital Trajectory & Debris Monitoring", className="text-muted lead"), width=12),
            ], className="mb-4 mt-3"),
            

            dbc.Row([
                dbc.Col(dbc.Card(dbc.CardBody([html.H5("Total Objects", className="text-muted"), html.H3(f"{kpi['Total_Objects']:,}", className="text-dark")]), className="shadow-sm border-0"), width=3),
                dbc.Col(dbc.Card(dbc.CardBody([html.H5("Active Payloads", className="text-muted"), html.H3(f"{kpi['Payload_Count']:,}", className="text-success")]), className="shadow-sm border-0"), width=3),
                dbc.Col(dbc.Card(dbc.CardBody([html.H5("Debris Count", className="text-muted"), html.H3(f"{kpi['Debris_Count']:,}", className="text-danger")]), className="shadow-sm border-0"), width=3),
                dbc.Col(dbc.Card(dbc.CardBody([html.H5("Debris Ratio", className="text-muted"), html.H3(f"{kpi['Debris_Ratio_Pct']:.1f}%", className="text-dark")]), className="shadow-sm border-0"), width=3),
            ], className="mb-4"),


            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody(dcc.Graph(id='globe-map', style={"height": "600px"}), className="p-0")
                    ], className="shadow-sm border-0")
                ], width=7),

                dbc.Col([
                    dbc.Card([
                        dbc.CardBody(dcc.Graph(figure=fig_owners, style={"height": "600px"}), className="p-0")
                    ], className="shadow-sm border-0")
                ], width=5)
            ], className="mb-4"),


            dbc.Row([
                dbc.Col(dbc.Card(dcc.Graph(figure=fig_alt), className="shadow-sm border-0 p-1"), width=6),
                dbc.Col(dbc.Card(dcc.Graph(figure=fig_inc), className="shadow-sm border-0 p-1"), width=6),
            ])
        ],

        style={"marginLeft": "22rem", "marginRight": "2rem", "paddingBottom": "4rem"} 
    )

    return html.Div([sidebar, content], style={
        "backgroundColor": "#f4f6f8", 
        "minHeight": "100vh",
        "position": "absolute", 
        "top": 0, 
        "left": 0, 
        "width": "100%"
    })

app.layout = serve_layout

@callback(
    Output('time-slider', 'value'),
    [Input('reset-time-btn', 'n_clicks')],
    [State('time-slider', 'value')]
)
def reset_slider(n_clicks, current_val):
    if ctx.triggered_id == 'reset-time-btn':
        return get_current_time_index()
    return current_val

@callback(
    [Output('globe-map', 'figure'),
     Output('time-display-sidebar', 'children')],
    [Input('time-slider', 'value'),
     Input('satellite-search', 'value')]
)
def update_map(time_index, search_name):
    if time_index is None: time_index = get_current_time_index()
    
    if df_main is None: load_data_smart()

    ts = timestamps[time_index]
    time_str = ts.strftime("%H:%M UTC")

    lats, lons, colors, sizes, opacities, texts = [], [], [], [], [], []

    for row in df_main.itertuples():
        if search_name and row.Object_Name != search_name:
            continue
        
        try:
            pt = row.Trajectory[time_index]
            lats.append(pt['lat'])
            lons.append(pt['lon'])
            
            if search_name:
                colors.append('#FFD700')
                sizes.append(20)
                opacities.append(1.0)
                texts.append(f"🎯 {row.Object_Name}")
            else:
                orbit = row.Orbit
                if orbit == 'LEO': colors.append('#10b981')
                elif orbit == 'MEO': colors.append('#3b82f6')
                elif orbit == 'GEO': colors.append('#ef4444')
                else: colors.append('#eab308')
                
                sizes.append(2)
                opacities.append(0.7)
                texts.append(f"{row.Object_Name} ({row.Owner})")
        except:
            continue

    fig = go.Figure(go.Scattergeo(
        lon=lons, lat=lats, text=texts,
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

        title=dict(
            text="<b>Globe Overview</b>", 
            x=0.02, 
            y=0.95, 
            font=dict(color="black", size=20) 
        ),
        margin={"r":0,"t":50,"l":0,"b":0}, 
        paper_bgcolor="rgba(0,0,0,0)",
        template="plotly_dark",
    )
    
    return fig, time_str

if __name__ == '__main__':
    logging.info("Starting Dash Server...")
    app.run(port=8050, debug=False)