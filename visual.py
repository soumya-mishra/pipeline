import pandas as pd
import numpy as np
import dash
from  dash import dcc
from  dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc
import subprocess
from jupyter_dash import JupyterDash

data = pd.read_csv("vx_data.csv")

def plot_graph(data):
    
    temp = data[["actions","person_count","age","derived_emotions","audio_reactions","gazes"]]
    heatmap_cols = {"person_count":"No Of Viewers","age":"Age","proximity":"Proximity",
                    "attention":"Attention","derived_emotions":"Emotions"}
    
    # app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
    app = JupyterDash()

    



    contents = dbc.Card([ html.H5("Select the date range ",style={"color":"green"}),
                            dcc.DatePickerRange(id='date-picker-range',
                                                start_date_placeholder_text="Start Date",
                                                end_date_placeholder_text="End Date",
                                                calendar_orientation='vertical',
                                                start_date =  data["datetime"].min(),
                                                end_date  = data["datetime"].max()

                                                ),
                          html.Br(),
                            html.H5("Select a Feature to view line plot ",style={"color":"green"}),
                            dcc.Dropdown(id="ticker_sc",
                                          options=[{"label": x, "value": x} 
                                          for x in list(temp.columns)],
                                          value= list(temp.columns)[1],
                                          clearable=False)



                          ])


    app.layout = html.Div(

        [html.Div(html.H1(children='Viewing Experience Analysis',style={'textAlign': 'center',
                                                                        'backgroundColor':'blue',
                                                                        "color":"white"



                                                                          }),),
    #      html.Hr(),                  
        dbc.Container( 
            [ html.Div( style={'color': 'green', 'fontSize': 18,'textAlign': 'center'},
            children=[html.P("Viewing experience analysis collects all thhe behavioral data of viewer."),
                    html.P(["""VXD streaming service analyszes these behavioral data to know about viewer's interest and
                            experiecne about the content.""",
                            html.Br(),
                            "This helps content owner,director in bettr decision making for a content."]),
                    html.P(["This data would help analyse the behavior of viewer about a content recommendation",
                            ])]),

            dbc.Row([

                dbc.Col(contents,md=4),
                dbc.Col( dcc.Graph(id="id_line",style={"display": "inline-block","border": "3px #5c5c5c solid",
                                      "padding-top": "1px",
                                      "padding-bottom": "1px",
                                      "padding-left": "1px",
                                      "padding-right": "1px","width": "100%",}),md=8)  

            ]),
             
             
              dbc.Row([ 
                  dbc.Col(html.Div([html.H4("HeatMap Visualization",style={'color': 'green'}),
                                      html.Hr(),
                                    html.H5("Select a Feature to view HeatMap ",style={"color":"green"}),
                                    dcc.Dropdown(id="col_tick",
                                    options=[{"label": y[1], "value": y[0]} for y in list(heatmap_cols.items())],
                                    value= list(heatmap_cols.keys())[1],
                                    clearable=False)
                                  
                                  
                                  ]),md=4),
                 
                 
                  dbc.Col(dcc.Graph(id="heatmap",
                                    style={"display": "inline-block","border": "3px #5c5c5c solid",
                                                    "padding-top": "1px",
                                                    "padding-bottom": "1px",
                                                    "padding-left": "1px",
                                                    "padding-right": "1px",
                                                    "width": "100%"}),
             
                                                    md=8  )


    ]) 
            
            ])  ])


    @app.callback(
        Output('id_line', 'figure'),
        [Input('date-picker-range', 'start_date'),
          Input('date-picker-range', 'end_date'),
          Input("ticker_sc","value")])

    def display_plots(start_date,end_date,ticker1):

        filter_df = data.loc[(data["datetime"]>=start_date)&(data["datetime"]<=end_date)]

        fig = px.line(filter_df, x="datetime", y=ticker1, animation_frame="proximity",markers=True,
                      labels={"person_count":"Viewer Counts"})

        fig.update_layout(dict(template='plotly_dark'))
        return fig

    
    @app.callback(
    Output("heatmap", "figure"), 
    [Input("col_tick", "value")])
             
    def filter_heatmap(col):
#         fig = px.imshow(data[cols].corr())
        fig = px.density_heatmap(data, x="datetime", y=col, nbinsx=20, nbinsy=20, 
                          color_continuous_scale="Viridis",animation_frame="proximity",
                        labels={"derived_emotions:Emotion"})
    
        fig.update_layout(dict(template='plotly_dark'))
        return fig


    app.run_server(port=8050,mode="external")
    print("Execution Started")
    # app._terminate_server_for_port("localhost", 8050)


plot_graph(data)
