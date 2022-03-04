import pandas as pd
import numpy as np
import dash
from  dash import dcc
from  dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc
import subprocess

data = pd.read_csv("vx_data.csv")