# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 08:29:35 2022

@author: 7000029183
"""
import dash
from dash import html
import subprocess
import webbrowser
from jupyter_dash import JupyterDash

app = JupyterDash(__name__)

app.layout = html.Div(children=[
                       html.Div([            
                       html.Button('Apply', id='apply-button', n_clicks=0),
                       html.Div(id='output-container-button', children='Hit the button to update.')
                       
                      ])
                ])                      
                
@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('apply-button', 'n_clicks')])
def run_script_onClick(n_clicks):
    #print('[DEBUG] n_clicks:', n_clicks)
    
    if not n_clicks:
        #raise dash.exceptions.PreventUpdate
        return dash.no_update

    # without `shell` it needs list ['/full/path/python', 'script.py']           
    #result = subprocess.check_output( ['/usr/bin/python', 'script.py'] )  

    # with `shell` it needs string 'python script.py'
    result = subprocess.check_output('python script.py', shell=True)  
    print(result)
    # convert bytes to string
    result = result.decode()  
#     webbrowser.open_new("http://127.0.0.1:8050/")
    
    return result
            
if __name__ == "__main__":
#     webbrowser.open_new("http://127.0.0.1:8050/")
    app.run_server(mode="external")

app._terminate_server_for_port("localhost", 8050)
