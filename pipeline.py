import pandas as pd
import numpy as np
import random
import datetime
# import time
from faker import Faker
# import radar
# import matplotlib.pyplot as plt
# import seaborn as sns
import warnings
warnings.filterwarnings("ignore")
import string
pd.set_option("display.max_rows", 101)
pd.set_option("display.max_columns", 101)
from faker import Faker
import logging
import psycopg2
from sqlalchemy import create_engine
# Visulaization 
import dash
from  dash import dcc
from  dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc
##***********************************
def generate_emotions(ndata):
    emotions = list()
    for i in range(ndata):
        p = np.random.dirichlet(np.ones(6),size=1) # Generate probability
        p = p.flat
        x = ["happy","sad","angry","fear","surprise","neutral"]
        random.shuffle(x)
        emotions.append(list(zip(p,x)))  # Combine EMotions with probability
    return emotions

# emotions = generate_emotions(10)
# print(emotions)

##**********************************
def generate_vx(ndata):

    username     = list()
    person_count = list()
    age          = list()
    gender       = list()
    proximity    = list()
    emotions     = list()
    actions      = list()
    gazes        = list()
    audio_reactions = list()
    imageUrl      = list()
    audioUrl     = list()
    
    fake = Faker()

    for i in range(ndata):
      
        #Generate User name
        uname = fake.user_name()
        username.append(uname)
        #Generate Person count
        pcount = random.randint(1,5)
        person_count.append(pcount)
        #Generate Age
        user_age = random.randint(10,90)
        age.append(user_age)
        #Generate Gender
        user_gender = random.choice(["male","female"])
        gender.append(user_gender)
        #Generate Proximity
        distance = random.randint(5,10) # 10 should be okay
        proximity.append(distance)
        #Generate Emotions
#         emotion = random.choice(["happy","sad","angry","fear","surprise","neutral"]) ### take it as percentages
#         emotions.append(emotion)
        emotions = generate_emotions(ndata)
        #Generate Gaze
        gaze = random.choice(["looking_straight", "looking_away"])
        gazes.append(gaze)
        #Generate Actions
        action = random.choice(["walking", "talking","jumping","dancing","sitting"])  
        actions.append(action)
        #Generate Audio Reactions
        audio_reaction = random.choice(["scream","cheer","LaughOutLoud"]) # include LOL,
        audio_reactions.append(audio_reaction)
        #Generate Image URL
        img_url = fake.domain_word()+"_"+fake.domain_name()
        imageUrl.append(img_url)
        
        #Generate Audio URL
        a_url = fake.domain_word()+"_"+fake.domain_name()
        audioUrl.append(a_url)
        
    # Convert to json :
    list_zip = list(zip(username,person_count,age,gender, proximity,emotions ,actions,gazes ,audio_reactions,imageUrl,audioUrl))
    vx_data= pd.DataFrame(list_zip,columns=["username","person_count","age","gender", "proximity","emotions" ,"actions","gazes" ,"audio_reactions","imageurl","audiourl"])
    vx_data.to_json("C://Users//7000029183//Desktop//Project//Sony_VXD//POC_Code//vxd.json",orient="records")
    #Return the json data in record format 
    return vx_data.to_json(orient="records")

ndata = 1000
djson = generate_vx(ndata)
vx_data = pd.read_json(djson)
vx_data.head()
vx_data.tail()
#********************************************
def generate_datetime():
    starting_time = datetime.datetime(2021,12,19,8,30,10)
    td = 5  # timedelta
    vx_data['datetime'] = [starting_time + datetime.timedelta(seconds=i*td) for i in range(len(vx_data))]
    #Convert datetime string to Datetime Object
    vx_data["datetime"] = pd.to_datetime(vx_data["datetime"])
    return vx_data

vx_data = generate_datetime()
#***************************************************

#Find Emotions with max probability & assign it to new columns
vx_data["derived_emotions"] = vx_data["emotions"].apply(lambda x:max(x)[1])

#**************************************************

vx_data["attention"] = vx_data["gazes"].apply(lambda x: 1 if x== "looking_straight" else 0)

#*************************************
interval = 30
def generate_contentid(ndata,interval):
    fake = Faker()
    vx_data["contentID"] = None
    for i in range(0,ndata,interval):
        vx_data["contentID"].iloc[i:i+30] =  fake.uuid4()
    return vx_data


vx_data = generate_contentid(ndata,interval)

#******************************************************
def check_null(dataframe):
    print(dataframe.isnull().sum())
    
#***************************************
def fill_null(vx_data):
    cols = vx_data.columns
    for col in cols:
        vx_data[col].fillna(-1)
    return vx_data

#************************************

def check_unique(vx_data):
    print(vx_data["derived_emotions"].unique())
    print("--"*50)
    print(vx_data["actions"].unique())
    print("--"*50)
    print(vx_data["gazes"].unique())
    print("--"*50)
    print(vx_data["audio_reactions"].unique())  


check_unique(vx_data)

#**********************************************************
def check_age(vx_data):
    vx_data.loc[vx_data["age"]<0,"age"] = -1
    return vx_data

vx_data = check_age(vx_data)

#***********************************************************

def check_gender():
    pass

#*********************************************
def check_emotions(vx_data):
    unwanted = string.punctuation + string.whitespace
    vx_data["derived_emotions"] = vx_data["derived_emotions"].str.strip(unwanted)
    return vx_data

#*****************
def check_actions():
    pass

#********************************************

def check_proximity(vx_data):
    vx_data.loc[vx_data["proximity"]<0,"proximity"] = -1
    return vx_data

#******************************************

def check_actions():
    pass

#****************************
def check_gazes():
    pass

#************************************
def removeSpecialChar(dataframe):
    unwanted = string.punctuation + string.whitespace
    columns = ['gender', 'derived_emotions', 'actions', 'audio_reactions']
    
    for col in columns:
        vx_data[col] = vx_data[col].str.strip(unwanted)
    return vx_data

vx_data = removeSpecialChar(vx_data)

#**********************************************

# Data Pipeline ###############

def data_pipeline(vx_data):
    #1. get the data in json or csv
    
    #2.check metadata
    print("Metadata")
    print("---"*20)
    print(f"{vx_data.info()}")
    print("---"*30)
    
    #3. Check basic statistics
    print(f"Numerical variable statistics\n {vx_data.describe()}")
    print("---"*30)
    
   
    print(f"Categorical variable statistics\n {vx_data.describe(include='O')}")
    print("---"*30)
    
    #4.check null values
    print("Check Null Values")
    print("---"*20)
    check_null(vx_data)
    print("---"*30)
    
    #5.Fill null values if any
    print("Check Unique Values")
    print("---"*20)
    vx_data = fill_null(vx_data)
    
    #6.Check unique values in categorical data
    check_unique(vx_data)
    print("---"*30)
    
    #7.remove special character in categorical data
    print("Removed special character")
    vx_data = removeSpecialChar(vx_data)
    
        
    return vx_data
    

vx_data = data_pipeline(vx_data)

#*******************************************
## Data is ready to push to DB
vx_clean_data = data_pipeline(vx_data)

## Rearrange the columns 
columns = ['datetime','contentID','username',
 'person_count',
 'age',
 'gender',
 'proximity',
 'emotions',
 'actions',
 'gazes',
 'audio_reactions',
 'imageurl',
 'audiourl',
 'derived_emotions','attention']

vx_clean_data = vx_clean_data[columns]
# Save the file as csv file
vx_clean_data.to_csv("C://Users//7000029183//Desktop//Project//Sony_VXD//vx_data.csv",index=False)

#****************************
def sync_metada(vx_clean_data):
    vx_clean_data["emotions"] = vx_clean_data["emotions"].astype("string")
    vx_clean_data["attention"] = vx_clean_data["attention"].astype("bool")
    return vx_clean_data

vx_clean_data = sync_metada(vx_clean_data)


#****************************************************
dbname = "postgres"
hostname = "localhost"
port = 5432
username = "postgres"
pword = "admin"

def push_df_to_db(vx_clean_data):
    cur = None
    conn = None
    error = None

    conn_string = "postgresql://postgres:admin@localhost/postgres"
    try:  
        db = create_engine(conn_string)
        conn = db.connect()
        vx_clean_data.to_sql("vxd",schema="sony", con=conn, if_exists='append',index=False)


    except Exception as error:
        error =error
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    if error:
        return error
    else:
        return 0.
    

return_code = push_df_to_db(vx_clean_data)
print(return_code)

#****************************
# Verify the correctnes of pushed data to db
def verify_db_data():

    cur = None
    conn = None
    try:
        conn = psycopg2.connect(host=hostname, port = port, database=dbname, user=username, password=pword)
        cur = conn.cursor()


        sql_query = """SELECT * FROM "sony"."vxd" """
        table = pd.read_sql_query(sql_query, conn)
        print(type(table))
        print(table)

  
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            
    return table

table_data = verify_db_data()

#*******************************************
# Final Visualization 

def plot_graph(data):
    
    temp = data[["actions","person_count","age","derived_emotions","audio_reactions","gazes"]]
    heatmap_cols = {"person_count":"No Of Viewers","age":"Age","proximity":"Proximity",
                    "attention":"Attention","derived_emotions":"Emotions"}
    
    app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

    



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


    app.run_server()




#*************************************************************************************
## Consolidated Pipeline

def data_visual_pipeline():

#1. Data generation Module:
    #Generate 1000 rows
    ndata = 1000
    djson = generate_vx(ndata)
    logging.info("Initial Data generation module executed")
#     print("Initial Data generation module executed")

    
#2. Convert json to Pandas DataFrame:
    vx_data = pd.read_json(djson)
    logging.info("json data converted to DataFrame")
#     print("json data converted to DataFrame")
    
#3. Find Emotions with max probability & assign it to new columns:

    vx_data["derived_emotions"] = vx_data["emotions"].apply(lambda x:max(x)[1])
    logging.info("Derive emotion based on probability")
    
#4. # Generate the time and add it to dataframe.
    vx_data = generate_datetime()
    logging.info("Datetime generation successful")
    
#5. Generate Attention from Gaze 
    vx_data["attention"] = vx_data["gazes"].apply(lambda x: 1 if x== "looking_straight" else 0)
    logging.info("Attention generation successful")
    
#6. Function to Generate & Assign Content ID to DataFrame:
    interval = 30
    vx_data = generate_contentid(ndata,interval)
    logging.info("Content ID Generation successful")
    
    
#7. Data Cleaning Module : 

    vx_clean_data = data_pipeline(vx_data)
    logging.info("Data Cleaning executed successfully")
    
#8.  Rearrange the columns Names for easy interpretations 
    columns = ['datetime','contentID','username',
                 'person_count',
                 'age',
                 'gender',
                 'proximity',
                 'emotions',
                 'actions',
                 'gazes',
                 'audio_reactions',
                 'imageurl',
                 'audiourl',
                 'derived_emotions','attention']

    vx_clean_data = vx_clean_data[columns]
    
    logging.info("Data Formatting successful")
    
#9.  Save the data as csv file
    vx_clean_data.to_csv("C://Users//7000029183//Desktop//Project//Sony_VXD//vx_data.csv",index=False)
    logging.info("DataFrame Saved to csv")

    
#10. Sync Metadata of emotions and attentions .
    vx_clean_data = sync_metada(vx_clean_data)
    logging.info("Datatype Conversion  successful for Emotion & Attention")
    
#11. Push the data to Database.
    # Database configuration 
    dbname = "postgres"
    hostname = "localhost"
    port = 5432
    username = "postgres"
    pword = "admin"
    
    return_code = push_df_to_db(vx_clean_data)
    print(return_code)
    logging.info("Data Pushed to Database")
    
#12. Verify the correctnes of pushed data to db
    # Verify and Fetch the data from Psotgress Database.
    table_data = verify_db_data()
    logging.info("Verify Table data in DB")
    

#13. Convert Table Data to DataFrame for visualization 
    

#14. Visualization Dashboard 
    data = table_data.copy()
    logging.info("Graph library started calling")
    data = data.iloc[:1000,:]
    plot_graph(data)

#*******************
data_visual_pipeline()

#************************