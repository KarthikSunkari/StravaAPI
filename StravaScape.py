from stravalib.client import Client
import pandas as pd
from datetime import datetime,date
import mysql.connector

mydb = mysql.connector.connect(
    host = 'localhost',
    user = '',
    passwd = '',
    database = 'stravadb'
)

codes = pd.read_csv('convertcsv.csv')   
#contains authentication_codes and DOB
code = codes.iloc[:,1].values
dob = codes.iloc[:,4].values

def calculate_age(born):
    dob = datetime.strptime(born, "%Y-%m-%d")
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

age = []
for key in dob:
    if key!="NULL" :
        age.append(calculate_age(key))
        continue
    age.append("NULL")
power_members = []

def get_data(code,age):

    client = Client()
    client_id = "36317"
    client_secret = "cc0218bf95e50c42048230f75f48a300986f52b1"
    age = age
    
    access_token = client.exchange_code_for_token(client_id=client_id,
                            client_secret=client_secret,
                            code=code)
    client = Client(access_token=access_token)
    athlete = client.get_athlete()
    try:
        weight = float(athlete.weight)
    except:
        weight = -1
    try:
        gender = athlete.sex
    except:
        gender = ''
    try:
        a_type = athlete.athlete_type
    except:
        a_type = ''    
    mycursor = mydb.cursor()
    stream_required = ["time", "distance", "altitude", "velocity_smooth","heartrate", 
                       "cadence", "watts", "moving"]
    
    activity_no = 1
    for activity in client.get_activities():
        if activity_no > 100:
            break
        if activity.type == "Ride" and activity.device_watts == True :
            power_members.append(athlete.firstname + ' -> ' + activity_no.__str__())
            stream = client.get_activity_streams(activity.id,stream_required,resolution = 'low',
                                                 series_type="time") 
            maxdp = len(stream['distance'].data)
            try:
                cadence = stream['cadence'].data
            except:
                cadence = [-1] * maxdp
            try:
                heartrate = stream['heartrate'].data
            except:
                heartrate = [-1] * maxdp
            try:
                altitude = stream['altitude'].data
            except:
                altitude = [-1.414] * maxdp
            for dp in range(0,maxdp):
                sql = "INSERT INTO data_points (age, active_time, altitude, cadence, category, distance, gender, heartrate, moving, speed, watts, weight) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (age, stream['time'].data[dp], altitude[dp], cadence[dp], a_type, stream['distance'].data[dp], gender, heartrate[dp], stream['moving'].data[dp], stream['velocity_smooth'].data[dp], stream['watts'].data[dp].__str__(), weight)
                mycursor.execute(sql, val)
            activity_no = activity_no + 1
        mydb.commit()

person_no = 0
for each_code in code:
    get_data(each_code,age[person_no])
    person_no = person_no + 1