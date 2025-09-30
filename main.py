# import package
from fastapi import FastAPI, HTTPException, Header
import psycopg2
import pandas as pd

# create FastAPI object
app = FastAPI()

# api key
password = "postgres"


def getConnection():
    # create connection
    conn = psycopg2.connect(
        dbname="neondb", user="neondb_owner", password="npg_sLfVg8iW4EwO",
        host="ep-steep-water-a102fmjl-pooler.ap-southeast-1.aws.neon.tech",
    )

    return conn

# endpoint - untuk mengambil data halaman utama
@app.get('/')
def getWelcome():
    return {
        "msg": "Hey There"
    }

# endpoint - mengambil data dari database
@app.get('/profiles')
def getProfiles():
    # define connection
    connection = getConnection()

    # get data dari database
    df = pd.read_sql("select * from profiles", connection)

    return {
        # dataframe dalam bentuk dictionary -> .to_dict()
        "data" : df.to_dict(orient = "records")
    }

# endpoint protected
@app.get('/profiles/{id}/{name}')
def getProfileById(id: int, name: str, api_key: str = Header(None)):
    # cek credential
    if api_key == None or api_key != password:
        #kasih error
        raise HTTPException(status_code = 401, detail = "password salah")

    # define connection
    connection = getConnection()

    # get data dari database
    df = pd.read_sql("select * from profiles", connection)

    # filter
    df = df.query(f"id == {id} and name == '{name}'")

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="data not found")
    
    return {
        # dataframe dalam bentuk dictionary -> .to_dict()
        "data": df.to_dict(orient="records"),
        "columns": list(df.columns)
    }


# @app.post(...)
# def createProfile():
#     pass


# @app.patch(...)
# def updateProfile():
#     pass


# @app.delete(...)
# def deleteProfile():
#     pass
