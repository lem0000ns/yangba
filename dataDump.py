import json, requests, pymysql, os
from utilities import MySQL_Writer, DataDumper, NameDumper, PlayerDumper, GameDumper

#configuration variables
url = "https://api-nba-v1.p.rapidapi.com"
headers = {
    'x-rapidapi-key': "b990592d51msh5e1029396589d1bp18dd72jsnce5f8c3e8b6c",
    'x-rapidapi-host': "api-nba-v1.p.rapidapi.com"
}
connection = pymysql.connect(
    host='yba-database.c30igyqguxod.us-west-1.rds.amazonaws.com', 
    user='admin', 
    password=os.getenv('SECRETS_MANAGER_CONTRASENA'), 
    database='yba'
)

def dumpJson(data, testFile):
    if not isinstance(testFile, str):
        print("testFile parameter must be of type string")
        return False
    try:
        with open(testFile, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        print(f"Could not dump into json file with exception {e}")
        return False

def getData(endpoint):
    try:
        response = requests.get(url + endpoint, headers=headers)
    except Exception as e:
        print(f"Could not fetch data with endpoint {endpoint}")
        return None
    
    if (response.status_code == 200):
        return response.json()
    print(f"Could not fetch data with endpoint {endpoint} with status code {response.status_code}")
    return None

def lambda_handler(event, context):
    try:
        #gets most recent season
        season = getData("/seasons")['response'][-1]

        dd = NameDumper(season)
        dd.dumpData()
        gd = GameDumper(season)
        gd.dumpData()
        pd = PlayerDumper(season)
        pd.dumpData()
        
        #transfer data to MySQL, updating games and player data in version2 under most recent season
        sqlWriter = MySQL_Writer("games", connection, season)
        sqlWriter.transferData()
        sqlWriter.setTable("version2")
        sqlWriter.transferData()
        sqlWriter.normalizeOPI()
        
        if connection:
            connection.close()
        
        return {
            'statusCode': 200,
            'body': 'Data uploaded successfully'
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error uploading file: {e}'
        }
    
if __name__ == "__main__":
    #gets most recent season
    season = 2021

    # dd = NameDumper(season)
    # dd.dumpData()
    # gd = GameDumper(season)
    # gd.dumpData()
    # pd = PlayerDumper(season)
    # pd.dumpData()
    
    #transfer data to MySQL, updating games and player data in version2 under most recent season
    sqlWriter = MySQL_Writer("games", season)
    sqlWriter.transferData()
    print("finished games")
    statWriter = MySQL_Writer("version2", season)
    statWriter.transferData()
    print("finished version2")
    statWriter.normalizeOPI()
    print("finished normalizing")