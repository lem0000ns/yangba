import json, boto3, threading, time, requests, pymysql, os
from random import randint
from datetime import datetime, timedelta

#configuration variables
bucket_name = "nba-players.bucket"
s3_client = boto3.client('s3', 'us-west-1')
url = "https://api-nba-v1.p.rapidapi.com"
headers = {
    'x-rapidapi-key': "b990592d51msh5e1029396589d1bp18dd72jsnce5f8c3e8b6c",
    'x-rapidapi-host': "api-nba-v1.p.rapidapi.com"
}
pw = os.getenv('DB_PASSWORD')
connection = pymysql.connect(
    host='yba-database.c30igyqguxod.us-west-1.rds.amazonaws.com', 
    user='admin', 
    password=pw, 
    database='yba'
)

def test_time(func):
    def wrapper(*args, **kwargs):
        start_time = int(time.time())
        res = func(*args, **kwargs)
        print(f"{func} took {int(time.time()) - start_time} seconds")
        return res
    return wrapper

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

#dumps into s3 bucket in ndjson format
def dumpS3(data, s3_path):
    jsonl_string = '\n'.join(json.dumps(item) for item in data)
    s3_client.put_object(
        Key=s3_path,
        Bucket=bucket_name,
        Body=jsonl_string
    )

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

def getSeasonStats(lastSzn, stats):
    try:
        id = stats['id']
        data = getData("/players/statistics?season={}&id={}".format(lastSzn, id))['response']
        if data is not None:
            return data
        return None
    except Exception as e:
        print(f"Could not fetch season stats for this player")
        return None

#gets data in regular json format to be parsed into ndjson format by flattenDump using 2 threads
def getAllPlayers(szn, data, startTeamID):
    nbaTeamIds = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 38, 40, 41]
    for team in range(startTeamID, startTeamID + 15):
        teamID = nbaTeamIds[team - 1]
        teamPlayers = getData(f"/players?season={szn}&team={teamID}")
        print("Currently on team " + str(team))
        for p in range(0, len(teamPlayers['response'])):
            stats = teamPlayers['response'][p]
            name = stats['firstname'].lower() + " " + stats['lastname'].lower()
            #stores in hashmap with players' names as keys and their szn stats as values
            sznStats = getSeasonStats(szn, stats)
            if (sznStats):
                data[name] = sznStats
            else:
                print(f'Failed to fetch player {name} data for season {szn}')
        time.sleep(randint(1,5))

@test_time
def flattenDump(season):
    allStats = []
    data = {}
    try:
        #two threads
        threads = []
        for i in range(2):
            thread = threading.Thread(target=getAllPlayers, args=(season, data, 1 + 15 * i))
            threads.append(thread)
            thread.start()
            time.sleep(5)
        for thread in threads:
            thread.join()

        playerStats = {}
        for player in data:
            for currGame in range(len(data[player])):
                currPlayer = data[player][currGame]
                #filters out players who didn't play / registered 0 minutes
                if currPlayer["points"] is not None and currPlayer["min"] != "0" and currPlayer["min"] != "0:00":
                    playerStats["name"] = currPlayer["player"]["firstname"] + " " + currPlayer["player"]["lastname"]
                    playerStats["playerID"] = currPlayer["player"]["id"]
                    playerStats["gameID"] = currPlayer["game"]["id"]
                    playerStats["season"] = season
                    playerStats["points"] = currPlayer["points"]
                    playerStats["min"] = currPlayer["min"]
                    playerStats["fgm"] = currPlayer["fgm"]
                    playerStats["fga"] = currPlayer["fga"]
                    playerStats["ftm"] = currPlayer["ftm"]
                    playerStats["fta"] = currPlayer["fta"]
                    playerStats["3pm"] = currPlayer["tpm"]
                    playerStats["3pa"] = currPlayer["tpa"]
                    playerStats["reb"] = currPlayer["totReb"]
                    playerStats["ast"] = currPlayer["assists"]
                    playerStats["steals"] = currPlayer["steals"]
                    playerStats["blocks"] = currPlayer["blocks"]
                    playerStats["turnovers"] = currPlayer["turnovers"]
                    allStats.append(playerStats)
                    playerStats = {}
                    
    except Exception as e:
        print(f"Error flattening data with exception: {e}")

    dumpS3(allStats, f"version2/playerStats{season}.json")

#puts game stages in s3 bucket
@test_time
def getGameStages(season):
    allGames = []
    allGames = []
    data = getData(f"/games?season={season}")['response']
    for game in data:
        currGame = {}
        if (game["league"] == "standard"):
            currGame['gameID'] = game['id']
            currGame['stage'] = game['stage']
            gameTime = game['date']['start']
            if len(gameTime) > 10:
                utc_time = datetime.strptime(gameTime, '%Y-%m-%dT%H:%M:%S.%fZ')
                pdt_time = utc_time - timedelta(hours=7)
            else:
                utc_time = datetime.strptime(gameTime, '%Y-%m-%d')
                pdt_time = utc_time - timedelta(days=1)
            currGame['gameDate'] = pdt_time.strftime('%m-%d-%Y')
            allGames.append(currGame)
    dumpS3(allGames, f"gameStages/games{season}.json")

@test_time
def writeMySQL(table, season):
    try:
        cursor = connection.cursor()
        if table == "version2":
            columns = ["name", "playerID", "gameID", "season", "points", "min", "fgm", "fga", "ftm", "fta", "3pm", "3pa", "reb", "ast", "steals", "blocks", "turnovers"]
            key_template = "version2/playerStats%s.json"

        elif table == "games":
            columns = ["gameID", "stage", "gameDate"]
            key_template = "gameStages/games%s.json"

        columns_placeholder = ', '.join(columns)
        values_placeholder = ', '.join(['%s'] * len(columns))

        # get actual data from S3 bucket and transfer to MySQL
        key = key_template % season
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        ndjson_data = obj['Body'].read().decode('utf-8')
        data = [json.loads(line) for line in ndjson_data.splitlines()]
        
        for game in data:
            values = [game.get(col, None) for col in columns]
            insert_query = f"INSERT INTO {table} ({columns_placeholder}) VALUES ({values_placeholder})"
            #ignore player games who played in summer league / another league outside of nba
            try:
                cursor.execute(insert_query, values)
            except Exception as e:
                continue
        
        connection.commit()
    
    except Exception as e:
        print(f"Error connecting with MySQL with exception: {e}")

# def lambda_handler(event, context):
#     try:
#         #gets most recent season
#         season = getData("/seasons")['response'][-1]
        
#         #maps gameids to stage/date in s3 as nba-players.bucket/gameStages/gamesXXXX.json
#         getGameStages(season)
        
#         #gathers all player data from most recent season, puts in s3 as nba-players.bucket/version2/playerStatsXXXX.json
#         flattenDump(season)
        
#         #transfer data to MySQL, updating games and player data in version2 under most recent season
#         writeMySQL("games", season)
#         writeMySQL("version2", season)
        
#         if connection:
#             connection.close()
        
#         return {
#             'statusCode': 200,
#             'body': 'Data uploaded successfully'
#         }
        
#     except Exception as e:
#         return {
#             'statusCode': 500,
#             'body': f'Error uploading file: {e}'
#         }

if __name__ == "__main__":
    print("HI")