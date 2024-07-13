import json
import boto3
import threading
import random
import time
import requests

bucket_name = "nba-players.bucket"
s3_client = boto3.client('s3', 'us-west-1')

url = "https://api-nba-v1.p.rapidapi.com"

headers = {
    'x-rapidapi-key': "b990592d51msh5e1029396589d1bp18dd72jsnce5f8c3e8b6c",
    'x-rapidapi-host': "api-nba-v1.p.rapidapi.com"
}

def dumpJson(testFile, data):
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

def getSeasonStats(lastSzn, stats):
    try:
        id = stats['id']
        data = getData("/players/statistics?season={}&id={}".format(lastSzn, id))['response']
        return data
    except Exception as e:
        print(f"Could not fetch season stats for this player")
        return None

def dumpS3(dataList, fileName, szn):
    print("K")
    s3_path = f"playerStats{szn}/{fileName}.json"
    save_to_s3 = s3_client.put_object(
        Key=s3_path,
        Bucket=bucket_name,
        Body=(json.dumps(dataList).encode('UTF-8'))
    )

def test_time(func):
    def wrapper(*args, **kwargs):
        start_time = int(time.time())
        res = func(*args, **kwargs)
        print(f"This data dump took {int(time.time()) - start_time} seconds")
        return res
    return wrapper

#startTeamID parameter only needed for threads
def getAllPlayers(szn, playerStats, startTeamID):
    nbaTeamIds = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 38, 40, 41]
    #nbaTeamIds = [29, 30, 31, 38, 40, 41]
    for team in range(startTeamID, startTeamID + 15):
        teamID = nbaTeamIds[team - 1]
        teamName = getData(f"/teams?id={teamID}")['response'][0]['name'].replace(" ", "")
        teamPlayers = getData(f"/players?season={szn}&team={teamID}")
        print("Currently on team " + str(team))
        for p in range(0, len(teamPlayers['response'])):
            stats = teamPlayers['response'][p]
            name = stats['firstname'].lower() + " " + stats['lastname'].lower()
            #stores in hashmap with players' names as keys and their szn stats as values
            sznStats = getSeasonStats(szn, stats)
            if (sznStats):
                playerStats[name] = sznStats
            else:
                print(f'Failed to fetch player {name} data for season {szn}')
        time.sleep(5 * random.random())
        dumpS3(playerStats, teamName, szn)
        playerStats = {}

@test_time
def startDump():
    try:
        playerStats = {}
        threads = []
        lastSeason = getData("/seasons")['response'][-1]
        #running 2 threads concurrently for 30 teams (each thread = 15 teams)
        for i in range(2):
            thread = threading.Thread(target=getAllPlayers, args=(lastSeason, playerStats, 1 + 15 * i))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# def lambda_handler(event, context):
#     try:
#         startDump()
#         return {
#             'statusCode': 200,
#             'body': 'File uploaded successfully'
#         }
#     except Exception as e:
#         print(f'Error uploading file: {str(e)}')
#         return {
#             'statusCode': 500,
#             'body': 'Error uploading file'
#         }

if __name__ == "__main__":
    startDump()