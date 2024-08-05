import boto3, json, threading, traceback, math, time
from datetime import datetime, timedelta
from random import randint

#configuration variables
s3_client = boto3.client('s3', 'us-west-1')
bucket_name = "nba-players.bucket"
allNames = set()

def test_time(func):
    def wrapper(*args, **kwargs):
        start_time = int(time.time())
        res = func(*args, **kwargs)
        print(f"{func} took {int(time.time()) - start_time} seconds")
        return res
    return wrapper

class MySQL_Writer:
    def __init__(self, table, season):
        self.table = table
        self.season = season
        if self.table == "version2":
            self.columns = ["name", "playerID", "gameID", "team", "season", "points", "min", "fgm", "fga", "ftm", "fta", "3pm", "3pa", "reb", "ast", "steals", "blocks", "turnovers", "OPI"]
            self.key_template = "version2/playerStats%s.json"
        elif self.table == "games":
            self.columns = ["gameID", "stage", "gameDate", "home", "homePts", "away", "awayPts"]
            self.key_template = "gameStages/games%s.json"
        self.columns_placeholder = ', '.join(self.columns)
        self.values_placeholder = ', '.join(['%s'] * len(self.columns))

    @test_time
    def transferData(self):
        from dataDump import connection
        cursor = connection.cursor()

        key = self.key_template % self.season
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        ndjson_data = obj['Body'].read().decode('utf-8')
        data = [json.loads(line) for line in ndjson_data.splitlines()]

        insert_query = f"INSERT INTO {self.table} ({self.columns_placeholder}) VALUES "
        allValues = []
        for game in data:
            values = [game.get(col, None) for col in self.columns]
            #if OPI is None then don't include it
            if values[18] != None:
                values[0] = '"' + values[0] + '"'
                values[3] = '"' + values[3] + '"'
                allValues.append('(' + ', '.join(map(str, values)) + ')')
            else:
                print(values)
        insert_query += ', '.join(allValues)
        insert_query += " ON DUPLICATE KEY UPDATE "
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in self.columns])
        insert_query += update_clause
        try:
            cursor.execute(insert_query)
            connection.commit()
        except Exception as e:
            print(f"error transferring data to version2 with error: {e}")
        
    @test_time
    def normalizeOPI(self):
        from dataDump import connection
        cursor = connection.cursor()
        if self.table == "version2":
            #gets rid of former nba players from database whose games are in different league
            cursor.execute(f"DELETE FROM version2 WHERE team=\"\" AND season={self.season}")
            cursor.execute(f"SELECT MAX(OPI) FROM version2 WHERE season={self.season}")
            max_OPI = cursor.fetchone()[0]
            cursor.execute(f"SELECT MIN(OPI) FROM version2 WHERE season={self.season}")
            min_OPI = cursor.fetchone()[0]
            cursor.execute(f"UPDATE version2 SET OPI = ABS(ROUND((OPI - {min_OPI}) / ({max_OPI} - {min_OPI}), 5)) WHERE season={self.season}")
            connection.commit()

    def setTable(self, newTable):
        self.table = newTable
    
    def setSeason(self, newSeason):
        self.season = newSeason

class DataDumper:
    def __init__(self, season):
        self.season = season
    
    def setSeason(self, newSeason):
        self.season = newSeason

    #calculating OPI using box score stats and scaling it relative to how many min they played and how many total pts team scored
    def calculateOPI(self, playerStats, gameID):
        from dataDump import connection
        cursor = connection.cursor()

        cursor.execute(f" SELECT home, homePts, away, awayPts FROM games WHERE gameID = {gameID}")
        result = cursor.fetchone()
        if result is None or result[0] == "":
            playerStats["OPI"] = None
        else:
            try:
                if playerStats["team"] == result[0]:
                    totalPts = result[1]
                else:
                    totalPts = result[3]
                playerStats["OPI"] = (playerStats["points"] + 3 * playerStats["3pm"] - playerStats["3pa"] + playerStats["ftm"] - playerStats["fta"] + 2 * playerStats["fgm"] - playerStats["fga"] + 2 * playerStats["ast"] - playerStats["turnovers"])
                minFactor = math.log10(playerStats["min"] + 10)
                playerStats["OPI"] /= float(minFactor * totalPts)
            except Exception as e:
                print(f"Failed to calculate OPI with exception: {e}")

    @test_time
    #gets data in regular json format to be parsed into ndjson format by flattenDump using 2 threads
    def getAllPlayers(self, szn, data, startTeamID, onlyNames):
        from dataDump import getData
        nbaTeamIds = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 38, 40, 41]
        for team in range(startTeamID, startTeamID + 15):
            teamID = nbaTeamIds[team - 1]
            teamPlayers = getData(f"/players?season={szn}&team={teamID}")
            print("Currently on team " + str(team))
            for p in range(0, len(teamPlayers['response'])):
                stats = teamPlayers['response'][p]
                name = stats['firstname'].lower() + " " + stats['lastname'].lower()
                #dumps names in names.txt
                if onlyNames:
                    if name not in allNames:
                        allNames.add(name)
                #stores in hashmap with players' names as keys and their szn stats as values
                else:
                    sznStats = self.getSeasonStats(szn, stats)
                    if (sznStats):
                        data[name] = sznStats
                    else:
                        print(f'Failed to fetch player {name} data for season {szn}')
            time.sleep(randint(1,5))

    def getSeasonStats(self, szn, stats):
        from dataDump import getData
        try:
            id = stats['id']
            data = getData("/players/statistics?season={}&id={}".format(szn, id))['response']
            if data is not None:
                return data
            return None
        except Exception as e:
            print(f"Could not fetch season stats for this player")
            return None
        
    #dumps into s3 bucket in ndjson format
    def dumpS3(self, data, s3_path):
        jsonl_string = '\n'.join(json.dumps(item) for item in data)
        s3_client.put_object(
            Key=s3_path,
            Bucket=bucket_name,
            Body=jsonl_string
        )
    
class NameDumper(DataDumper):
    def __init__(self, season):
        super().__init__(season)

    @test_time
    def dumpData(self):
        nameFile = s3_client.get_object(Bucket=bucket_name, Key="names.txt")
        names = nameFile['Body'].read().decode('utf-8')
        for n in names.splitlines():
            allNames.add(n)

        #adds any new names in most recent season to allNames
        threads = []
        for i in range(2):
            thread = threading.Thread(target=super().getAllPlayers, args=(self.season, allNames, 1 + 15 * i, True))
            threads.append(thread)
            thread.start()
            time.sleep(5)
        for thread in threads:
            thread.join()
            
        s3_client.put_object(Bucket=bucket_name, Key="names.txt", Body="\n".join(allNames), ContentType='text/plain')

class GameDumper(DataDumper):
    def __init__(self, season):
        super().__init__(season)
    
    @test_time
    def dumpData(self):
        allGames = []
        from dataDump import getData
        data = getData(f"/games?season={self.season}")['response']
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
                currGame['home'] = game['teams']['home']['code']
                currGame['homePts'] = game['scores']['home']['points']
                currGame['away'] = game['teams']['visitors']['code']
                currGame['awayPts'] = game['scores']['visitors']['points']
                allGames.append(currGame)

        super().dumpS3(allGames, f"gameStages/games{self.season}.json")

class PlayerDumper(DataDumper):
    def __init__(self, season):
        super().__init__(season)
    
    @test_time
    def dumpData(self):
        try:
            #reading from s3 to get player data
            obj = s3_client.get_object(Bucket=bucket_name, Key=f"version1/allPlayers{self.season}.json")
            json_data = obj['Body'].read().decode('utf-8')
            data = json.loads(json_data)

            playerStats = {}
            allStats = []

            for player in data:
                for currGame in range(len(data[player])):
                    currPlayer = data[player][currGame]
                    #filters out players who didn't play / registered 0 minutes
                    valid = (
                        currPlayer["min"] not in ["--", "-", "0", "0:00", "00:00"]
                        and currPlayer["min"] is not None
                        and currPlayer["points"] is not None
                    )
                    if valid:
                        playerStats["name"] = currPlayer["player"]["firstname"] + " " + currPlayer["player"]["lastname"]
                        playerStats["playerID"] = currPlayer["player"]["id"]
                        gameID = currPlayer["game"]["id"]
                        playerStats["gameID"] = gameID
                        playerStats["team"] = currPlayer["team"]["code"]
                        playerStats["season"] = self.season
                        playerStats["points"] = currPlayer["points"]
                        #if min has : in it e.g. 30:35
                        index = currPlayer["min"].find(':')
                        playerStats["min"] = (int)(currPlayer["min"][:index]) if index != -1 else (int)(currPlayer["min"])
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
                        super().calculateOPI(playerStats, gameID)
                        allStats.append(playerStats)
                        playerStats = {}
            super().dumpS3(allStats, f"version2/playerStats{self.season}.json")
                            
        except Exception as e:
            print(f"Error flattening data with exception: {e}")
            traceback.print_exc()