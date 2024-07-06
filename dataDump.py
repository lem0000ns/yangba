import threading
import time
from main import dumpJson, getData, getSeasonStats

def test_time(func):
    def wrapper():
        start_time = int(time.time())
        res = func()
        print(f"This data dump took {int(time.time()) - start_time} seconds")
        return res
    return wrapper

def getAllPlayers(szn, playerStats, startTeamID):
    for team in range(startTeamID, startTeamID + 15):
        teamPlayers = getData(f"/players?season={szn}&team={team}")
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

@test_time
def getData():
    try:
        #seasons = getData("/seasons")['response']
        seasons = [2021, 2022, 2023]
        playerStats = {}
        threads = []
        for szn in seasons:
            print(szn)
            #running 2 threads concurrently for 30 teams (each thread = 15 teams)
            for i in range(2):
                thread = threading.Thread(target=getAllPlayers, args=(szn, playerStats, 1 + 15 * i))
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()
            dumpJson(f'playerStats{szn}.json', playerStats)
            playerStats = {}
            threads = []
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def main():
    getData()

if __name__ == "__main__":
    main()