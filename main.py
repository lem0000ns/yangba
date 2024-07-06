import json
import requests

url = "https://api-nba-v1.p.rapidapi.com"

headers = {
    'x-rapidapi-key': "b990592d51msh5e1029396589d1bp18dd72jsnce5f8c3e8b6c",
    'x-rapidapi-host': "api-nba-v1.p.rapidapi.com"
}

def dumpJson(testFile, data):
    with open(testFile, 'w') as f:
        json.dump(data, f, indent=4)

def getData(endpoint):
    response = requests.get(url + endpoint, headers=headers)
    if (response.status_code == 200):
        return response.json()
    print(f"Could not fetch data with endpoint {endpoint} with status code {response.status_code}")
    return None

def getSeasonStats(lastSzn, stats):
    id = stats['id']
    data = getData("/players/statistics?season={}&id={}".format(lastSzn, id))['response']
    if (data):
        return data
    return None

def main():
    try:
        dumpJson('test1.json', getData("/games?id=1427"))
        with open('playerStats2023.json', 'r') as f:
            data = json.load(f)
        #player's stats from every game this season
        dumpJson('test1.json', data['jarred vanderbilt'])
        #player's stats from individual game this season
        dumpJson('test2.json', data['klay thompson'][0])
    except Exception as e:
        print(f"An error occurred: {e}")
    

if __name__ == "__main__":
    main()