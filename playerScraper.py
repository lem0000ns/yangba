import requests, wikipedia, json, boto3, pymysql, os, time
from bs4 import BeautifulSoup

s3_client = boto3.client('s3', 'us-west-1')
bucket_name = "nba-players.bucket"

connection = pymysql.connect(
    host='yba-database.c30igyqguxod.us-west-1.rds.amazonaws.com', 
    user='admin', 
    password=os.getenv('SECRETS_MANAGER_CONTRASENA'), 
    database='yba'
)
cursor = connection.cursor()

allNames = []

def test_time(func):
    def wrapper(*args, **kwargs):
        start_time = int(time.time())
        res = func(*args, **kwargs)
        print(f"{func} took {int(time.time()) - start_time} seconds")
        return res
    return wrapper

def wikibot(orig, name, url):
    url_open = requests.get(url)
    soup = BeautifulSoup(url_open.content, 'html.parser')
    table = soup('table', {'class': 'infobox vcard'})
    if len(table) == 0:
        raise Exception("Infobox Vcard is absent")
    
    playerData = {}
    #get playerID
    cursor.execute(f"SELECT playerID FROM version2 WHERE name=\"{name}\" GROUP BY name")
    try:
        playerID = cursor.fetchone()[0]
        allNames.append(name)
    except Exception:
        raise Exception("No name")
    
    playerData["playerID"] = str(playerID)
    playerData["Name"] = name
    for i in table:
        rows = i.find_all('tr')
        rIndex = 0
        while rIndex < len(rows):
            r = rows[rIndex]
            header = r.find('th')

            #get awards
            if header and header.text in ["Career highlights and awards", "honors"]:
                rIndex += 1
                r = rows[rIndex]
                data = r.find('td', class_='infobox-full-data')
                if data:
                    playerData["Awards"] = data.text
                    break

            #get teams
            elif header and header.text == "Career history":
                teams = []
                rIndex += 1
                if rows[rIndex].find('th') and rows[rIndex].find('th').text == "As player:":
                    rIndex += 1
                while rows[rIndex].find('td'):
                    try:
                        curTeam = {}
                        time = rows[rIndex].find('th').text
                        curTeam[time] = rows[rIndex].find('td').text
                        teams.append(curTeam)
                    except Exception as e:
                        rIndex += 1
                    rIndex += 1
                playerData["Teams"] = json.dumps(teams)
                rIndex -= 1
                
            else:
                heading = r.find_all('th')
                detail = r.find_all('td')
                if heading is not None and detail is not None:
                    for x,y in zip(heading, detail):
                        if x.text != "League":
                            playerData[x.text] = y.text
            rIndex += 1
    playerData["Intro"] = wikipedia.summary(orig, sentences=3, auto_suggest=False)

    #insert into MySQL
    columns_placeholder = ["playerID", "Name", "Position", "Born", "Listed height", "Listed weight", "High school", "College", "NBA draft", "Playing career", "Teams", "Awards", "Intro"]
    mySQL_columns = "(playerID, name, pos, born, height, weight, high_school, college, draft, career, teams, awards, intro)"
    values = []
    for c in columns_placeholder:
        if c not in playerData:
            values.append("NULL")
        else:
            if c == "Teams": values.append("'" + playerData[c] + "'")
            elif c == "Intro": 
                playerData[c] = playerData[c].replace("'", "\\'")
                values.append("'" + playerData[c] + "'")
            else: values.append("\"" + playerData[c] + "\"")
    values = "(" + ", ".join(values) + ")"
    cursor.execute(f"INSERT INTO players {mySQL_columns} VALUES {values}")
    return playerData

@test_time
def namesToSQL():
    nameFile = s3_client.get_object(Bucket=bucket_name, Key="allNames.txt")
    names = nameFile['Body'].read().decode('utf-8')

    for name in names.splitlines():
        cursor.execute("SELECT * FROM players WHERE name=\"{name}\"")
        name = " ".join([n.capitalize() for n in name.split(" ")])
        url = "https://en.wikipedia.org/wiki/" + name + " (basketball)"
        try:
            search_results = wikipedia.search(name + " (basketball)")
            if search_results:
                name = search_results[0]
                url = "https://en.wikipedia.org/wiki/" + name
                paren = name.find('(')
                orig = name
                if paren != -1:
                    name = name[:paren-1]
                wikibot(orig, name, url)
        except Exception as e:
            print("Error fetching wiki data for " + name + " with error: " + str(e))
            continue
        connection.commit()