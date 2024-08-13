import boto3, pymysql, os, time
from fuzzywuzzy import fuzz

#uses levenshtein distance to get closest word
#returns array [name, match score (edit distance)]
def getClosestName(name):
    if name in names:
        return name
    currMatch = float("-inf")
    res = ""
    for n in names.splitlines():
        match = fuzz.ratio(n, name)
        if match > currMatch:
            currMatch = match
            res = n

    return res
    
connection = pymysql.connect(
    host='yba-database.c30igyqguxod.us-west-1.rds.amazonaws.com', 
    user='admin', 
    password=os.getenv('SECRETS_MANAGER_CONTRASENA'), 
    database='yba'
)
cursor = connection.cursor()
table = " FROM version2 v INNER JOIN games g ON v.gameID = g.gameID"
s3_client = boto3.client('s3', 'us-west-1')
nameFile = s3_client.get_object(Bucket="nba-players.bucket", Key="names.txt")
names = nameFile['Body'].read().decode('utf-8')

class QueryComputer:
    def __init__(self, agg, event):
        self.agg = agg
        self.stat = event['queryStringParameters'].get('stat', None)
        self.name = event['queryStringParameters'].get('name', None)
        self.editDistanceTime = 0
        if self.name is not None:
            startTime = time.time()
            self.name = getClosestName(self.name)
            self.editDistanceTime = time.time() - startTime
        self.season = event['multiValueQueryStringParameters'].get('season', None)
        self.stage = event['queryStringParameters'].get('stage', None)
        self.filters = event['queryStringParameters'].get('filter', None)
        self.limit = event['queryStringParameters'].get('limit', None)
        self.team = event['queryStringParameters'].get('team', None)
        self.order = event['queryStringParameters'].get('order', None)
        self.sToQ = {'3pct': '3p', 'fgpct': 'fg', 'ftpct': 'ft'}
    
    def getEditDistanceTime(self):
        return self.editDistanceTime
    
    def getName(self):
        return self.name

    def compute_filterQuery(self, rank, filters):
        query = "HAVING" if rank else ""
        first = True
        tokens = filters.split(',')
        #each token formatted as statopx where x is int and op is either <, >, or = e.g. points>30
        for t in tokens:
            
            #find first index of either <, >, or =
            def findOpIndex(t):
                operators = ['<', '>', '=']
                for op in operators:
                    try:
                        return t.index(op)
                    except ValueError:
                        continue
                return -1  # Return -1 if none of the operators are found

            index = findOpIndex(t)

            def helper(stat, first):
                tempQuery = ""
                if not rank:
                    tempQuery += f" AND {stat} {t[index]} {t[index+1:]}"
                else:
                    tempQuery += " AND" if not first else ""
                    if t[0:index] == "games":
                        tempQuery += f" COUNT(*) {t[index]} {t[index+1:]}"
                    else:
                        tempQuery += f" AVG({stat}) {t[index]} {t[index+1:]}"
                return tempQuery

            if t[0:index] != "3pct" and t[0:index] != "fgpct" and t[0:index] != "ftpct":
                query += helper(t[0:index], first)
            else:
                query += helper(f"v.{self.sToQ[t[0:index]]}m/v.{self.sToQ[t[0:index]]}a", first)
            first = False
            
        return query
    
    def compute_Query(self):
        query = ""
        if self.season:
            query += " AND ("
            for szn in self.season:
                query += f"v.season={szn}"
                query += " OR "
            query = query[:len(query) - 4]
            query += ")"
        if self.stage:
            query += f" AND g.stage={self.stage}"

        return query
    
class RankComputer(QueryComputer):
    def __init__(self, agg, event):
        super().__init__(agg, event)
    
    def compute_Query(self):
        if self.stat != '3pct' and self.stat != 'fgpct' and self.stat != 'ftpct':
            query = f"SELECT name, {self.agg}({self.stat})" + table
        else:
            query = f"SELECT name, {self.agg}({self.sToQ[self.stat]}m / {self.sToQ[self.stat]}a)" + table

        query += super().compute_Query()
        if self.team:
            query += f" AND team='{self.team}'"

        query += " GROUP BY name "
        if self.filters:
            query += super().compute_filterQuery(True, self.filters)
        if self.stat != "3pct" and self.stat != "fgpct" and self.stat != "ftpct":
            query += f" ORDER BY {self.agg}({self.stat}) {self.order}"
        else:
            query += f" ORDER BY {self.agg}({self.sToQ[self.stat]}m/{self.sToQ[self.stat]}a) {self.order}"
        query += f" LIMIT {self.limit}"

        return query
    
class GameComputer(QueryComputer):
    def __init__(self, agg, event):
        super().__init__(agg, event)

    def compute_Query(self):
        query = "SELECT v.name, v.playerID, g.gameID, g.stage, g.gameDate, v.team, v.season, v.points, v.min, v.fgm, v.fga, v.ftm, v.fta, v.3pm, v.3pa, v.reb, v.ast, v.steals, v.blocks, v.turnovers, v.OPI" + table
        if self.name:
            query += f" WHERE v.name='{self.name}'"
        
        query += super().compute_Query()
        if self.filters:
            query += super().compute_filterQuery(False, self.filters)
        if self.team:
            query += f" AND team='{self.team}'"
        query += f" LIMIT {self.limit}"

        return query

class StatComputer(QueryComputer):
    def __init__(self, agg, event):
        super().__init__(agg, event)
    
    def compute_Query(self):
        query = ""
        if self.stat == 'games':
            query = "SELECT COUNT(*)" + table;
        elif self.stat != '3pct' and self.stat != 'fgpct' and self.stat != 'ftpct':
            query = f"SELECT {self.agg}({self.stat})" + table;
        else:
            query = f"SELECT {self.agg}({self.sToQ[self.stat]}m / {self.sToQ[self.stat]}a)" + table;
        if self.name:
            query += f" WHERE v.name='{self.name}'"
        
        query += super().compute_Query()
        if self.filters:
            query += super().compute_filterQuery(False, self.filters)
        if self.team:
            query += f" AND team='{self.team}'"
        
        return query