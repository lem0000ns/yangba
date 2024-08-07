import boto3
import pymysql
from botocore.exceptions import ClientError

def get_secret():
    secret_name = "Mysql/password"
    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return get_secret_value_response['SecretString'][32:46]
    except ClientError as e:
        print(f"Error retrieving password: {e}")
        raise

#uses levenshtein distance to get closest word
#returns array [name, match score (edit distance)]
def getClosestName(name):
    minDistance = float("inf")
    res = ""
    for n in names.splitlines():
        
        #edit distance on leetcode
        def levenshtein(n, name):
            cache = [[float("inf")] * (len(name) + 1) for i in range(len(n) + 1)]
        
            for j in range(len(name) + 1):
                cache[len(n)][j] = len(name) - j
            for i in range(len(n) + 1):
                cache[i][len(name)] = len(n) - i
                
            for i in range(len(n) - 1, -1, -1):
                for j in range(len(name) - 1, -1, -1):
                    if n[i] == name[j]:
                        cache[i][j] = cache[i+1][j+1]
                    else:
                        cache[i][j] = 1 + min(cache[i][j+1], cache[i+1][j], cache[i+1][j+1])
            
            return cache[0][0]

        currDist = levenshtein(n, name)
        if currDist < minDistance:
            minDistance = currDist
            res = n

    return [res, minDistance]

pw = get_secret()
connection = pymysql.connect(
    host='yba-database.c30igyqguxod.us-west-1.rds.amazonaws.com', 
    user='admin', 
    password=pw, 
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
        self.editDistance = None
        if self.name is not None:
            cursor.execute("SELECT COUNT(*)" + table + f" WHERE v.name = '{self.name}'")
            if cursor.fetchone()[0] == 0:
                #fuzzy matching in case user input name is not in database
                fuz = getClosestName(self.name)
                self.name = fuz[0]
                self.editDistance = fuz[1]
        self.seasons = event['queryStringParameters'].get('seasons', None)
        self.stage = event['queryStringParameters'].get('stage', None)
        self.filters = event['queryStringParameters'].get('filter', None)
        self.limit = event['queryStringParameters'].get('limit', None)
        self.team = event['queryStringParameters'].get('team', None)
        self.order = event['queryStringParameters'].get('order', None)
        self.sToQ = {'3pct': '3p', 'fgpct': 'fg', 'ftpct': 'ft'}
    
    def getName(self):
        return self.name

    def getEditDistance(self):
        return self.editDistance

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
        if self.seasons:
            #if seasons is just one season, e.g. 2023
            if len(self.seasons) == 4:
                query += f" AND v.season={self.seasons}"
            #if seasons spans multiple seasons, e.g. 2015-2019
            else:
                seasonSplit = self.seasons.split('-')
                query += f" AND v.season BETWEEN " + seasonSplit[0] + " AND " + seasonSplit[1]
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
        if self.stat != '3pct' and self.stat != 'fgpct' and self.stat != 'ftpct':
            query = f"SELECT {self.agg}({self.stat})" + table + f" WHERE v.name='{self.name}'"
        else:
            query = f"SELECT {self.agg}({self.sToQ[self.stat]}m / {self.sToQ[self.stat]}a)" + table + f" WHERE v.name='{self.name}'"
        
        query += super().compute_Query()
        if self.filters:
            query += super().compute_filterQuery(False, self.filters)
        if self.team:
            query += f" AND team='{self.team}'"
        
        return query