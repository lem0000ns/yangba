import json
import pymysql
import boto3
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

#uses levenshtein distance to get closest word
def getClosestName(name):
    nameFile = s3_client.get_object(Bucket="nba-players.bucket", Key="names.txt")
    names = nameFile['Body'].read().decode('utf-8')
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

    return res

def compute_filterQuery(filters):
    query = ""
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
        query += f" AND v.{t[0:index]} {t[index]} {t[index+1:]}"
        
    return query

def compute_Query(agg, event):
    stat = event['queryStringParameters'].get('stat', None)
    name = event['queryStringParameters'].get('name', None)
    if name is not None:
        cursor.execute("SELECT COUNT(*)" + table + f" WHERE v.name = '{name}'")
        if cursor.fetchone()[0] == 0:
            #fuzzy matching in case user input name is not in database
            name = getClosestName(name)
    playerid = event['queryStringParameters'].get('playerid', None)
    seasons = event['queryStringParameters'].get('seasons', None)
    stages = event['multiValueQueryStringParameters'].get('stage', None)
    filters = event['queryStringParameters'].get('filter', None)
    limit = event['queryStringParameters'].get('limit', None)
    
    query = ""
    
    if agg == 'Games':
        query = "SELECT v.name, v.playerID, g.gameID, g.stage, g.gameDate, v.season, v.points, v.min, v.fgm, v.fga, v.ftm, v.fta, v.3pm, v.3pa, v.reb, v.ast, v.steals, v.blocks, v.turnovers" + table
        if name:
            query += f" WHERE v.name='{name}'"
    else:
        if stat != '3pct' and stat != 'fgpct' and stat != 'ftpct':
            query = f"SELECT {agg}({stat})" + table + f" WHERE v.name='{name}'"
        else:
            sToQ = {'3pct': '3p', 'fgpct': 'fg', 'ftpct': 'ft'}
            query = f"SELECT SUM({sToQ[stat]}m) / SUM({sToQ[stat]}a)" + table + f" WHERE v.name='{name}'"
        
    if playerid:
        query += f" AND v.playerid={playerid}"
    if seasons:
        #if seasons is just one season, e.g. 2023
        if len(seasons) == 4:
            query += f" AND v.season={seasons}"
        #if seasons spans multiple seasons, e.g. 2015-2019
        else:
            seasonSplit = seasons.split('-')
            query += f" AND v.season BETWEEN " + seasonSplit[0] + " AND " + seasonSplit[1]
    if stages:
        stages_str = ', '.join(map(str, stages))
        query += f" AND g.stage IN ({stages_str})"
    if filters:
        query += compute_filterQuery(filters)
    if agg == 'Games':
        query += f" LIMIT {limit}"
    
    return query

def lambda_handler(event, context):
    res_body = {}
    http_res = {}
    try:
        path = event['resource'].split('/')
        resource = path[1]
        query = ""
        
        if resource == 'stats':
            stat = event['queryStringParameters'].get('stat', None)
            aggregate = event['queryStringParameters'].get('agg', None)
            query = compute_Query(aggregate, event)
            cursor.execute(query)
            result = cursor.fetchone()
            res_body[f'{aggregate}_{stat}'] = float(result[0])
            
        elif resource == 'games':
            query = compute_Query('Games', event)
            cursor.execute(query)
            result = cursor.fetchall()
            #combine column names with row values
            columns = [colName[0] for colName in cursor.description]
            res_body['games'] = [dict(zip(columns, row)) for row in result]
            res_body['query'] = query

        http_res['statusCode'] = 200
    
    except Exception as e:
        print(f"Error handling route with exception: {e}")
        http_res['statusCode'] = 500
        http_res['error'] = str(e)
    
    http_res['headers'] = {}
    http_res['headers']['Content-Type'] = "application/json"
    http_res['body'] = json.dumps(res_body, indent=4)
    
    return http_res