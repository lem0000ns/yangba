import json
import pymysql
import boto3
import re
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

def compute_aggQuery(agg, stat, event):
    name = event['queryStringParameters']['name']
    playerid = event['queryStringParameters'].get('playerid', None)
    seasons = event['multiValueQueryStringParameters'].get('season', None)
    stages = event['multiValueQueryStringParameters'].get('stage', None)
    filters = event['queryStringParameters'].get('filter', None)
    
    query = ""
    
    if stat != '3pct' and stat != 'fgpct' and stat != 'ftpct':
        query = f"SELECT {agg}({stat})" + table + f" WHERE v.name='{name}'"
    else:
        sToQ = {'3pct': '3p', 'fgpct': 'fg', 'ftpct': 'ft'}
        query = f"SELECT SUM({sToQ[stat]}m) / SUM({sToQ[stat]}a)" + table + f" WHERE v.name='{name}'"
        
    if playerid:
        query += f" AND v.playerid={playerid}"
    if seasons:
        seasons_str = ', '.join(map(str, seasons))
        query += f" AND v.season IN ({seasons_str})"
    if stages:
        stages_str = ', '.join(map(str, stages))
        query += f" AND g.stage IN ({stages_str})"
        
    if filters:
        query += compute_filterQuery(filters)
    
    return query

def lambda_handler(event, context):
    res_body = {}
    http_res = {}
    try:
        path = event['resource'].split('/')
        resource = path[1]
        
        if resource == 'stats':
            stat = event['queryStringParameters'].get('stat', None)
            aggregate = event['queryStringParameters'].get('agg', None)
            query = compute_aggQuery(aggregate, stat, event)
            cursor.execute(query)
            result = cursor.fetchone()
            res_body[f'{aggregate}_{stat}'] = float(result[0])
            res_body['query'] = query

        http_res['statusCode'] = 200
    
    except Exception as e:
        print(f"Error handling route with exception: {e}")
        http_res['statusCode'] = 500
    
    http_res['headers'] = {}
    http_res['headers']['Content-Type'] = "application/json"
    http_res['body'] = json.dumps(res_body, indent=4)
    
    return http_res