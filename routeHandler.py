import json
import pymysql

connection = pymysql.connect(
    host='yba-database.c30igyqguxod.us-west-1.rds.amazonaws.com', 
    user='admin', 
    password='LemonsPoke9264', 
    database='yba'
)
cursor = connection.cursor()
table = " FROM version2 v INNER JOIN games g ON v.gameID = g.gameID"

def compute_aggQuery(agg, stat, event):
    name = event['queryStringParameters']['name']
    playerid = event['queryStringParameters'].get('playerid', None)
    seasons = event['multiValueQueryStringParameters'].get('season', None)
    stages = event['multiValueQueryStringParameters'].get('stage', None)
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
    
    return query

# def lambda_handler(event, context):
#     res_body = {}
#     http_res = {}
#     try:
#         path = event['resource'].split('/')
#         aggregate = path[1]
        
#         stats = event['multiValueQueryStringParameters'].get('stat', None)
#         for stat in stats:
#             query = compute_aggQuery(aggregate, stat, event)
#             cursor.execute(query)
#             result = cursor.fetchone()
#             res_body[f'{aggregate}_{stat}'] = float(result[0])
            
#         http_res['statusCode'] = 200
    
#     except Exception as e:
#         print(f"Error handling route with exception: {e}")
#         http_res['statusCode'] = 500
    
#     http_res['headers'] = {}
#     http_res['headers']['Content-Type'] = "application/json"
#     http_res['body'] = json.dumps(res_body, indent=4)
    
#     return http_res

if __name__ == "__main__":
    print("HI")