import json
from query_utilities import StatComputer, GameComputer, RankComputer

def lambda_handler(event, context):
    from query_utilities import cursor
    res_body = {}
    http_res = {}
    try:
        path = event['resource'].split('/')
        resource = path[1]
        query = ""
        stat = event['queryStringParameters'].get('stat', None)
        aggregate = "sum" if stat == "games" else event['queryStringParameters'].get('agg', None)
        
        if resource == 'stats':
            test = StatComputer(aggregate, event)
            query = test.compute_Query()
            cursor.execute(query)
            result = cursor.fetchone()
            name = test.getName()
            if name:
                res_body['name'] = name
                res_body['edit distance'] = test.getEditDistance()
            res_body[f'{aggregate}_{stat}'] = float(result[0]) if result[0]  else 0
            res_body['query'] = query
        
        elif resource == 'rank':
            test = RankComputer(aggregate, event)
            query = test.compute_Query()
            cursor.execute(query)
            result = cursor.fetchall()

            #converting non-Json serializable Decimal objects to float
            res = []
            cur = {}
            for row in result:
                cur['name'] = row[0]
                cur[f'{aggregate}_{stat}'] = float(row[1]) if row[1] else 0
                res.append(cur)
                cur = {}

            res_body['result'] = res
            res_body['query'] = query
        
        elif resource == 'games':
            test = GameComputer(aggregate, event)
            query = test.compute_Query()
            cursor.execute(query)
            result = cursor.fetchall()

            #combine column names with row values
            columns = [colName[0] for colName in cursor.description]
            name = test.getName()
            if name:
                res_body['name'] = name
                res_body['edit distance'] = test.getEditDistance()

            res_body['games'] = [dict(zip(columns, row)) for row in result]
            res_body['query'] = query

        http_res['statusCode'] = 200
    
    except Exception as e:
        print(f"Error handling route with exception: {e}")
        http_res['statusCode'] = 500
        http_res['error'] = str(e)
    
    http_res['headers'] = {}
    http_res['headers']['Content-Type'] = "application/json"
    http_res['headers']['Access-Control-Allow-Origin'] = "*"
    http_res['body'] = json.dumps(res_body, indent=4)
    
    # return http_res
    return http_res