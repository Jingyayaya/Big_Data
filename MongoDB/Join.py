from pymongo import MongoClient
from hdfs.client import InsecureClient

client=MongoClient()

db=client.baseball

def count_hr():

    # calculate the HR for each player in each year
    # and store the result into a new collection 'playerhc'
    db.batting.aggregate([
        {
            '$match':{'yearID':{'$gt':1900}}
        },
        {
            '$group': {
                '_id': {'playerID': '$playerID', 'yearID': '$yearID'},
                'hits': {'$sum': '$HR'}
            }
        },
        {
            '$out': 'playerhc'
        }
    ])
    # calculate the HR for each team in each year and then store the result into a new collection 'teamhc'
    db.teams.aggregate([
        {
            '$match': {'yearID': {'$gt': 1900}}
        },
        {
            '$group': {
                '_id': {'teamID': '$teamID', 'yearID': '$yearID','franchID':'$franchID'},
                'hits': {'$sum': '$HR'}
            }
        },
        {
            '$out': 'teamhc'
        }
    ])

    # analyze the playerID/teamID/yearID so that the playerID has more home runs than teamID in yearID
    # join this result with people
    # join the result with teamsfranchises
    result = db.playerhc.aggregate([
        {
            '$lookup': {
                'from': 'teamhc',
                'localField': '_id.yearID',
                'foreignField': '_id.yearID',
                'as': 'teams'
            }
        },
        {
            '$unwind': '$teams'
        },
        {
            '$match': {'$expr': {'$gt': ['$hits', '$teams.hits']}}
        },
        {
            '$lookup': {
                'from': 'people',
                'localField': '_id.playerID',
                'foreignField': 'playerID',
                'as': 'playersinfo'
            }
        },
        {
            '$lookup': {
                'from': 'teamsfranchises',
                'localField': 'teams._id.franchID',
                'foreignField': 'franchID',
                'as': 'teamsinfo'
            }
        }
    ])

    return result

results=count_hr()

output_path="/users/mongo/Wang/output"
# create connection with hdfs cluster
hdfs_client = InsecureClient("http://localhost:9870/",user="jingya")
# create a file
hdfs_client.write(output_path, "",overwrite=False,append=False)

# append data into the output
for row in results:
    line = row['teamsinfo'][0]['franchName']+" "+row['playersinfo'][0]['nameFirst']+" "+row['playersinfo'][0]['nameLast']+" "+str(row['_id']['yearID'])
    hdfs_client.write(output_path, line, overwrite=False, append=True)
    hdfs_client.write(output_path, "\n", overwrite=False, append=True)
