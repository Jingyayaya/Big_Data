import sys
from pyspark import SparkContext, SparkConf

# Map the lines from Batting.csv to new tuple
def map_player_hr(line):
    line_splits=line.split(",")
    player_id=line_splits[0]
    year_id=line_splits[1]
    hr=line_splits[11]
    new_key=player_id+"_"+year_id
    return (new_key, (player_id,year_id, int(hr)))

# Map the lines from Teams.csv to new tuple
def map_team_hr(line):
    line_splits=line.split(",")
    team_id=line_splits[2]
    year_id=line_splits[0]
    franch_id=line_splits[3]
    hr=line_splits[19]
    new_key=team_id+"_"+year_id
    return (new_key, (team_id, year_id, int(hr),franch_id))

# Map the lines from People.csv to new tuple
def map_playerid_name(line):
    line_splits=line.split(",")
    playerid=line_splits[0]
    name_first=line_splits[13]
    name_last=line_splits[14]

    return (playerid, (name_first, name_last))

def compre_join():
    print()

def main():
    # Set the spark cluster as local
    conf = SparkConf().setAppName('Home run count').setMaster('local')
    sc = SparkContext(conf=conf)

    # Get four parameters from the command line
    batting_path=sys.argv[1]
    teams_path=sys.argv[2]
    people_path=sys.argv[3]
    franch_path=sys.argv[4]

    # calculate the number of home runs for each player in each year
    player_hr=sc.textFile(batting_path).filter(lambda line: line.split(",")[11].isnumeric())\
        .map(map_player_hr)\
        .reduceByKey(lambda x, y: (x[0],x[1],x[2]+y[2]))\
        .filter(lambda x: x[1][2]>0)\
        .map(lambda x: (x[1][1],(x[1][0],x[1][2])))

    # calculate the number of home runs for each team in each year
    team_hr=sc.textFile(teams_path).filter(lambda line: line.split(",")[19].isnumeric())\
        .map(map_team_hr)\
        .reduceByKey(lambda x,y: (x[0],x[1],x[2]+y[2],x[3]))\
        .map(lambda x: (x[1][1],(x[1][0],x[1][2],x[1][3])))

    compare_result=player_hr.join(team_hr)\
        .filter(lambda x: x[1][0][1]>x[1][1][1])\
        .map(lambda x: (x[1][0][0],(x[1][1][2],x[0])))

    # Read People.csv
    player_id_name = sc.textFile(people_path).map(map_playerid_name)
    # join with People.csv
    join_playername=compare_result.join(player_id_name).map(lambda x: (x[1][0][0],(x[1][0][1], x[1][1][0],x[1][1][1])))

    # Read TeamsFranchises.csv
    teams_franchises = sc.textFile(franch_path).map(lambda line: (line.split(",")[0], line.split(",")[1]))
    # join with TeamsFranchises.csv
    join_teamname=join_playername.join(teams_franchises)

    result=join_teamname.map(lambda x: (x[1][1], x[1][0][1],x[1][0][2],x[1][0][0])).filter(lambda line: int(line[3])> 1900)

    output_path="/user/Wang/spark"

    # format the output and save the RDD into the "/user/Wang/spark" in hdfs
    result.map(lambda row: str(row[0])+" "+str(row[1]+" "+str(row[2]+" "+str(row[3]))))\
        .saveAsTextFile(output_path)

    sc.stop()

# main function
if __name__ == '__main__':
    main()