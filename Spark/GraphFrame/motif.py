from pyspark.context import SparkContext
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import monotonically_increasing_id

result = dict()
def calculate(motif):
    teams = motif.groupBy("b", "c").count().filter("count == 1")

    c = motif.join(teams, ["b", "c"], "inner")
    for r in c.collect():
        team1 = r[0].id
        team2 = r[1].id
        key=""
        for i in range(2, len(r)-2):
            key += r[i].id+"_"
        key += r[len(r)-2].id

        if key not in result.keys():
            result[key] = set()

        result[key].add(team1)
        result[key].add(team2)


def main():
    conf = SparkConf().setAppName('Home run count').setMaster('local')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("pyspark sql").getOrCreate()

    batting_path = "hdfs://localhost:8020/user/baseball/Batting.csv"
    batting_path="hdfs://localhost:9000/data/Batting.csv"

    data = spark.read.csv(batting_path, inferSchema=True, header=True)
    df = data.filter(data.yearID == '2018').select('playerID', 'teamID')

    # filter players that play on two or more teams
    players_vertices = df.groupBy("playerID").count().filter("count > 1").select("playerID")

    edges = df.withColumnRenamed("playerID", "src")
    edges = edges.withColumnRenamed("teamID", "dst")
    edges=players_vertices.join(edges, players_vertices.playerID == edges.src, "inner").select("src","dst")

    players_vertices=players_vertices.withColumnRenamed("playerID","id")
    teams_vertices = edges.select("dst").distinct().withColumnRenamed("dst","id")
    vertices = players_vertices.union(teams_vertices)
    # add one column with auto increasing id
    vertices = vertices.withColumn('num', monotonically_increasing_id())

    graph=GraphFrame(vertices,edges)

    # motif 1
    motif = graph.find("(a)-[]->(b); (a)-[]->(c)").filter("c.num > b.num")
    calculate(motif)
    # motif 2
    motif = graph.find("(a)-[]->(b); (a)-[]->(c);(d)-[]->(b); (d)-[]->(c)").filter("c.num > b.num and d.num > a.num")
    calculate(motif)
    # motif 3
    motif = graph.find("(a)-[]->(b); (a)-[]->(c);(d)-[]->(b); (d)-[]->(c);(e)-[]->(b); (e)-[]->(c)").filter(
        "c.num > b.num and d.num > a.num and e.num > d.num").distinct()
    calculate(motif)
    # motif 4
    motif = graph.find(
        "(a)-[]->(b); (a)-[]->(c);(d)-[]->(b); (d)-[]->(c);(e)-[]->(b); (e)-[]->(c);(f)-[]->(b);(f)-[]->(c)").filter(
        "c.num > b.num and d.num > a.num and e.num > d.num and f.num > e.num").distinct()
    calculate(motif)

    output_path = "/user/Wang/graphframe"

    # format the output
    final_result=[]
    for key in result.keys():
        line=""
        key_split=key.split("_")
        for i in range(len(key_split)):
            line += " " + key_split[i]
        for team in result[key]:
            line += " " + team
        final_result.append(line)

    data = sc.parallelize(final_result)
    data.saveAsTextFile(output_path)


if __name__ == '__main__':
    main()


