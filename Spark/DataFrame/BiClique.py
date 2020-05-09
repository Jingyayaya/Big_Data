from pyspark.sql import *
from pyspark import SparkContext, SparkConf

results=[]
def biclique_find(L, R, P, Q,neighbor_dict):
    i = 0
    while len(P) != 0:
        x = P[i]
        # i = i + 1
        r_prim = set()
        l_prim = set()
        r_prim = R.union(set((x,)))
        l_prim = neighbor_dict[x].intersection(L)
        l_prim_comp = L - l_prim
        c = set((x,))
        p_prim = []
        q_prim = set()

        is_maximal = True

        for v in Q:
            neighbor1 = neighbor_dict[v].intersection(l_prim)
            if len(neighbor1) == len(l_prim):
                is_maximal = False
                break
            elif len(neighbor1) > 0:
                q_prim = q_prim.union(set((v,)))
            if is_maximal == False:
                break
        if is_maximal == True:
            for v in P:
                if v != x:
                    neighbor = neighbor_dict[v].intersection(l_prim)
                    if len(neighbor) == len(l_prim):
                        r_prim = r_prim.union(set((v,)))

                        s = neighbor_dict[v].intersection(l_prim_comp)
                        if len(s) == 0:
                            c = c.union(set((v,)))
                    elif len(neighbor) > 0:
                        p_prim = list(set(p_prim).union(set((v,))))
            if len(r_prim) > 1:
                clique = []
                clique.append(l_prim)
                clique.append(r_prim)
                results.append(clique)
                # print(l_prim)
                # print(r_prim)
                # print()

            if len(p_prim) != 0:
                biclique_find(l_prim, r_prim, p_prim, q_prim,neighbor_dict)

        Q = Q.union(c)
        P = list(set(P) - c)


    return results

def main():
    spark = SparkSession.builder.appName("pyspark sql").getOrCreate()
    data = spark.read.csv('/data/Batting.csv',inferSchema=True, header=True)
    df = data.filter(data.yearID == '2018').select('playerID', 'teamID')

    players_df= df.select("playerID").distinct()
    teams_df=df.select("teamID").distinct()

    players=set()
    teams=set()

    for p in players_df.collect():
        players.add(p.playerID)

    for t in teams_df.collect():
        teams.add(t.teamID)


    L_set = set(players)

    R_set = set()

    P_set = list(teams)

    Q_set = set()

    connection_dict = dict()


    for team in teams:
        neighbors = df.filter(df.teamID == team).collect()
        n_set=set()
        for p in neighbors:
            n_set.add(p.playerID)
        connection_dict[team] = n_set


    results=biclique_find(L_set, R_set, P_set, Q_set, connection_dict)

    for cliq in results:
        print(str(cliq[0]))
        print(str(cliq[1]))
        print("\n")

    #save(results)


def save(results):
    with open('/home/elham/Desktop/results.txt', 'w') as file:
        for cliq in results:
            file.write(str(cliq[0]))
            file.write(str(cliq[1]))
            file.write("\n")

if __name__ == '__main__':
    main()




