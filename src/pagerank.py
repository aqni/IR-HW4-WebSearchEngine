from igraph import *
import sqlite3
from tqdm import tqdm

def generateEdgeListFromDB(dbPath:str,listPath:str):
    con = sqlite3.connect(dbPath)
    cu = con.cursor()
    cu.execute("SELECT DISTINCT suid,duid FROM link")
    print("fetching data...")
    edgeList=cu.fetchall()
    with open(listPath,'w') as f:
        for s,d in tqdm(edgeList):
            f.write(f"{s} {d}\n")
    con.close() 

def calPageRank(Ncol_filename:str)->list:
    print("reading edge list...")
    g=Graph.Read_Ncol(Ncol_filename,names=False)
    print("calculating pagerank scores...")
    #https://igraph.org/python/api/latest/igraph.Graph.html#pagerank
    scores=g.pagerank()
    return scores

def exportScores(scores:list,dbPath:str):
    con = sqlite3.connect(dbPath)
    cu = con.cursor()
    print("building update list...")
    l=[(s,i) for i,s in enumerate(scores,start=1)]
    print("executing sql...")
    cu.executemany("UPDATE url SET score=? WHERE u_id==?",l)
    print("committing...")
    con.commit()
    con.close()

DB_FILE="src/mySpider/data.sqlite"
LIST_FILE="src/edge_list.txt"
if __name__=="__main__":
    generateEdgeListFromDB(DB_FILE,LIST_FILE)
    scores=calPageRank(LIST_FILE)
    print(len(scores))
    print(scores[:10])
    exportScores(scores,DB_FILE)
