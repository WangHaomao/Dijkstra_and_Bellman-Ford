def arrange_edges(edge):
    u,v,w = edge.split(' ')
    if (float(w) < 0):
        return (u, [v + ":" + w]), (v, []), (-1, [])
    return (u,[v+":"+w]),(v,[])

def gr_arrange_edges(edge):
    split_list = edge.split(' ')
    if(split_list[0] !='a'): return []
    else:
        u, v, w = split_list[1],split_list[2],split_list[3]
        if(int(w)<0):
            return (u, [v + ":" + w]), (v, []),(-1,[])
        return (u, [v + ":" + w]), (v, [])
def get_negative_flag(it):
    for i in it:
        # just return first 'unification' in each partition
        if(i[0]==-1):return i
    # if no 'unification' pair
    return (None,None)
def merge_nodes(node1,node2):
    if(node1 == None):
        return node2
    else:
        return node1

def get_graph(filePath,sc):
    data = sc.textFile(filePath,8)
    if('.gr' in filePath):
        nodesMapper = data.flatMap(gr_arrange_edges)
    else:
        nodesMapper = data.flatMap(arrange_edges)

    flag_exist_negative = True if -1 in nodesMapper.mapPartitions(get_negative_flag).collect() else False
    # print(flag_exist_negative)
    graph = nodesMapper.\
        reduceByKey(lambda x, y: x + y).\
        filter(lambda x:x[0]!=-1).sortByKey()
    return graph,flag_exist_negative

from pyspark import SparkContext
from pprint import pprint
if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    # get_graph('data/input_negative.dat',sc)
    a,b = get_graph('data/input_slides.dat', sc)
    pprint(a.collect())
