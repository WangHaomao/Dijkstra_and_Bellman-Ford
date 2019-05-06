import sys
sys.path.append('.')
from data_operation import get_graph
import numpy as np
from pyspark import SparkContext

def initial(node,source):
    """
    :param node: current node,<tuple> (uid,[edges])
    :param source: begin node <str>
    :return:
    """
    u = node[0]
    dis = np.inf
    edges = node[1]
    path = str(u)
    if(u == source): dis = 0
    return (u,(edges,dis,path,0))

def relax(value1, value2):
    if value1[0] != 'None':
        neighbors = value1[0]
    else:
        neighbors = value2[0]
    dist1 = value1[1]
    dist2 = value2[1]
    if dist1 <= dist2:
        distance = dist1
        path = value1[2]
    else:
        distance = dist2
        path = value2[2]
    flag1 = value1[3]
    flag2 = value1[3]

    return (neighbors, distance, path ,flag1|flag2)
    # return (neighbors, distance, path ,0)

def split_nodes_iterative(node,uid):
    nid = node[0]
    distance = node[1][1]
    neighbors = node[1][0]
    path = node[1][2]
    elements = path.split('->')
    if elements[len(elements) - 1] != nid:
        path = path + '->' + nid
    if (nid == uid):flag = 1
    else: flag = node[1][3]
    return (nid, (neighbors, distance, path,flag))


def format_result(node):
    nid = node[0]
    minDistance = node[1][1]
    path = node[1][2]
    return nid, minDistance, path

def show(ll):
    for i in list(ll):
        print(i)
    print("------one------")

def generate_edges(u_node,neighbor):
    # u_id = u_node[0]
    old_path = u_node[1][2]
    old_dis = u_node[1][1]
    # if neighbor != None:
    v_id, weight = neighbor.split(":")
    new_dis = old_dis + float(weight)
    new_path = old_path + '->' + v_id
    return (v_id, ('None', new_dis, new_path,0))

def Dijkstra(source,graph,sc):
    # sc = SparkContext()
    nodes = graph.map(lambda node:initial(node,source))
    nodes_count = nodes.count()
    for _ in range(nodes_count):
        candidate_nodes = nodes.filter(lambda node:node[1][3] == 0)
        min_node = candidate_nodes.min(lambda node:node[1][1])
        all_edges = sc.parallelize(map(
            lambda neighbor: generate_edges(min_node,neighbor),min_node[1][0]
        ))
        mapper = nodes.union(all_edges)
        # show(mapper.collect())
        reducer = mapper.reduceByKey(lambda x, y: relax(x, y))
        # show(reducer.collect())
        nodes = reducer.map(lambda node: split_nodes_iterative(node,min_node[0]))

    result = reducer.sortByKey()
    result = result.map(lambda node: format_result(node))

    return result

from pprint import pprint
import datetime

if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    # graph,a = get_graph('data/email-Eu-core.txt',sc)
    print(datetime.datetime.now())
    graph, a = get_graph('data/input5.dat', sc)
    # graph,a = get_graph('data/input_fb.dat', sc)
    # graph, a = get_graph('data/tinyData.dat', sc)
    res = Dijkstra('1', graph, sc)
    # res.saveAsTextFile('test')
    pprint(res.collect())

    print(datetime.datetime.now())