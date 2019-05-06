import sys
sys.path.append('.')
from data_operation import get_graph
import numpy as np
from pyspark import SparkContext
from pyspark_dijkstra import format_result

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
    return (u,(edges,dis,path))

def generate_edges(old_path, old_dis, neighbor):
    # if neighbor != None:
    v_id, weight = neighbor.split(":")
    new_dis = old_dis + float(weight)
    new_path = old_path + '->' + v_id
    return (v_id, ('None', new_dis, new_path))

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
    return (neighbors, distance, path)

def split_nodes_iterative(node):
    nid = node[0]
    distance = node[1][1]
    neighbors = node[1][0]
    path = node[1][2]
    elements = path.split('->')
    if elements[len(elements) - 1] != nid:
        path = path + '->' + nid
    return (nid, (neighbors, distance, path))

def show(ll):
    for i in list(ll):
        print(i)
    print("------one------")


def Bellman_Ford(source,graph,sc):
    # sc = SparkContext()
    nodes = graph.map(lambda node:initial(node,source))
    total_nodes_number = nodes.count()
    count = sc.accumulator(0)
    iterations=0
    nodes = nodes.sortByKey()
    for _ in range(total_nodes_number - 1):
        iterations += 1
        nodes_values = nodes.map(lambda node:node[1])
        probable_ways = nodes_values.filter(lambda nodeDataFilter: nodeDataFilter[0] != None).map(
            lambda nodeData: map(
                lambda neighbor: generate_edges(
                    nodeData[2], nodeData[1], neighbor
                ),nodeData[0]
            )
        ).flatMap(lambda x:x)
        mapper = nodes.union(probable_ways)
        reducer = mapper.reduceByKey(lambda x, y: relax(x, y))
        nodes = reducer.map(lambda node: split_nodes_iterative(node)).sortByKey()
        nodes.count()
        # if oldCount == count.value:
        #     print(oldCount)
        #     break
        # oldCount = count.value

    result = reducer.sortByKey()
    result = result.map(lambda node: format_result(node))
    return result

import datetime
from pprint import pprint
if __name__ == '__main__':
    # sc = SparkContext.getOrCreate()
    # graph, a = get_graph('data/input_slides.dat', sc)
    # print(Bellman_Ford('A', graph, sc))

    sc = SparkContext.getOrCreate()
    # graph,a = get_graph('data/email-Eu-core.txt',sc)
    print(datetime.datetime.now())
    # graph, a = get_graph('data/input5.dat', sc)
    # graph,a = get_graph('data/input_fb.dat', sc)
    graph, a = get_graph('data/tinyData.dat', sc)
    res = Bellman_Ford('1', graph, sc)
    # res.saveAsTextFile('test')
    pprint(res.collect())

    print(datetime.datetime.now())