import sys
sys.path.append('.')
from pyspark import SparkContext
from data_operation import get_graph
from pyspark_dijkstra import Dijkstra
from pyspakr_bellman_ford import Bellman_Ford
import numpy as np
from pprint import pprint

if __name__ == '__main__':
    input_info = sys.argv
    input_len = len(input_info)
    if(input_len < 2):
        raise ValueError('at least one input file(graph) required')
    else:
        graph_path = sys.argv[1]
        sc = SparkContext.getOrCreate()
        print(sys.argv[1])
        graph,exist_negative = get_graph(graph_path,sc)

        if(exist_negative):
            res = Bellman_Ford('1',graph,sc)
            # pass
        else:
            res = Dijkstra('1',graph,sc)

        if(input_len ==3):
            res.saveAsTextFile(sys.argv[2])
        else:
            pprint(res.collect())

