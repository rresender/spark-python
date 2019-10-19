import sys
import re
from operator import add

from pyspark import SparkContext


def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub('-', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower().split(' ')

def pass_filter(x):
    return (len(x) > 0 or x != " " or x != None)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: wordcount <master> <file>"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonWordCountSorted")
    lines = sc.textFile(sys.argv[2], 1)
    counts = lines.flatMap(map_phase) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .filter(pass_filter)
    stopwords=sc.textFile("stopwords_en.txt")
    stops=stopwords.flatMap(lambda x: x.split("\n")).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)  
    counts=counts.subtractByKey(stops)                
                  
    output = counts.map(lambda (k,v): (v,k)).sortByKey(False).collect()
    f= open(sys.argv[2]+".result", "w+")
    for (count, word) in output:
        msg = "%i: %s" % (count, word.encode('ascii', 'ignore'))
        print msg
        f.write(msg)
        f.write("\n")
    f.close()
