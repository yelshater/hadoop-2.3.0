#!/usr/bin/env python3
#
# Copyright (c) 2010-2012 Cloudera, Inc. All rights reserved.
#
import traceback
 
'''
This script is provided by CMF for hadoop to determine network/rack topology.
It is automatically generated and could be replaced at any time. Any changes
made to it will be lost when this happens.
'''

import os
import sys
import xml.dom.minidom

 
def main():
  MAP_FILE = 'topology.map'
  DEFAULT_RACK = '/default'
  if 'CMF_CONF_DIR' in MAP_FILE:
    # variable was not substituted. Use this file's dir
    MAP_FILE = os.path.join(os.path.dirname(__file__), 'topology.map')
  
  MAP_FILE = '/home/yehia/workspace/research/Hadoop_230/src/test/resources/topology.map'
        

  # We try to keep the default rack to have the same
  # number of elements as the other hosts available.
  # There are bugs in some versions of Hadoop which
  # make the system error out.
  max_elements = 1

  map = dict()

  try:
    mapFile = open(MAP_FILE, 'r')

    dom = xml.dom.minidom.parse(mapFile)
    for node in dom.getElementsByTagName('node'):
      rack = str(node.getAttribute('rack'))
      max_elements = max(max_elements, rack.count('/'))
      map[node.getAttribute('name')] = str(node.getAttribute('rack'))
  except Exception:
    print (traceback.format_exc())
    default_rack = ''.join([ DEFAULT_RACK for _ in range(max_elements)])
    print (default_rack)
    return -1
  
  default_rack = ''.join([ DEFAULT_RACK for _ in range(max_elements)])
  if len(sys.argv)==1:
    print (default_rack)
  else:
    print (' '.join([map.get(i, default_rack) for i in sys.argv[1:]]))
    
  return 0

if __name__ == '__main__':
  sys.exit(main())

