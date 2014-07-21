#!/usr/bin/env python
#encoding:utf-8

import json
import sys

colNames = []
for colName in open("fieldFile"):
    colNames.append(colName.strip())

for line in sys.stdin:
    line = line.strip()

    cols = line.split("\001")

    dic = {}
    i = 0
    for colName in colNames:
        if cols[i]:
            dic[colName] = cols[i]
        i += 1
    
    print json.dumps(dic, ensure_ascii=False)
