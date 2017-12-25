#!/usr/bin/env python3
#统计categorical特征的数量
import argparse, csv, sys, collections

from common import *
if len(sys.argv) == 1:
    sys.argv.append('-h')

parser = argparse.ArgumentParser()
parser.add_argument('csv_path', type=str)
args = vars(parser.parse_args())

counts = collections.defaultdict(lambda : [0, 0, 0]) #括号里面的参数代表当map中的键为空的时候，返回括号里面的函数值

for i, row in enumerate(csv.DictReader(open(args['csv_path'])), start=1):#start代表开始的索引从1开始，即i的值从1开始计数
    label = row['Label']
    for j in range(1, 27):
        field = 'C{0}'.format(j)
        value = row[field]
        if label == '0':
            counts[field+','+value][0] += 1
        else:
            counts[field+','+value][1] += 1
        counts[field+','+value][2] += 1
    if i % 1000000 == 0:
        sys.stderr.write('{0}m\n'.format(int(i/1000000)))

print('Field,Value,Neg,Pos,Total,Ratio')
#按照字段的总个数排序
for key, (neg, pos, total) in sorted(counts.items(), key=lambda x: x[1][2]):  #map.items()将map中的键值组成一个元组放在列表中[('r1', [3, 0, 0])]
    if total < 10:
        continue
    ratio = round(float(pos)/total, 5)
    print(key+','+str(neg)+','+str(pos)+','+str(total)+','+str(ratio))
