#coding=utf-8
import argparse, csv, sys, collections
from common import *
import hashlib, math, os, pickle, subprocess
# res=csv.DictReader(open("../tr.csv"))
# print  res
count=0
# map={"r1":[3, 0, 0]}
# print map.items()

# list=[("r1",3),("r2",1),("r3",7)]
# print sorted(list,key=lambda x: x[0])
# for i, row in enumerate(res, start=0):
#     print i,row
#     if(count==3):
#         break
#     count=count+1
# target_cat_feats = ['C9-a73ee510', 'C22-', 'C17-e5ba7672', 'C26-', 'C23-32c7478e', 'C6-7e0ccccf', 'C14-b28479f6', 'C19-21ddcdc9', 'C14-07d13a8f', 'C10-3b08e48b', 'C6-fbad5c96', 'C23-3a171ecb', 'C20-b1252a9d', 'C20-5840adea', 'C6-fe6b92e5', 'C20-a458ea53', 'C14-1adce6ef', 'C25-001f3601', 'C22-ad3062eb', 'C17-07c540c4', 'C6-', 'C23-423fab69', 'C17-d4bb7bd8', 'C2-38a947a1', 'C25-e8b83407', 'C9-7cc72ec2']
# print len(target_cat_feats)
# with open("te.gbdt.dense", 'w') as f_d, open("te.gbdt.sparse", 'w') as f_s:
#     for row in csv.DictReader(open("../te.csv")):
#         feats = []
#         for j in range(1, 14):
#             val = row['I{0}'.format(j)]
#             if val == '':
#                 val = -10
#             feats.append('{0}'.format(val))
#         f_d.write(row['Label'] + ' ' + ' '.join(feats) + '\n')
#
#         cat_feats = set()
#         for j in range(1, 27):
#             field = 'C{0}'.format(j)
#             key = field + '-' + row[field]
#             cat_feats.add(key)
#
#         feats = []
#         for j, feat in enumerate(target_cat_feats, start=1):
#             if feat in cat_feats:
#                 feats.append(str(j))
#         f_s.write(row['Label'] + ' ' + ' '.join(feats) + '\n')
counts = collections.defaultdict(lambda : [0, 0, 0]) #括号里面的参数代表当map中的键为空的时候，返回括号里面的函数值
#

for i, row in enumerate(csv.DictReader(open("../tr.csv")), start=1):#start代表开始的索引从1开始，即i的值从1开始计数
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

sys.stderr.write('Field,Value,Neg,Pos,Total,Ratio')
#按照字段的总个数排序
for key, (neg, pos, total) in sorted(counts.items(), key=lambda x: x[1][2]):  #map.items()将map中的键值组成一个元组放在列表中[('r1', [3, 0, 0])]
    if total < 10:
        continue
    ratio = round(float(pos)/total, 5)
    print(key+','+str(neg)+','+str(pos)+','+str(total)+','+str(ratio))




#################
#feats=['0:40189:1', '1:498397:1', '2:131438:1', '3:947702:1', '4:205745:1', '5:786172:1',
#  '6:754008:1', '7:514500:1', '8:735727:1', '9:255381:1', '10:756430:1', '11:832677:1',
# '12:120252:1', '13:172672:1', '14:398230:1', '15:98079:1', '16:550602:1', '17:397270:1',
# '18:182671:1', '19:760878:1', '20:241196:1', '21:198788:1', '22:538959:1', '23:295561:1',
# '24:540660:1', '25:391696:1', '26:78061:1', '27:462176:1', '28:433710:1', '29:166818:1',
# '30:755327:1', '31:765122:1', '32:382381:1', '33:758475:1', '34:541960:1', '35:979212:1',
# '36:345058:1', '37:396665:1', '38:254077:1', '39:578185:1', '40:319016:1', '41:394038:1',
#  '42:73083:1', '43:939002:1', '44:821103:1', '45:978607:1', '46:205991:1', '47:186960:1',
# '48:75897:1', '49:593404:1', '50:746562:1', '51:957901:1', '52:950467:1', '53:617299:1',
# '54:5494:1', '55:863412:1', '56:302059:1', '57:728712:1', '58:288818:1', '59:265710:1',
# '60:37395:1', '61:629862:1', '62:760652:1', '63:572728:1', '64:384118:1', '65:360730:1',
# '66:906348:1', '67:249369:1', '68:748254:1']
def gen_hashed_fm_feats(feats, nr_bins):
    feats = ['{0}:{1}:1'.format(field-1, hashstr(feat, nr_bins)) for (field, feat) in feats]
    return feats
frequent_feats = read_freqent_feats()

with open('tr.ffm', 'w') as f:
    for row, line_gbdt in zip(csv.DictReader(open('tr.csv')), open('tr.gbdt.out')):
        feats = []
         #feat=['I1-SP1', 'I2-SP1', 'I3-2', 'I4-SP0', 'I5-52', 'I6-1', 'I7-7', 'I8-SP2', 'I9-27', 'I10-SP1',
          #  'I11-SP2', 'I12-', 'I13-SP2', 'C1-68fd1e64', 'C2-80e26c9b', 'C3-fb936136', 'C4-7b4723c4', 'C5-25c83c98', 'C6-7e0ccccf', 'C7-de7995b8', 'C8-1f89b562', 'C9-a73ee510', 'C10-a8cd5504', 'C11-b2cb9c98', 'C12-37c9c164', 'C13-2824a5f6', 'C14-1adce6ef',
         # 'C15-8ba8b39a', 'C16-891b62e7', 'C17-e5ba7672', 'C18-f54016b9', 'C19-21ddcdc9', 'C20-b1252a9d', 'C21-07b5194c', 'C22-', 'C23-3a171ecb', 'C24-c5c50484', 'C25-e8b83407', 'C26-9727dd16']
        for feat in gen_feats(row):
            field = feat.split('-')[0]
            type, field = field[0], int(field[1:]) #type 为特征的类型I或C filed为索引1-39
            if type == 'C' and feat not in frequent_feats:
                feat = feat.split('-')[0]+'less'
            if type == 'C':
                field += 13
            feats.append((field, feat))  #append的内容为元组,(特征的索引，特征对应的值)

        for i, feat in enumerate(line_gbdt.strip().split()[1:], start=1):
            field = i + 39
            feats.append((field, str(i)+":"+feat))

        feats = gen_hashed_fm_feats(feats, int(1e+6))
        f.write(row['Label'] + ' ' + ' '.join(feats) + '\n')
