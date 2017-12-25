#!/usr/bin/env python3

import argparse, csv, sys

from common import *

if len(sys.argv) == 1:
    sys.argv.append('-h')

from common import *

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--nr_bins', type=int, default=int(1e+6))
parser.add_argument('-t', '--threshold', type=int, default=int(10))
parser.add_argument('csv_path', type=str)
parser.add_argument('gbdt_path', type=str)
parser.add_argument('out_path', type=str)
args = vars(parser.parse_args())
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

frequent_feats = read_freqent_feats(args['threshold'])

with open(args['out_path'], 'w') as f:
    for row, line_gbdt in zip(csv.DictReader(open(args['csv_path'])), open(args['gbdt_path'])):
        feats = []
        # feat=['I1-SP1', 'I2-SP1', 'I3-2', 'I4-SP0', 'I5-52', 'I6-1', 'I7-7', 'I8-SP2', 'I9-27', 'I10-SP1',
        #  'I11-SP2', 'I12-', 'I13-SP2', 'C1-68fd1e64', 'C2-80e26c9b', 'C3-fb936136', 'C4-7b4723c4', 'C5-25c83c98', 'C6-7e0ccccf', 'C7-de7995b8', 'C8-1f89b562', 'C9-a73ee510', 'C10-a8cd5504', 'C11-b2cb9c98', 'C12-37c9c164', 'C13-2824a5f6', 'C14-1adce6ef',
        # 'C15-8ba8b39a', 'C16-891b62e7', 'C17-e5ba7672', 'C18-f54016b9', 'C19-21ddcdc9', 'C20-b1252a9d', 'C21-07b5194c', 'C22-', 'C23-3a171ecb', 'C24-c5c50484', 'C25-e8b83407', 'C26-9727dd16']
        for feat in gen_feats(row):
            field = feat.split('-')[0]
            type, field = field[0], int(field[1:])#type 为特征的类型I或C filed为索引1-39
            if type == 'C' and feat not in frequent_feats:
                feat = feat.split('-')[0]+'less'
            if type == 'C':
                field += 13
            feats.append((field, feat)) #append的内容为元组,(特征的索引，特征对应的值)

        for i, feat in enumerate(line_gbdt.strip().split()[1:], start=1):
            field = i + 39
            feats.append((field, str(i)+":"+feat))

        feats = gen_hashed_fm_feats(feats, args['nr_bins'])
        f.write(row['Label'] + ' ' + ' '.join(feats) + '\n')
