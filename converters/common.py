import hashlib, csv, math, os, pickle, subprocess

HEADER="Id,Label,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,C16,C17,C18,C19,C20,C21,C22,C23,C24,C25,C26"

def open_with_first_line_skipped(path, skip=True):
    f = open(path)
    if not skip:
        return f
    next(f) #将文件向下读取一行
    return f
#计算特征的MD5值
def hashstr(str, nr_bins):
    return int(hashlib.md5(str.encode('utf8')).hexdigest(), 16)%(nr_bins-1)+1

#处理特征
#feat=['I1-SP1', 'I2-SP1', 'I3-2', 'I4-SP0', 'I5-52', 'I6-1', 'I7-7', 'I8-SP2', 'I9-27', 'I10-SP1',
          #  'I11-SP2', 'I12-', 'I13-SP2', 'C1-68fd1e64', 'C2-80e26c9b', 'C3-fb936136', 'C4-7b4723c4', 'C5-25c83c98', 'C6-7e0ccccf', 'C7-de7995b8', 'C8-1f89b562', 'C9-a73ee510', 'C10-a8cd5504', 'C11-b2cb9c98', 'C12-37c9c164', 'C13-2824a5f6', 'C14-1adce6ef',
         # 'C15-8ba8b39a', 'C16-891b62e7', 'C17-e5ba7672', 'C18-f54016b9', 'C19-21ddcdc9', 'C20-b1252a9d', 'C21-07b5194c', 'C22-', 'C23-3a171ecb', 'C24-c5c50484', 'C25-e8b83407', 'C26-9727dd16']
def gen_feats(row):
    feats = []
    for j in range(1, 14):
        field = 'I{0}'.format(j)
        value = row[field]
        if value != '':
            value = int(value)
            if value > 2:  #数值特征中，值大于2的进行对数处理
                value = int(math.log(float(value))**2)
            else:
                value = 'SP'+str(value)
        key = field + '-' + str(value)
        feats.append(key)
    for j in range(1, 27):
        field = 'C{0}'.format(j)
        value = row[field]
        key = field + '-' + value
        feats.append(key)
    return feats
#计算经常出现的特征
def read_freqent_feats(threshold=10):
    frequent_feats = set()
    for row in csv.DictReader(open('fc.trva.t10.txt')):
        if int(row['Total']) < threshold:
            continue
        frequent_feats.add(row['Field']+'-'+row['Value'])
    return frequent_feats
###将文件根据线程的个数分割成小的文件
def split(path, nr_thread, has_header):
     #将原始的文件切片分割成每个进程要读取的文件
    def open_with_header_witten(path, idx, header):
        f = open(path+'.__tmp__.{0}'.format(idx), 'w')
        if not has_header:
            return f 
        f.write(header)
        return f
    #计算每个进程计算的行数
    def calc_nr_lines_per_thread():   #wc -l 统计文件的行数
        nr_lines = int(list(subprocess.Popen('wc -l {0}'.format(path), shell=True, 
            stdout=subprocess.PIPE).stdout)[0].split()[0])
        if not has_header:
            nr_lines += 1 
        return math.ceil(float(nr_lines)/nr_thread)

    header = open(path).readline()#读取表头

    nr_lines_per_thread = calc_nr_lines_per_thread()

    idx = 0
    f = open_with_header_witten(path, idx, header)
     #将原始文件分割成小文件
    for i, line in enumerate(open_with_first_line_skipped(path, has_header), start=1):
        if i%nr_lines_per_thread == 0:
            f.close()
            idx += 1
            f = open_with_header_witten(path, idx, header)
        f.write(line)
    f.close()
#处理特征，将categorical特征进行one-hot编码
def parallel_convert(cvt_path, arg_paths, nr_thread):

    workers = []
    for i in range(nr_thread):
        cmd = '{0}'.format(os.path.join('.', cvt_path)) #拼接路径
        for path in arg_paths: #[args['src_path'], args['dst1_path'], args['dst2_path']]
            cmd += ' {0}'.format(path+'.__tmp__.{0}'.format(i))
        worker = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        workers.append(worker)
    for worker in workers:
        worker.communicate()
#将多线程生成的文件合并到一个文件中
def cat(path, nr_thread):
    
    if os.path.exists(path):
        os.remove(path)
    for i in range(nr_thread):
        cmd = 'cat {svm}.__tmp__.{idx} >> {svm}'.format(svm=path, idx=i)
        p = subprocess.Popen(cmd, shell=True)
        p.communicate()
#删除生成的中间临时文件
def delete(path, nr_thread):
    
    for i in range(nr_thread):
        os.remove('{0}.__tmp__.{1}'.format(path, i))

