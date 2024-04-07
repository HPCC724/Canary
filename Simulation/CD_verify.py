import csv
import sys
from functools import reduce


def Coding_switch(src, dst):
    src = '0' + str(src) if src < 10 else str(src)
    dst = '0' + str(dst) if dst < 10 else str(dst)
    return src, dst

def data_read(bidir, data_path):
    data_list = []
    f = open(data_path, "r")
    reader = csv.reader(f)
    for row in reader:
        data_list.append(row)  # [segment_id,success_prob,send_num,lost_num,switches]
    n_path = len(data_list)
    path2prob = [float(d[1]) for d in data_list]
    path2switch = [d[4][1:-1].split(', ') for d in data_list]
    path2len = [len(d[4][1:-1].split(', ')) for d in data_list]
    if bidir:
        n_path *= 2
        path2prob += path2prob
        path2switch += [s[::-1] for s in path2switch]
        path2len += path2len
    link2path = {}  # {str(src)+stc(dst): path_id}, path_id=[0, n-1]
    for i in range(n_path):
        for j in range(len(path2switch[i]) - 1):
            src, dst = Coding_switch(int(path2switch[i][j]), int(path2switch[i][j + 1]))  # 获取2位switch_id
            link_id = src + dst
            if link2path.get(link_id) is None:
                link2path[link_id] = [i]
            else:
                link2path[link_id].append(i)
            if bidir:
                link_id = dst + src
                if link2path.get(link_id) is None:
                    link2path[link_id] = [i]
                else:
                    link2path[link_id].append(i)
    # print("link2path:", link2path)
    path2link = []  # [[link_id_list], [], ...]
    for path in path2switch:
        link_list = []
        for j in range(len(path) - 1):
            src, dst = Coding_switch(int(path[j]), int(path[j + 1]))
            link_list.append(src + dst)
            if bidir:
                link_list.append(dst + src)
        path2link.append(link_list)
    # print("path2link:", path2link)
    return n_path, path2prob, link2path, path2link, path2len

def Coordinate_Descend(Epsilon, Lambda, iteration_num, n_path, path2prob, link2path, path2link, path2len):
    link2x = {} 
    for path_id in range(n_path):
        for link_id in path2link[path_id]:
            if link2x.get(link_id) is None:
                link2x[link_id] = [path2len[path_id] * path2prob[path_id], path2len[path_id]]
            else:
                link2x[link_id][0] += path2len[path_id] * path2prob[path_id]
                link2x[link_id][1] += path2len[path_id]

    link2x = {l: d[0] / d[1] for l, d in link2x.items()}  # {link_id: x_i}
    link_ids = list(link2x.keys())  # [link_id]
    n_link = len(link_ids)

    loss_list = [100]  
    for k in range(iteration_num):
        rm = [-2 * Lambda] * n_link
        sm = [-1 * Lambda] * n_link
        for im in range(n_link):
            for path in link2path[link_ids[im]]:
                p2link = path2link[path]
                mul = reduce(lambda x, y: x * y, [b for a, b in link2x.items() if a in p2link and a != im])
                rm[im] += 2 * (mul ** 2)
                sm[im] += 2 * path2prob[path] * mul
            if rm[im] == 0:
                link2x[link_ids[im]] = 1 if sm[im] > 0 else 0
            else:
                tm = rm[im] / sm[im]
                if rm[im] > 0:
                    link2x[link_ids[im]] = 1 if tm > 1 else 0 if tm < 0 else tm
                else:
                    link2x[link_ids[im]] = 1 if tm <= 1 / 2 else 0
        path2mul = []
        for path_id in range(n_path):
            mul = reduce(lambda x, y: x * y, [link2x[link_id] for link_id in path2link[path_id]])
            path2mul.append(mul)
        regulation = Lambda * sum([link2x[i] * (1 - link2x[i]) for i in link2x.keys()])
        loss = sum([(path2prob[i] - path2mul[i]) ** 2 for i in range(n_path)]) + regulation
        print("abs(loss_last - loss):", abs(loss_list[-1] - loss))  
        if abs(loss_list[-1] - loss) < Epsilon:
            print("end iteration:", k)  
            loss_list.append(loss)
            break
        else:
            loss_list.append(loss)

    return link2x, loss_list


if __name__ == '__main__':
    bidir = False  # True: paths and links are bi-directional
    Epsilon = 0.01  # float: threshold of path error
    Lambda = 1  # tuning parameter of regularization  # 3:->1.6  5:bidir==True->1.8, bidir==False->1.7
    iteration_num = 100  
    ##################################################################################
    if bidir:
        experiment_data_path = "CANARY_path_segment.csv"
    else:
        experiment_data_path = "CANARY_path_segment.csv"
    setting_faulty_link_path = "FaultyLink.csv"
#     # log_path = "log.txt"
#     # log_file = open(log_path, 'w+')
#     # # sys.stdout = log_file

    n_path, path2prob, link2path, path2link, path2len = data_read(bidir, experiment_data_path)

    algorithm_prob, loss = Coordinate_Descend(Epsilon, Lambda, iteration_num, n_path,
                                              path2prob, link2path, path2link, path2len)
    print("algorithm_prob:", algorithm_prob)  


