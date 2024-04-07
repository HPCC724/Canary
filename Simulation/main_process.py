from platform import release
import topo
import flow
import random
import csv
import os
from functools import reduce
import matplotlib.pyplot as plt


class _Main_Process:
    def __init__(self, k=8, path_type="NetBouncer", core_monitor=False, bidir=False, faulty_rate=0.01,loss_rate_tuple=(0.1, 0.2),in_pod=False) -> None:
        self.path_type = path_type
        self.core_monitor = core_monitor
        self.bidir = bidir
        self.faulty_rate = faulty_rate
        self.loss_rate_tuple = loss_rate_tuple
        self.topo = topo._Topo(k=k, path_type=path_type, core_monitor=core_monitor, bidir=bidir, faulty_rate=self.faulty_rate, loss_rate_tuple=self.loss_rate_tuple,in_pod=in_pod)
        self.flows = flow._Flows(self.topo.paths,self.topo.path_segments)
        self.packet = flow._Packet()

    def Switches_to_Segment(self,switch_list):
        segment_str = ""
        for switch_ in switch_list:
            switch_str = self.topo.Code_Switch(switch_, self.topo.switch_id_bit)
            segment_str = segment_str + switch_str
            # if switch_ >= 10:
            #     segment_str = segment_str + str(switch_)
            # else:
            #     segment_str = segment_str + "0" + str(switch_)
        return segment_str

    def Main_Process(self, run_time=0):
        if run_time > 0:
            self.flows.Generate_Flowlist_dataset(run_time=run_time)
        else:
            self.flows.Generate_Flowlist()
        
        segment_list = {}   # key:segment_str , value: [segment_id,success_prob,send_num,lost_num,switch_list,str(switches)]
        path_segment_list = [] 
        for flow in self.flows.flow_list:
            path = flow.path           
            switch_id_list = path.switches
            switch_list = [self.topo.switches[id - 1] for id in switch_id_list]
            if self.core_monitor == True and len(switch_id_list) == 5:
                segment1 = self.Switches_to_Segment(switch_id_list[0:3])
                segment2 = self.Switches_to_Segment(switch_id_list[2:6])
                if self.bidir == False:
                    segment1_reverse = self.Switches_to_Segment(switch_id_list[2::-1])
                    segment2_reverse = self.Switches_to_Segment(switch_id_list[4:1:-1])
                    if segment_list.get(segment1) == None:
                        if segment_list.get(segment1_reverse) == None:
                            segment_id = self.topo.path_segments[segment1][0]
                            #segment_list[segment1] = [segment_id, -1, 0, 0, switch_id_list[:3]]
                            temp_switch_list = []
                            for s_id in switch_id_list[:3]:
                                temp_switch_list.append(self.topo.Code_Switch(s_id, self.topo.switch_id_bit))
                            segment_list[segment1] = [segment_id, -1, 0, 0, temp_switch_list, segment1]
                        else:
                            segment1 = segment1_reverse
                    if segment_list.get(segment2) == None:
                        if segment_list.get(segment2_reverse) == None:
                            segment_id = self.topo.path_segments[segment2][0]
                            #segment_list[segment2] = [segment_id, -1, 0, 0, switch_id_list[2:]]
                            temp_switch_list = []
                            for s_id in switch_id_list[2:]:
                                temp_switch_list.append(self.topo.Code_Switch(s_id, self.topo.switch_id_bit))
                            segment_list[segment2] = [segment_id, -1, 0, 0, temp_switch_list, segment2]
                        else:
                            segment2 = segment2_reverse
                else:
                    if segment_list.get(segment1) == None:
                        segment_id = self.topo.path_segments[segment1][0]
                        temp_switch_list = []
                        for s_id in switch_id_list[:3]:
                            temp_switch_list.append(self.topo.Code_Switch(s_id, self.topo.switch_id_bit))
                        segment_list[segment1] = [segment_id, -1, 0, 0, temp_switch_list, segment1]
                    if segment_list.get(segment2) == None:
                        segment_id = self.topo.path_segments[segment2][0]
                        temp_switch_list = []
                        for s_id in switch_id_list[2:]:
                            temp_switch_list.append(self.topo.Code_Switch(s_id, self.topo.switch_id_bit))
                        segment_list[segment2] = [segment_id, -1, 0, 0, temp_switch_list, segment2]
                loss_rate_list = []
                for link_id in flow.path.links:
                    loss_rate_list.append(self.topo.links[link_id].loss_rate)
                for i in range(flow.size):
                    loss_flag = False
                    core_flag = False   
                    self.packet.New_Packet(flow.flowID)
                    packet = self.packet
                    for j in range(len(loss_rate_list)):
                        temp_switch = switch_list[j]
                        packet = temp_switch.Receive_Packet(packet)
                        if temp_switch.monitor == True and j != 0:
                            core_flag = True
                        if self.Packet_Loss(loss_rate_list[j]) == True:
                            loss_flag = True
                            break
                    if loss_flag == True:
                        flow.path.lost_num += 1
                    else:
                        packet = switch_list[-1].Receive_Packet(packet)
                    flow.path.send_num += 1
                    if core_flag == False:
                      
                        segment_list[segment1][2] += 1
                        segment_list[segment1][3] += 1
                    elif core_flag == True and loss_flag == False:
                       
                        segment_list[segment1][2] += 1
                        segment_list[segment2][2] += 1
                    else:
                       
                        segment_list[segment1][2] += 1
                        segment_list[segment2][2] += 1
                        segment_list[segment2][3] += 1
            else:
                segment = self.Switches_to_Segment(switch_id_list)
                if segment_list.get(segment) == None:
                    segment_id = self.topo.path_segments[segment][0]
                    temp_switch_list = []
                    for s_id in switch_id_list:
                        temp_switch_list.append(self.topo.Code_Switch(s_id, self.topo.switch_id_bit))
                    segment_list[segment] = [segment_id, -1, 0, 0, temp_switch_list, segment]
                loss_rate_list = []
                for link_id in flow.path.links:
                    loss_rate_list.append(self.topo.links[link_id].loss_rate)
                for i in range(flow.size):
                    loss_flag = False
                    self.packet.New_Packet(flow.flowID)
                    packet = self.packet
                    for j in range(len(loss_rate_list)):
                        temp_switch = switch_list[j]
                        packet = temp_switch.Receive_Packet(packet)
                        if self.Packet_Loss(loss_rate_list[j]) == True:
                            loss_flag = True
                            break
                    if loss_flag == True:
                        flow.path.lost_num += 1
                        segment_list[segment][3] += 1
                    else:
                        packet = switch_list[-1].Receive_Packet(packet)
                    flow.path.send_num += 1
                    segment_list[segment][2] += 1
             
        for temp_key in segment_list:
            temp_value = segment_list[temp_key]
            temp_value[1] = float(temp_value[2]-temp_value[3]) / float(temp_value[2])
            path_segment_list.append(temp_value)

        for path in self.topo.paths:
            path.Update_Success_Prob()


        with open(self.path_type + "_path_segment_simulation.csv", "w+", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(path_segment_list)

    def Main_Process_Segment(self, run_time=0):
        if run_time > 0 :
            self.flows.Generate_Pathsegment_Flowlist_dataset(run_time=run_time)
        else:
            self.flows.Generate_Pathsegment_Flowlist()

        segment_list = self.topo.path_segments
        for temp_key in segment_list:
            temp_value = segment_list[temp_key]
            temp_value[1] = -1
            temp_value[2] = 0
            temp_value[3] = 0
        for flow in self.flows.flow_list:
            segment_temp = flow.path_segment
            switch_id_list = segment_temp[4]
            switch_list = [self.topo.switches[int(id) - 1] for id in switch_id_list]
            loss_rate_list = []
            segment_link_list = [segment_temp[-1][0:(2*self.topo.switch_id_bit)],segment_temp[-1][self.topo.switch_id_bit:(3*self.topo.switch_id_bit)]]
            for link_id in segment_link_list:
                loss_rate_list.append(self.topo.links[link_id].loss_rate)
            for i in range(flow.size):
                loss_flag = False
                core_flag = False
                self.packet.New_Packet(flow.flowID)
                packet = self.packet
                for j in range(len(loss_rate_list)):
                    temp_switch = switch_list[j]
                    packet = temp_switch.Receive_Packet(packet)
                    if temp_switch.monitor == True and j != 0:
                        core_flag = True
                    if self.Packet_Loss(loss_rate_list[j]) == True:
                        loss_flag = True
                        break
                if loss_flag == True:
                    segment_temp[3] += 1
                else:
                    packet = switch_list[-1].Receive_Packet(packet)
                segment_temp[2] += 1
        for temp_key in segment_list:
            temp_value = segment_list[temp_key]
            temp_value[1] = float(temp_value[2]-temp_value[3]) / float(temp_value[2])

        with open(self.path_type + "_path_segment_simulation.csv", "w+", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(list(segment_list.values()))

    def Main_Process_NetBouncer(self):

        self.flows.Generate_Flowlist_NetBouncer()
        for flow in self.flows.flow_list:
            loss_rate_list = []
            for link_id in flow.path.links:
                loss_rate_list.append(self.topo.links[link_id].loss_rate)
            for i in range(flow.size):
                loss_flag = False
                for loss_rate in loss_rate_list:
                    if self.Packet_Loss(loss_rate) == True:
                        loss_flag = True
                        break
                if loss_flag == True:
                    flow.path.lost_num += 1
                flow.path.send_num += 1
        for path in self.topo.paths:
            path.Update_Success_Prob()

        result_list = []
        for path in self.topo.paths:
            temp_switch_list = []
            for s_id in path.switches[:3]:
                temp_switch_list.append(self.topo.Code_Switch(s_id, self.topo.switch_id_bit))
            result_list.append([path.path_id, path.success_prob, path.send_num, path.lost_num, temp_switch_list, self.Switches_to_Segment(path.switches)[:(3*self.topo.switch_id_bit)]])        
        with open(self.path_type + "_path_segment_simulation.csv", "w+", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(result_list)


    def Packet_Loss(self, loss_rate):
        r = random.uniform(0, 1)
        if r <= loss_rate:
            return True
        else:
            return False

    def Coding_switch(self, src, dst):
        if src > dst:  
            src, dst = dst, src
        src = self.topo.Code_Switch(src, self.topo.switch_id_bit)
        dst = self.topo.Code_Switch(dst, self.topo.switch_id_bit) 
        return src, dst

    def Coordinate_Descend(self, n, m, y_j_list, n_j_list, switch_list,
                           bidir=False, iteration_num=100, Epsilon=0.1, Lambda=1):

        link2path = {}  
        for i in range(n):
            for j in range(len(switch_list[i]) - 1):
                src, dst = self.Coding_switch(int(switch_list[i][j]), int(switch_list[i][j + 1]))  
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

        path2link = [] 
        for path in switch_list:
            link_list = []
            for j in range(len(path) - 1):
                src, dst = self.Coding_switch(int(path[j]), int(path[j + 1]))
                link_list.append(src + dst)
                if bidir:
                    link_list.append(dst + src)
            path2link.append(link_list)

        link2x = {} 
        for path_id in range(n):
            for link_id in path2link[path_id]:
                if link2x.get(link_id) is None:
                    link2x[link_id] = [n_j_list[path_id] * y_j_list[path_id], n_j_list[path_id]]
                else:
                    link2x[link_id][0] += n_j_list[path_id] * y_j_list[path_id]
                    link2x[link_id][1] += n_j_list[path_id]
        link2x = {l: d[0] / d[1] for l, d in link2x.items()}  
        link_ids = list(link2x.keys())  
        loss_list = [100] 
        for k in range(iteration_num):  
            rm = [-2 * Lambda] * m  
            sm = [-1 * Lambda] * m
            for im in range(m):
                for path in link2path[link_ids[im]]:
                    rl, sl = 0, 0  
                    for l in path2link[path]:
                        p2link = path2link[path] 
                        mul = reduce(lambda x, y: x * y, [b for a, b in link2x.items() if a in p2link and a != l])
                        rl += mul ** 2
                        sl += y_j_list[path] * mul
                    rm[im] += rl
                    sm[im] += sl
            tm = [rm[x] / sm[x] for x in range(m)]  

            for i in range(m): 
                if rm[i] == 0:
                    link2x[link_ids[i]] = 1 if sm[i] > 0 else 0
                elif rm[i] > 0:
                    link2x[link_ids[i]] = 1 if tm[i] > 1 else 0 if tm[i] < 0 else tm[i]
                else:
                    link2x[link_ids[i]] = 1 if tm[i] <= 1 / 2 else 0

            path2mul = []  
            for i in range(n):
                mul = 1
                for link_id in path2link[i]:
                    mul *= link2x[link_id]
                path2mul.append(mul)

            regulation = Lambda * sum([link2x[i] * (1 - link2x[i]) for i in link2x.keys()])  
            loss = sum([(y_j_list[i] - path2mul[i]) ** 2 for i in range(n)]) + regulation  
            print("Loss_difference:", abs(loss_list[-1] - loss))
            if abs(loss_list[-1] - loss) < Epsilon:  
                print("end iteration:", k)
                loss_list.append(loss)
                break
            else:
                loss_list.append(loss)

        return link2x, loss_list

    def MM_Prob_Map(self):

        MM_Prob_list = []  
        segment_id = 0
        monitor_switch = []  # [switch_id if switch is monitor]
        flow_cnt = 0
        for switch in self.topo.switches:
            if switch.monitor:  
                monitor_switch.append(switch.switch_id_str)

        for switch in self.topo.switches:
            if switch.monitor: 
                segment2flow = {} 
                switch.Generate_MM(self.topo.switches)  
                for term in switch.MM:  # [flowID,pathBF,send_num,lost_num]
                    pathBF2switch = [term[1][i:i + 2] for i in range(0, len(term[1]), 2)]  # str->int_list
                    if len(pathBF2switch) < 3:  
                        continue
                    str_ = ''
                    for i in range(len(pathBF2switch)):
                        if str_ != '':
                            str_ += pathBF2switch[i]
                        if pathBF2switch[i] in monitor_switch:
                            if str_ == '':
                                str_ += pathBF2switch[i]
                            elif segment2flow.get(str_) is not None:
                                segment2flow[str_].append(term[0])
                            else:
                                segment2flow[str_] = [term[0]]
                            str_ = pathBF2switch[i]

                segment2prob = {}  
                
                for _, value in segment2flow.items():
                    flow_cnt += len(value)
                
                for term in switch.MM:
                    flow2segment = [s for s, f in segment2flow.items() if term[0] in f]
                    for segment in flow2segment:
                        if segment2prob.get(segment) is not None:
                            segment2prob[segment][0] += term[2]  # send
                            segment2prob[segment][1] += term[3]  # loss
                        else:
                            segment2prob[segment] = term[2:4]

                for s, p in segment2prob.items():
                    prob = (p[0] - p[1]) / p[0]
                    seg_switch_list = [s[j:j + 2] for j in range(0, len(s), 2)]
                    seg_str = ""
                    for ele in seg_switch_list:
                        seg_str = seg_str + str(ele)
                    segment_id += 1
                    MM_Prob_list.append([segment_id, prob, p[0], p[1], seg_switch_list, seg_str])
        with open("CANARY_path_segment.csv", "w+", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(MM_Prob_list)  # [segment_id,success_prob,send_num,lost_num,switches]
        
        # print(flow_cnt)

    def CD_Verify(self, path_type, al_link2prob, th_link2prob, switch_list):
        path2link = []  # [[link_id_list], [], ...]
        for path in switch_list:
            link_list = []
            for j in range(len(path) - 1):
                src, dst = self.Coding_switch(int(path[j]), int(path[j + 1]))
                link_list.append(src + dst)
                if bidir:
                    link_list.append(dst + src)
            path2link.append(link_list)
        # print("path2link:", path2link)

        al_probs = []  
        for link_list in path2link:
            prob = 1
            for link in link_list:
                prob *= al_link2prob[link]
            al_probs.append(prob)
        print("al_probs:", al_probs)

        th_probs = []  
        for link_list in path2link:
            prob = 1
            for link in link_list:
                if th_link2prob.get(link):
                    prob *= (1 - th_link2prob[link])
            th_probs.append(prob)
        print("th_probs:", th_probs)

        ex_probs = []  
        with open(path_type + "_path.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                ex_probs.append(float(row[1]))
        print("ex_probs:", ex_probs)

        verify_list = []
        for al_prob, th_prob, ex_prob, s_list in zip(al_probs, th_probs, ex_probs, switch_list):
            verify_list.append([al_prob, th_prob, ex_prob, '[' + ', '.join(s_list) + ']'])
        with open("CD_verify.csv", "w+", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(verify_list)  # [al_prob, th_prob, ex_prob]

    def Faulty_Link_Diagnose(self, path_type='CANARY', bidir=False, iteration_num=100, Epsilon=0.1, Lambda=1):
        y_list = []  
        if path_type == "NetBouncer":
            f = open("NetBouncer_path.csv", "r")
        else:
            self.MM_Prob_Map()
            f = open("CANARY_path_segment.csv", "r")
        reader = csv.reader(f)
        for row in reader:
            y_list.append(row)  # [segment_id,success_prob,send_num,lost_num,switches]

        x_dict, loss = self.Coordinate_Descend(len(y_list), len(self.topo.links),
                                               [float(y[1]) for y in y_list],
                                               [len(y[4][1:-1].split(', ')) for y in y_list],
                                               [y[4][1:-1].split(', ') for y in y_list],
                                               bidir=bidir, iteration_num=iteration_num, Epsilon=Epsilon, Lambda=Lambda)
        print("algorithm:", x_dict)  
        f_dict = {}  
        with open("FaultyLink.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                f_dict[row[0]] = float(row[1])
        print("theory:", f_dict)  

        self.CD_Verify(path_type, x_dict, f_dict, [y[4][1:-1].split(', ') for y in y_list])  

        pred = [0] * 4  # [TP, FP, TN, FN]
        for link_id, prob in x_dict.items(): 
            if prob != 1 and f_dict.get(link_id):  # TP
                pred[0] += 1
            elif prob != 1 and f_dict.get(link_id) is None:  # FP
                pred[1] += 1
            elif prob == 1 and f_dict.get(link_id) is None:  # TN
                pred[2] += 1
            else:  # FN
                pred[3] += 1
        return pred


if __name__ == '__main__':
    k = 8  
    path_type = "CANARY"  # "NetBouncer" or "CANARY"
    core_monitor = True  # True: core switch is monitor switch
    if path_type == 'NetBouncer':
        core_monitor = False
    bidir = False  # True: paths and links are bi-directional
    if path_type == 'NetBouncer':
        bidir = False
    else:
        bidir = True
    faulty_rate = 0.1  # float: rate of faulty link in the network
    loss_rate_tuple = (0.1,0.1)  # tuple: range of loss rate
    iteration_num = 100  # maximal number of iterations
    Epsilon = 0.01  # float: threshold of path error
    Lambda = 1  # tuning parameter of regularization
    in_pod = False  # True: build 3-hops paths in pods

    run_time = 0.1  # duration of packet sending

    m = _Main_Process(k=k, path_type=path_type, core_monitor=core_monitor, bidir=bidir,faulty_rate=faulty_rate,loss_rate_tuple=loss_rate_tuple,in_pod=in_pod)
    if path_type == "CANARY":
        m.Main_Process_Segment(run_time) 
        m.MM_Prob_Map()
    else:
        m.Main_Process_NetBouncer()

