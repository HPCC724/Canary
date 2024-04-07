from importlib.resources import path
import Utility.Hash
import random
import csv
import math
import os

class _Topo:
    def __init__(self,k,path_type,core_monitor,bidir,faulty_rate=0.01,loss_rate_tuple=(0.1, 0.2),in_pod=False) -> None:
        self.k = k
        self.path_type = path_type  # "NetBouncer" or "CANARY"
        self.core_monitor = core_monitor    # True: core switch is monitor switch
        self.bidir = bidir  # True: paths and links are bi-directional
        self.faulty_rate = faulty_rate  # float: rate of faulty link in the network
        self.loss_rate_tuple = loss_rate_tuple  # tuple: range of loss rate
        self.in_pod = in_pod # True: build 3-hops paths in pods
        self.switches = []  # List of switches in the network
        self.switch_id_bit = 0 # Bits of max switch ID 
        self.links = {} # key: str(src)+str(dst), value: link
        self.faulty_links = []  # element is [fault link, loss rate]
        self.paths = [] # List of paths in the network
        self.path_segments = {} # key:segment_str , value: [segment_id,success_prob,send_num=100, lost_num, swtich_list, str(switches)]
        self.Init_Topo(self.k,self.bidir)
    

    def Init_Topo(self, k, bidir):
        self.Init_Switches(k)
        self.Init_Links(k, bidir)
        self.Init_Path(k, self.path_type, bidir, self.in_pod)
        # self.Random_Choose_Faulty_Links(self.faulty_rate, self.loss_rate_tuple, self.bidir)
        self.Random_Choose_Faulty_Links_Config(self.faulty_rate, self.bidir)
        self.Update_Faulty_Link(self.faulty_links)
        self.Save_Path_Segment_Success_Prob()

    def Code_Switch(self, switch_id, switch_id_bit):
        switch_id_str = str(switch_id)
        if len(switch_id_str) < switch_id_bit:
            switch_id_str = "0" * (switch_id_bit-len(switch_id_str)) + switch_id_str
        return switch_id_str

    def Init_Switches(self, k, d=2**10):
        self.switch_id_bit = len(str(k*k*5//4))
        if self.core_monitor == True:
            for i in range(k*k*5//4):
                monitor = False
                switch_id = i + 1
                if switch_id <= k*k//2 or switch_id >= k*k+1:
                    monitor = True
                self.switches.append(_Switch(i+1,self.Code_Switch(i+1,self.switch_id_bit),d,monitor))    
        else:
            for i in range(k*k*5//4):
                monitor = False
                switch_id = i + 1
                if switch_id <= k*k//2:
                    monitor = True            
                self.switches.append(_Switch(i+1,self.Code_Switch(i+1,self.switch_id_bit),d,monitor))
        write_list = []
        for switch in self.switches:
            write_list.append([switch.switch_id_str])
        with open("TopoSwitch.csv","w+",newline="") as f:
            writer = csv.writer(f)
            writer.writerows(write_list)
    
    def Init_Links(self, k, bidir):
        link_cnt = 0
        for switch_id in range(1, k * k // 2 + 1):  # Edge -> Aggregation
            pod = (switch_id - 1) // (k // 2)  # [0, k-1]
            for j in range(1, k // 2 + 1):
                link_cnt += 1
                src = switch_id
                dst = k * k // 2 + k // 2 * pod + j
                src, dst = self.Coding_switch(src, dst)
                self.links[src + dst] = _Link(link_cnt, int(src), int(dst), False, 0)

        for switch_id in range(k * k // 2 + 1, k * k + 1):  # Aggregation -> core
            core = (switch_id - 1) % (k // 2)  # [0, k*k//4-1]
            for j in range(1, k // 2 + 1):
                link_cnt += 1
                src = switch_id
                dst = k * k + k // 2 * core + j
                src, dst = self.Coding_switch(src, dst)
                self.links[src + dst] = _Link(link_cnt, int(src), int(dst), False, 0)

        if bidir:  
            for switch_id in range(1, k * k // 2 + 1):
                pod = (switch_id - 1) // (k // 2)
                for j in range(1, k // 2 + 1):
                    link_cnt += 1
                    src = k * k // 2 + k // 2 * pod + j
                    dst = switch_id
                    src, dst = self.Coding_switch(src, dst)
                    self.links[src + dst] = _Link(link_cnt, int(src), int(dst), False, 0)

            for switch_id in range(k * k // 2 + 1, k * k + 1):
                core = (switch_id - 1) % (k // 2)
                for j in range(1, k // 2 + 1):
                    link_cnt += 1
                    src = k * k + k // 2 * core + j
                    dst = switch_id
                    src, dst = self.Coding_switch(src, dst)
                    self.links[src + dst] = _Link(link_cnt, int(src), int(dst), False, 0)
    
    def Init_Path(self, k, path_type, bidir, in_pod=False):
        path_cnt = 0
        if path_type == "NetBouncer":
            for edge_switch_id in range(1, k*k//2+1):
                pod = (edge_switch_id-1) // (k//2)
                for core_switch_id in range(k*k+1, k*k*5//4+1):
                    core = (core_switch_id-k*k-1) // (k//2)
                    agg_switch_id = k*k//2 + k//2*pod + core + 1
                    switch_list = [edge_switch_id, agg_switch_id, core_switch_id, agg_switch_id, edge_switch_id]
                    link_list = self.Switch_Link_Map(switch_list, bidir)
                    path_cnt += 1
                    self.paths.append(_Path(path_cnt, switch_list, link_list))
                    self.Update_Link_Path(link_list, path_cnt)
        elif path_type == "CANARY":
            if in_pod == True:
                for edge_switch_id in range(1, k*k//2+1):
                    pod = (edge_switch_id-1) // (k//2)
                    for edge_switch_id_ in range(edge_switch_id+1, k//2*(pod+1)+1):
                        for j in range(1, k//2+1):
                            agg_switch_id = k*k//2 + k//2*pod + j
                            switch_list = [edge_switch_id, agg_switch_id, edge_switch_id_]
                            link_list = self.Switch_Link_Map(switch_list, bidir)
                            path_cnt += 1
                            self.paths.append(_Path(path_cnt, switch_list, link_list))
                            self.Update_Link_Path(link_list, path_cnt)
            for edge_switch_id in range(1, k*k//2+1):
                pod = (edge_switch_id-1) // (k//2)
                for edge_switch_id_ in range(k//2*(pod+1)+1, k*k//2+1):
                    pod_ = (edge_switch_id_-1) // (k//2)
                    for core_switch_id in range(k*k+1, k*k*5//4+1):
                        core = (core_switch_id-k*k-1) // (k//2)
                        agg_switch_id = k*k//2 + k//2*pod + core + 1
                        agg_switch_id_ = k*k//2 + k//2*pod_ + core + 1
                        switch_list = [edge_switch_id, agg_switch_id, core_switch_id, agg_switch_id_, edge_switch_id_]
                        link_list = self.Switch_Link_Map(switch_list, bidir)
                        path_cnt += 1
                        self.paths.append(_Path(path_cnt, switch_list, link_list))
                        self.Update_Link_Path(link_list, path_cnt)
            if bidir == True:
                unbidir_path_num = len(self.paths)
                for i in range(unbidir_path_num):
                    path_ = self.paths[i]
                    path_cnt += 1
                    switch_list = path_.switches[::-1]
                    link_list = self.Switch_Link_Map(switch_list, bidir)
                    self.paths.append(_Path(path_cnt, switch_list, link_list))
                    self.Update_Link_Path(link_list, path_cnt)                    

    def Coding_switch(self, src, dst):
        src = self.Code_Switch(src, self.switch_id_bit)
        dst = self.Code_Switch(dst, self.switch_id_bit)      
        return src, dst

    def Switch_Link_Map(self,switch_list,bidir):
        link_list = []
        if bidir:
            for i in range(len(switch_list) - 1):
                src, dst = self.Coding_switch(switch_list[i], switch_list[i + 1])
                link_list.append(src + dst)
        else:
            for i in range(len(switch_list) - 1):
                if switch_list[i] < switch_list[i + 1]:
                    src, dst = self.Coding_switch(switch_list[i], switch_list[i + 1])
                    link_list.append(src + dst)
                elif switch_list[i] > switch_list[i + 1]:
                    src, dst = self.Coding_switch(switch_list[i + 1], switch_list[i])
                    link_list.append(src + dst)     
                else:
                    print("error")       
        return link_list 
    
    def Update_Link_Path(self,link_list,pathid):
        for link_key in link_list:
            link_value = self.links[link_key]
            link_value.Update_Path(pathid)

    def Random_Choose_Faulty_Links(self,faulty_rate,loss_rate_tuple,bidir):
        faulty_num = 0 
        if bidir == False:
            faulty_num = faulty_rate*len(self.links)
            if faulty_num < 1:
                faulty_num = 1
            else:
                faulty_num = round(faulty_num)
        else:
            half_link_num = int(len(self.links)/2)
            faulty_num = faulty_rate*half_link_num
            if faulty_num < 1:
                faulty_num = 1
            else:
                faulty_num = round(faulty_num)    
            faulty_num = 2 * faulty_num        
        faulty_links = []
        try:        
            with open("FaultyLink.csv","r") as f:
                reader = csv.reader(f)
                for row in reader:
                    link_id = row[0]
                    loss_rate = float(row[1])
                    faulty_links.append([self.links[link_id],loss_rate])
            if bidir == False:
                if len(faulty_links) == faulty_num:
                    self.faulty_links = faulty_links
                else:
                    faulty_links[len(faulty_links)] 
            else:
                if (len(faulty_links)*2) == faulty_num:
                    temp_link_list = faulty_links.copy()
                    for link in temp_link_list:
                        link_id = link[0]
                        loss_rate = link[1]
                        s1 = self.Code_Switch(link_id.dst, self.switch_id_bit)
                        s2 = self.Code_Switch(link_id.src, self.switch_id_bit)
                        faulty_links.append([self.links[s1+s2],loss_rate])
                    self.faulty_links = faulty_links
                else:
                    faulty_links[len(faulty_links)] 
        
        except :
            key_list = list(self.links.keys())
            faulty_links = []
            write_list = []
            random_key_list = []
            if bidir == False:
                while True:
                    if len(faulty_links) >= faulty_num:
                        break
                    random_key = random.choice(key_list)
                    while random_key in random_key_list:
                        random_key = random.choice(key_list)
                    random_key_list.append(random_key)
                    random_value = self.links[random_key]
                    loss_rate = random.uniform(loss_rate_tuple[0],loss_rate_tuple[1])
                    faulty_links.append([random_value, loss_rate])
                    write_list.append([random_key, loss_rate]) 
            else:
                while True:
                    if len(faulty_links) >= faulty_num:
                        break
                    random_key = random.choice(key_list)
                    random_key_ = random_key[self.switch_id_bit:] + random_key[:self.switch_id_bit]
                    while random_key in random_key_list or random_key_ in random_key_list:
                        random_key = random.choice(key_list)
                        random_key_ = random_key[self.switch_id_bit:] + random_key[:self.switch_id_bit]  
                    random_key_list.append(random_key)
                    random_key_list.append(random_key_)                      
                    random_value = self.links[random_key]
                    random_value_ = self.links[random_key_]
                    loss_rate = random.uniform(loss_rate_tuple[0],loss_rate_tuple[1])
                    faulty_links.append([random_value, loss_rate])
                    faulty_links.append([random_value_, loss_rate])
                    write_list.append([random_key, loss_rate])                
            with open("FaultyLink.csv","w+",newline="") as f:
                writer = csv.writer(f)
                writer.writerows(write_list)
            self.faulty_links = faulty_links

    def Random_Choose_Faulty_Links_Config(self,faulty_rate,bidir):
        faulty_num = 0 
        if bidir == False:
            faulty_num = faulty_rate*len(self.links)
            if faulty_num < 1:
                faulty_num = 1
            else:
                faulty_num = round(faulty_num)
        else:
            half_link_num = int(len(self.links)/2)
            faulty_num = faulty_rate*half_link_num
            if faulty_num < 1:
                faulty_num = 1
            else:
                faulty_num = round(faulty_num)    
            faulty_num = 2 * faulty_num        
        faulty_links = []
        loss_rate_tuple_list = []
        with open("LossRateTupleConfig.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                temp_loss_rate = row[0][1:-1].split(', ')
                loss_rate_tuple = (float(temp_loss_rate[0]), float(temp_loss_rate[1]))
                loss_rate_tuple_list.append([loss_rate_tuple, float(row[1])])
        if bidir == False:
            faulty_link_sum = 0
            for tuple_ in loss_rate_tuple_list:
                link_num = math.floor(faulty_num * tuple_[1])
                tuple_.append(link_num)
                faulty_link_sum += link_num
            if faulty_link_sum != faulty_num:
                temp_tuple_index = []
                for i in range(faulty_num-faulty_link_sum):
                    temp_index = random.randint(0,len(loss_rate_tuple_list)-1)
                    while temp_index in temp_tuple_index:
                        temp_index = random.randint(0,len(loss_rate_tuple_list)-1)
                    temp_tuple_index.append(temp_index)
                    loss_rate_tuple_list[temp_index][2] += 1
        else:
            faulty_link_sum = 0
            for tuple_ in loss_rate_tuple_list:
                link_num = math.floor(faulty_num * tuple_[1] * 0.5)
                tuple_.append(link_num)
                faulty_link_sum += link_num
            if (faulty_link_sum*2) != faulty_num:
                temp_tuple_index = []
                for i in range(int(faulty_num/2-faulty_link_sum)):
                    temp_index = random.randint(0,len(loss_rate_tuple_list)-1)
                    while temp_index in temp_tuple_index:
                        temp_index = random.randint(0,len(loss_rate_tuple_list)-1)
                    temp_tuple_index.append(temp_index)
                    loss_rate_tuple_list[temp_index][2] += 1            
        try:        
            with open("FaultyLink.csv","r") as f:
                reader = csv.reader(f)
                for row in reader:
                    link_id = row[0]
                    loss_rate = float(row[1])
                    faulty_links.append([self.links[link_id],loss_rate])
            if bidir == False:
                if len(faulty_links) == faulty_num:
                    self.faulty_links = faulty_links
                else:
                    faulty_links[len(faulty_links)] 
            else:
                if (len(faulty_links)*2) == faulty_num:
                    temp_link_list = faulty_links.copy()
                    for link in temp_link_list:
                        link_id = link[0]
                        loss_rate = link[1]
                        s1 = self.Code_Switch(link_id.dst, self.switch_id_bit)
                        s2 = self.Code_Switch(link_id.src, self.switch_id_bit)
                        faulty_links.append([self.links[s1+s2],loss_rate])
                    self.faulty_links = faulty_links
                else:
                    faulty_links[len(faulty_links)]         
        except :
            key_list = list(self.links.keys())
            faulty_links = []
            write_list = []
            random_key_list = []
            if bidir == False:
                for tuple_ in loss_rate_tuple_list:
                    for k in range(tuple_[2]):
                        if len(faulty_links) >= faulty_num:
                            break
                        random_key = random.choice(key_list)
                        while random_key in random_key_list:
                            random_key = random.choice(key_list)
                        random_key_list.append(random_key)
                        random_value = self.links[random_key]
                        loss_rate = random.uniform(tuple_[0][0], tuple_[0][1])
                        faulty_links.append([random_value, loss_rate])
                        write_list.append([random_key, loss_rate]) 
            else:
                for tuple_ in loss_rate_tuple_list:
                    for k in range(tuple_[2]):
                        if len(faulty_links) >= faulty_num:
                            break
                        random_key = random.choice(key_list)
                        random_key_ = random_key[self.switch_id_bit:] + random_key[:self.switch_id_bit]
                        while random_key in random_key_list or random_key_ in random_key_list:
                            random_key = random.choice(key_list)
                            random_key_ = random_key[self.switch_id_bit:] + random_key[:self.switch_id_bit]  
                        random_key_list.append(random_key)
                        random_key_list.append(random_key_)                      
                        random_value = self.links[random_key]
                        random_value_ = self.links[random_key_]
                        loss_rate = random.uniform(tuple_[0][0], tuple_[0][1])
                        faulty_links.append([random_value, loss_rate])
                        faulty_links.append([random_value_, loss_rate])
                        write_list.append([random_key, loss_rate])                
            with open("FaultyLink.csv","w+",newline="") as f:
                writer = csv.writer(f)
                writer.writerows(write_list)
            self.faulty_links = faulty_links            
            


    def Update_Faulty_Link(self,faulty_link_list):
        for faulty_link, loss_rate in faulty_link_list:
            faulty_link.Update_Loss_rate(loss_rate)
        write_list = []
        for link_id in self.links:
            success_prob = self.links[link_id].loss_rate
            write_list.append([link_id, 1 - success_prob])
        with open("TopoLink.csv","w+",newline="") as f:
            writer = csv.writer(f)
            writer.writerows(write_list)        


    def Switches_to_Segment(self,switch_list):
        segment_str = ""
        for switch_ in switch_list:
            segment_str = segment_str + self.Code_Switch(switch_, self.switch_id_bit)
        return segment_str


    def Save_Path_Segment_Success_Prob(self):
        write_list = []
        segment_list = {}   # key:segment_str , value: [segment_id,success_prob,send_num=100, lost_num, swtich_list, str(switches)]
        segment_cnt = 0
        send_num = 500  
        for path in self.paths:
            path_link = path.links
            path_switches_id = path.switches
            if self.core_monitor == True and len(path_switches_id) == 5:
                segment1 = self.Switches_to_Segment(path_switches_id[0:3])
                segment2 = self.Switches_to_Segment(path_switches_id[2:6])
                if self.bidir == False:
                    segment1_reverse = self.Switches_to_Segment(path_switches_id[2::-1])
                    segment2_reverse = self.Switches_to_Segment(path_switches_id[4:1:-1])
                    if segment_list.get(segment1) == None and segment_list.get(segment1_reverse) == None:
                        segment_cnt += 1
                        success_prob = 1
                        for link_id in path_link[0:2]:
                            loss_rate = self.links[link_id].loss_rate
                            if loss_rate != 0:
                                success_prob = success_prob * (1 - loss_rate)
                        success_prob = success_prob
                        lost_num = int(send_num * (1 - success_prob))
                        temp_switch_list = []
                        for s_id in path_switches_id[0:3]:
                            temp_switch_list.append(self.Code_Switch(s_id, self.switch_id_bit))
                        segment_list[segment1] = [segment_cnt, success_prob, send_num, lost_num, temp_switch_list, segment1] 
                        write_list.append(segment_list[segment1])                        
                    if segment_list.get(segment2) == None and segment_list.get(segment2_reverse) == None:
                        segment_cnt += 1
                        success_prob = 1
                        for link_id in path_link[2:4]:
                            loss_rate = self.links[link_id].loss_rate
                            if loss_rate != 0:
                                success_prob = success_prob * (1 - loss_rate) 
                        success_prob = success_prob
                        lost_num = int(send_num * (1 - success_prob))
                        temp_switch_list = []
                        for s_id in path_switches_id[2:6]:
                            temp_switch_list.append(self.Code_Switch(s_id, self.switch_id_bit))
                        segment_list[segment2] = [segment_cnt, success_prob, send_num, lost_num, temp_switch_list, segment2] 
                        write_list.append(segment_list[segment2])  
                else:    
                    if segment_list.get(segment1) == None:
                        segment_cnt += 1
                        success_prob = 1
                        for link_id in path_link[0:2]:
                            loss_rate = self.links[link_id].loss_rate
                            if loss_rate != 0:
                                success_prob = success_prob * (1 - loss_rate)
                        success_prob = success_prob
                        lost_num = int(send_num * (1 - success_prob))
                        temp_switch_list = []
                        for s_id in path_switches_id[0:3]:
                            temp_switch_list.append(self.Code_Switch(s_id, self.switch_id_bit))
                        segment_list[segment1] = [segment_cnt, success_prob, send_num, lost_num, temp_switch_list, segment1]   
                        write_list.append(segment_list[segment1])                        
                    if segment_list.get(segment2) == None:
                        segment_cnt += 1
                        success_prob = 1
                        for link_id in path_link[2:4]:
                            loss_rate = self.links[link_id].loss_rate
                            if loss_rate != 0:
                                success_prob = success_prob * (1 - loss_rate) 
                        success_prob = success_prob
                        lost_num = int(send_num * (1 - success_prob))
                        temp_switch_list = []
                        for s_id in path_switches_id[2:6]:
                            temp_switch_list.append(self.Code_Switch(s_id, self.switch_id_bit))
                        segment_list[segment2] = [segment_cnt, success_prob, send_num, lost_num, temp_switch_list, segment2] 
                        write_list.append(segment_list[segment2])  
            else:
                segment = self.Switches_to_Segment(path_switches_id)
                if segment_list.get(segment) == None:
                    segment_cnt += 1
                    success_prob = 1
                    for link_id in path_link:
                        loss_rate = self.links[link_id].loss_rate
                        if loss_rate != 0:
                            success_prob = success_prob * (1 - loss_rate) 
                    success_prob = success_prob
                    lost_num = int(send_num * (1 - success_prob))
                    if self.path_type == "NetBouncer":
                        segment_truncated= segment[:(self.switch_id_bit*3)]
                        temp_switch_list = []
                        for s_id in path_switches_id[0:3]:
                            temp_switch_list.append(self.Code_Switch(s_id, self.switch_id_bit))
                        segment_list[segment] = [segment_cnt, success_prob, send_num, lost_num, temp_switch_list, segment_truncated]
                    else:
                        temp_switch_list = []
                        for s_id in path_switches_id:
                            temp_switch_list.append(self.Code_Switch(s_id, self.switch_id_bit))
                        segment_list[segment] = [segment_cnt, success_prob, send_num, lost_num, temp_switch_list, segment]
                    write_list.append(segment_list[segment])
        self.path_segments = segment_list
        with open(self.path_type+"_path_segment_groundtruth.csv","w+",newline="") as f:
            writer = csv.writer(f)
            writer.writerows(write_list)

    def Compute_Meter_Occupancy(self):
        occupancy_sum = 0
        monitor_cnt = 0
        for switch in self.switches:
            if switch.monitor == True:
                occupancy_sum += switch.Compute_Occupancy()
                monitor_cnt += 1
        return occupancy_sum / float(monitor_cnt)


class _Path:
    def __init__(self,id,switch_list,link_list) -> None:
        self.path_id = id
        self.switches = switch_list
        self.links = link_list
        self.segments = []
        self.flows = []
        self.send_num = 0
        self.lost_num = 0
        self.success_prob = 0
        
    def Update_Flow(self,flow):
        self.flows.append(flow)
    
    def Update_Success_Prob(self):
        self.success_prob = float(self.send_num-self.lost_num) / float(self.send_num)


class _Link:
    def __init__(self,id,src,dst,faulty_flag,loss_rate) -> None:
        self.link_id = id
        self.src = src  # src switch id
        self.dst = dst  # dst switch id
        self.faulty = faulty_flag   # True while faulty
        self.loss_rate = loss_rate
        self.paths = []

    def Update_Path(self,path):
        self.paths.append(path)

    def Update_Loss_rate(self,loss_rate):
        if loss_rate > 0 and loss_rate < 1 :
            self.faulty = True
            self.loss_rate = loss_rate
    
    def Trans_Packet(self):
        loss_flag = False
        r = random.uniform(0, self.loss_rate)
        if r <= self.loss_rate:
            loss_flag = True
        return loss_flag


class _Switch:
    def __init__(self,id,id_str,w,monitor,d=3) -> None:
        self.switch_id = id # switch id: int
        self.switch_id_str = id_str # switch id: str
        self.monitor = monitor  # True: switch is monitor switch
        self.d = d
        self.w = w
        self.hashfunc = ["MD5","SHA256","SHA1"]
        self.USM = None
        self.DSM = None
        self.MM = []    
        self.flow_cnt = {} 
        self.occpancy = 0
        self.Generate_Meter()
    
    def Generate_Meter(self):
        if self.monitor == True:
            self.USM = _USM(self.d, self.w, self.hashfunc)
            self.DSM = _DSM(self.d, self.w, self.hashfunc)
    
    def Generate_MM(self, switches):
        MM_clean_flow = []  # [flowID]
        for row in self.DSM.DSM_table:  
            for bucket in row:  # [dirtyFlag, flowID, pathBF, counter]
                if bucket[0] == 0 and bucket[1] != -1 and bucket[1] not in MM_clean_flow:
                    MM_clean_flow.append(bucket[1])
                    self.MM.append(bucket[-3:] + [0])
        remove_MM = []  
        for i in range(len(self.MM)):
            term = self.MM[i]  # [flowID,pathBF,send_num,lost_num]
            if len(term[1]) <= 2:  
                continue
            us_switch = int(term[1][-2:]) - 1  
            for j in range(len(term[1]) - 2, 0, -2):
                us_switch = int(term[1][j - 2:j]) - 1
                if switches[us_switch].monitor:
                    break
            us_USM = switches[us_switch].USM  
            USM_table_term = []
            for row in us_USM.USM_table:
                for bucket in row:  # [dirtyFlag, flowID, counter]
                    if bucket[0] == 0 and bucket[1] == term[0]:
                        USM_table_term = bucket
                        break
            if len(USM_table_term): 
                term[3] = USM_table_term[2] - term[2]  # loss = counter - send
                term[2] = USM_table_term[2]  # send = counter
            else:
                remove_MM.append(term)
        self.MM = [x for x in self.MM if x not in remove_MM]

    def Receive_Packet(self,packet):
        new_BF = self.switch_id_str
        packet.pathBF = packet.pathBF + new_BF
        if self.monitor:
            self.DSM.Receive_Packet(packet)
            packet.pathBF = ""
            self.USM.Receive_Packet(packet)
            packet.pathBF = packet.pathBF + new_BF
        return packet

    def Receive_Packet_NetBouncer(self,packet):
        packet.pathBF = packet.pathBF + self.switch_id_str
        if len(packet.pathBF) == (5*len(self.switch_id_str)):
            if packet.flowID in self.flow_cnt:
                self.flow_cnt[packet.flowID] += 1
            else:
                self.flow_cnt[packet.flowID] = 1
        return packet

    def Compute_Occupancy(self):
        if self.USM != None and self.DSM != None:
            self.occpancy = (self.USM.Compute_Occupancy()+self.DSM.Compute_Occupancy()) / 2
            return self.occpancy


class _USM:
    def __init__(self,d,w,hashFunction):
        self.hashfunc = hashFunction
        self.d = d  # d rows
        self.w = w  # w columns
        self.USM_table=[]
        self.hash = Utility.Hash._Hash()
        self.Generate_Hash_Table()

    def Generate_Hash_Table(self):
        for i in range (0,self.d):
            # dirtyFlag, flowID, counter
            self.USM_table.append([[0, -1, 0] for x in range(0, self.w)])

    def Receive_Packet(self,packet):
        for i in range(0,self.d):
            hash = self.hash.Hash_Function(str(packet.flowID),self.w,self.hashfunc[i])
            if self.USM_table[i][hash][0] != 1:
                if self.USM_table[i][hash][1] == -1:
                    self.USM_table[i][hash][1] = packet.flowID
                    self.USM_table[i][hash][2] = 1
                elif self.USM_table[i][hash][1] != packet.flowID:
                    self.USM_table[i][hash][0] = 1
                else:
                    self.USM_table[i][hash][2] += 1
    
    def Compute_Occupancy(self):
        occ_bucket_cnt = 0
        for i in range(self.d):
            for j in range(self.w):
                if self.USM_table[i][j][1] != -1:
                    occ_bucket_cnt += 1
        return float(occ_bucket_cnt) / float(self.d*self.w)


class _DSM:
    def __init__(self,d,w,hashFunction):
        self.hashfunc = hashFunction
        self.d = d
        self.w = w
        self.DSM_table=[]
        self.hash = Utility.Hash._Hash()
        self.Generate_Hash_Table()

    def Generate_Hash_Table(self):
        for i in range (0,self.d):
            # dirtyFlag, flowID, pathBF, counter
            self.DSM_table.append([[0, -1, "", 0] for x in range(0, self.w)])

    def Receive_Packet(self,packet):
        for i in range(0,self.d):
            hash = self.hash.Hash_Function(str(packet.flowID),self.w,self.hashfunc[i])
            if self.DSM_table[i][hash][0] != 1:
                if self.DSM_table[i][hash][1] == -1:
                    self.DSM_table[i][hash][1] = packet.flowID
                    self.DSM_table[i][hash][2] = packet.pathBF
                    self.DSM_table[i][hash][3] = 1
                elif (self.DSM_table[i][hash][1] != packet.flowID) or (self.DSM_table[i][hash][2] != packet.pathBF):
                    self.DSM_table[i][hash][0] = 1
                else:
                    self.DSM_table[i][hash][3] += 1

    def Compute_Occupancy(self):
        occ_bucket_cnt = 0
        for i in range(self.d):
            for j in range(self.w):
                if self.DSM_table[i][j][1] != -1:
                    occ_bucket_cnt += 1
        return float(occ_bucket_cnt) / float(self.d*self.w)





