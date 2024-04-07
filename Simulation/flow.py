import topo
import csv
import random

class _Flows:
    def __init__(self,path_list,segment_dict) -> None:
        self.path_list = path_list
        self.flow_list = []
        self.segment_dict = segment_dict #key:segment_str , value: [segment_id,success_prob,send_num=100, lost_num, swtich_list, str(switches)]

    def Generate_Flowlist(self):
        flow_cnt = 0
        for path in self.path_list:
            flow_cnt += 1
            pps = 100
            size = 100  
            src = path.switches[0]
            dst = path.switches[-1]
            self.flow_list.append(_Flow(flow_cnt,pps,size,src,dst,path,None))
            path.Update_Flow(flow_cnt)

    def Generate_Pathsegment_Flowlist(self):
        flow_cnt = 0
        for segment_key in self.segment_dict.keys():
            segment_temp = self.segment_dict[segment_key]
            flow_cnt += 1
            pps = 100
            size = 500  
            src = segment_temp[4][0]
            dst = segment_temp[4][-1]  
            self.flow_list.append(_Flow(flow_cnt,pps,size,src,dst,None,segment_temp))

    def Generate_Flowlist_dataset(self, run_time=1, pps_floor=10, pps_ceil=100000,packet_floor=200):
        pps_dict = {}   # key: pps_id, value: pps  
        with open("ppsfilesize_2pktup.csv","r") as f:
            reader = csv.reader(f)
            pps_cnt = 0
            for row in reader:
                if int(row[1]) != 0:
                    if float(row[0]) >= pps_floor or float(row[0]) <= pps_ceil:
                        pps_cnt += 1
                        pps_dict[pps_cnt] = float(row[0])
        try:
            flow_cnt = 0
            with open("FlowConfig.csv","r") as f:
                reader = csv.reader(f)
                row_cnt = 0
                flow_list_temp = []
                for row in reader:
                    row_cnt += 1
                    path_id = int(row[0]) 
                    pps_index_list = row[1][1:-1].split(', ')
                    packet_num = int(row[2])
                    path = self.path_list[path_id-1]
                    src = path.switches[0]
                    dst = path.switches[-1] 
                    packet_num_sum = 0                   
                    # if len(pps_index_list) != m:
                    #     pps_index_list[len(pps_index_list)]  
                    for pps_index in pps_index_list:
                        flow_cnt += 1
                        pps = float(pps_dict[int(pps_index)])
                        size = round(pps * run_time)
                        if size < 1:
                            size = 1
                        packet_num_sum += size
                        flow_list_temp.append(_Flow(flowID=flow_cnt,pps=pps,size=size,src=src,dst=dst,path=path,path_segment=None))
                    if packet_num != packet_num_sum or packet_num_sum < packet_floor:
                        pps_index_list[len(pps_index_list)]  
                if row_cnt != len(self.path_list):
                    self.path_list[len(self.path_list)+1]  
                self.flow_list = flow_list_temp
        except: 
            write_list = []
            flow_cnt = 0
            key_list = list(pps_dict.keys())
            m_sum = 0
            path_packet_num_sum = 0
            for path in self.path_list:
                path_id = path.path_id
                pps_index = []
                packet_num = 0
                src = path.switches[0]
                dst = path.switches[-1]                
                while True:
                    flow_cnt += 1
                    random_pps = -1
                    random_pps_index = -1
                    while random_pps < pps_floor or random_pps > pps_ceil:
                        random_pps_index = random.choice(key_list)
                        random_pps = float(pps_dict[random_pps_index])
                    pps_index.append(random_pps_index)
                    random_size = round(random_pps * run_time)
                    if random_size < 1:
                        random_size = 1
                    packet_num += random_size
                    path_packet_num_sum += random_size
                    self.flow_list.append(_Flow(flowID=flow_cnt,pps=random_pps,size=random_size,src=src,dst=dst,path=path,path_segment=None))
                    path.Update_Flow(flow_cnt)
                    if packet_num >= packet_floor:
                        break
                write_list.append([path_id, pps_index, packet_num])
                m_sum += len(pps_index)
            with open("FlowConfig.csv","w+",newline="") as f:
                writer = csv.writer(f)
                writer.writerows(write_list)
            path_packet_num_avg = float(path_packet_num_sum) / float(len(self.path_list))
            with open("compute_m.csv","a",newline="") as f:
                writer = csv.writer(f)
                writer.writerow([round(float(m_sum)/float(len(self.path_list))), round(path_packet_num_avg)])         

    def Generate_Pathsegment_Flowlist_dataset(self, run_time=1, pps_floor=10, pps_ceil=100000,packet_floor=200):

        pps_dict = {}   # key: pps_id, value: pps  
        with open("ppsfilesize_2pktup.csv","r") as f:
            reader = csv.reader(f)
            pps_cnt = 0
            for row in reader:
                if int(row[1]) != 0:
                    if float(row[0]) >= pps_floor or float(row[0]) <= pps_ceil:
                        pps_cnt += 1
                        pps_dict[pps_cnt] = float(row[0])
        try:
            flow_cnt = 0
            with open("SegmentFlowConfig.csv","r") as f:
                reader = csv.reader(f)
                row_cnt = 0
                flow_list_temp = []
                for row in reader:
                    row_cnt += 1
                    segment_id = int(row[0]) 
                    pps_index_list = row[1][1:-1].split(', ')
                    packet_num = int(row[2])
                    segment_temp = self.segment_dict[row[3]]
                    src = segment_temp[4][0]
                    dst = segment_temp[4][-1] 
                    packet_num_sum = 0                   
                    # if len(pps_index_list) != m:
                    #     pps_index_list[len(pps_index_list)]  
                    for pps_index in pps_index_list:
                        flow_cnt += 1
                        pps = float(pps_dict[int(pps_index)])
                        size = round(pps * run_time)
                        if size < 1:
                            size = 1
                        packet_num_sum += size
                        flow_list_temp.append(_Flow(flowID=flow_cnt,pps=pps,size=size,src=src,dst=dst,path=None,path_segment=segment_temp))
                    if packet_num != packet_num_sum or packet_num_sum < packet_floor:
                        pps_index_list[len(pps_index_list)]  
                if row_cnt != len(list(self.segment_dict.keys())):
                    self.path_list[len(self.path_list)+1]   
                self.flow_list = flow_list_temp
        except: 
            write_list = []
            flow_cnt = 0
            key_list = list(pps_dict.keys())
            m_sum = 0
            segment_packet_num_sum = 0
            for segment_key in self.segment_dict.keys():
                segment_temp = self.segment_dict[segment_key]
                segment_id = segment_temp[0]
                pps_index = []
                packet_num = 0
                src = segment_temp[4][0]
                dst = segment_temp[4][-1]              
                while True:
                    flow_cnt += 1
                    random_pps = -1
                    random_pps_index = -1
                    while random_pps < pps_floor or random_pps > pps_ceil:
                        random_pps_index = random.choice(key_list)
                        random_pps = float(pps_dict[random_pps_index])
                    pps_index.append(random_pps_index)
                    random_size = round(random_pps * run_time)
                    if random_size < 1:
                        random_size = 1
                    packet_num += random_size
                    segment_packet_num_sum += random_size
                    self.flow_list.append(_Flow(flowID=flow_cnt,pps=random_pps,size=random_size,src=src,dst=dst,path=None,path_segment=segment_temp))
                    if packet_num >= packet_floor:
                        break
                write_list.append([segment_id, pps_index, packet_num, segment_temp[-1]])
                m_sum += len(pps_index)
            with open("SegmentFlowConfig.csv","w+",newline="") as f:
                writer = csv.writer(f)
                writer.writerows(write_list)
            path_packet_num_avg = float(segment_packet_num_sum) / float(len(list(self.segment_dict.keys())))
            with open("compute_m.csv","a",newline="") as f:
                writer = csv.writer(f)
                writer.writerow([round(float(m_sum)/float(len(list(self.segment_dict.keys())))), round(path_packet_num_avg)])                       

    def Generate_Flowlist_NetBouncer(self):
        flow_cnt = 0
        for path in self.path_list:
            flow_cnt += 1
            pps = 100
            size = 100
            src = path.switches[0]
            dst = path.switches[-1]
            self.flow_list.append(_Flow(flow_cnt,pps,size,src,dst,path,None))
            path.Update_Flow(flow_cnt)

class _Packet:
    def __init__(self) -> None:
        self.flowID = -1
        self.pathBF = ""
    
    def New_Packet(self,flowID):
        self.flowID = flowID
        self.pathBF = ""

class _Flow:
    def __init__(self,flowID,pps,size,src,dst,path,path_segment) -> None:
        self.flowID = flowID
        self.pps = pps
        self.size = size    
        self.real_send_num = 0 
        self.src = src
        self.dst = dst
        self.path = path
        self.path_segment = path_segment