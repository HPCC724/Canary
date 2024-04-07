import logging
import itertools
import threading
import schedule
import time

import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
import bfrt_grpc.client as gc

class FDcontroller():
    def setUp(self):
        self.GRPC_CLIENT = gc.ClientInterface(grpc_addr="localhost:50052", client_id=0,device_id=0)
        self.bfrt_info = bfrt_info=self.GRPC_CLIENT.bfrt_info_get(p4_name=None)
        self.GRPC_CLIENT.bind_pipeline_config(p4_name=bfrt_info.p4_name)
        self.target = gc.Target(device_id=0, pipe_id=0xFFFF)
        #paras:
        self.digest_read_interval = 5
        self.counter_read_interval = 100
        self.nextindex = 0
        self.indexmax = 1024
        self.counterinuse = {}
        self.entryinserted = set()
        self.indexdirty = ()
        self.pktmin = 100
        self.flowmin = 20
        self.pktperflow = 5
        self.flowinneed = 20
        self.flagneedflow = True
        self.setUpForward()
        self.totalcnt = 0   #total count get in a period
        self.timecount = 0
        self.logfile = open('log'+str(time.time())+'.log','w')
        self.next_cnt_index = 0

    def insertTableEntry(self, table_name, key_fields=None, action_name=None, data_fields=[]):
        test_table = self.bfrt_info.table_get(table_name)
        key_list = [test_table.make_key(key_fields)]
        data_list = [test_table.make_data(data_fields, action_name)]
        try:
            test_table.entry_add(self.target, key_list, data_list)
        except Exception:
            pass
        # self.table_entries.append((table_name, key_list))

    def modifyTableEntry(
        self, table_name, key_fields=None, action_name=None, data_fields=[]
    ):
        test_table = self.bfrt_info.table_get(table_name)
        key_list = [test_table.make_key(key_fields)]
        data_list = [test_table.make_data(data_fields, action_name)]
        test_table.entry_mod(self.target, key_list, data_list)
        # self.table_entries.append((table_name, key_list))

    def deleteTableEntry(
        self, table_name, key_fields=None
    ):
        test_table = self.bfrt_info.table_get(table_name)
        key_list = [test_table.make_key(key_fields)]
        try:
            test_table.entry_del(self.target, key_list)
        except Exception:
            pass
        # self.table_entries.append((table_name, key_list))

    def setUpForward(self):
        #forward paras
        output_port = 156
        srcport1 = 132
        srcport2 = 140
        srcport3 = 148
        match_table_forward = self.bfrt_info.table_get("SwitchIngress.table_forward")
        #10.0.0.4
        match_table_forward.entry_add(
        self.target,
        [match_table_forward.make_key([gc.KeyTuple("hdr.ipv4.dst_addr",0x0A000004)])],
        [match_table_forward.make_data([gc.DataTuple("port",output_port)],action_name = "forward")]
        )
        #10.0.0.1
        match_table_forward.entry_add(
        self.target,
        [match_table_forward.make_key([gc.KeyTuple("hdr.ipv4.dst_addr",0x0A000001)])],
        [match_table_forward.make_data([gc.DataTuple("port",srcport1)],action_name = "forward")]
        )
        #10.0.0.2
        match_table_forward.entry_add(
        self.target,
        [match_table_forward.make_key([gc.KeyTuple("hdr.ipv4.dst_addr",0x0A000002)])],
        [match_table_forward.make_data([gc.DataTuple("port",srcport2)],action_name = "forward")]
        )
        #10.0.0.3
        match_table_forward.entry_add(
        self.target,
        [match_table_forward.make_key([gc.KeyTuple("hdr.ipv4.dst_addr",0x0A000003)])],
        [match_table_forward.make_data([gc.DataTuple("port",srcport3)],action_name = "forward")]
        )

        self.insertTableEntry(
            'SwitchIngress.tbl_needflow',
            [gc.KeyTuple('hdr.ethernet.ether_type',0x0800)],
            'SwitchIngress.needflows',
            []
        )

    def insertEntryNeedFlow(self):
        self.flagneedflow = True
        self.insertTableEntry(
            'SwitchIngress.tbl_needflow',
            [gc.KeyTuple('hdr.ethernet.ether_type',0x0800)],
            'SwitchIngress.needflows',
            []
        )

    def EntryNotNeedFlow(self):
        self.flagneedflow = False
        self.deleteTableEntry(
            'SwitchIngress.tbl_needflow',
            [gc.KeyTuple('hdr.ethernet.ether_type',0x0800)]
        )


    def dict_to_id(self,data_dict):
        string = ''
        string += str(data_dict['dst_addr'])+str(data_dict['protocol'])+str(data_dict['src_addr'])+str(data_dict['pathBF'])+str(data_dict['dst_port'])+str(data_dict['src_port'])
        return string

    def getFlowidDigest(self):
        learn_filterf = self.bfrt_info.learn_get('digest_f')
        data_dict = {}
        try:
            digest = self.GRPC_CLIENT.digest_get()
            # pass

            print('digest',file=self.logfile)
            print(digest,file=self.logfile)
            data_list = learn_filterf.make_data_list(digest)
            print('datalist',file=self.logfile)
            print(data_list,file=self.logfile)
            if(len(data_list) != 0):
                for data in  data_list:
                    data_dict = data.to_dict()
                    try:
                        if(self.dict_to_id(data_dict) not in self.entryinserted):
                            self.insertFromDigest(data_dict)
                            self.flowinneed -= 1
                            print('data_dict',file=self.logfile)
                            print(data_dict,file=self.logfile)
                    except Exception as reason:
                        print(reason)
        except Exception:
                    # print(data_dict)
            print('no message f',file=self.logfile)
        if(self.flowinneed <= 0 and self.flagneedflow == True):
            self.EntryNotNeedFlow()

    def insertFromDigest(self,data_dict):
        print('try to insert',file=self.logfile)
        print(data_dict,file=self.logfile)
        try:
            self.insertTableEntry(
            'SwitchIngress.tbl_DownstreamRecord',
            [
                gc.KeyTuple('hdr.ipv4.dst_addr', data_dict['dst_addr']),
                gc.KeyTuple('hdr.ipv4.protocol', data_dict['protocol']),
                gc.KeyTuple('hdr.ipv4.src_addr', data_dict['src_addr']),
                gc.KeyTuple('meta.pathBF', data_dict['pathBF'],0xffffffff),
                gc.KeyTuple('meta.dstp', data_dict['dst_port']),
                gc.KeyTuple('meta.srcp', data_dict['src_port']),
                gc.KeyTuple('$MATCH_PRIORITY',0),
            ],
            'SwitchIngress.recorded',
            [
                gc.DataTuple('index', self.next_cnt_index),
            ]
            )

            self.insertTableEntry(
            'SwitchIngress.tbl_UpstreamRecord',
            [
                gc.KeyTuple('hdr.ipv4.dst_addr', data_dict['dst_addr']),
                gc.KeyTuple('hdr.ipv4.protocol', data_dict['protocol']),
                gc.KeyTuple('hdr.ipv4.src_addr', data_dict['src_addr']),
                gc.KeyTuple('meta.dstp', data_dict['dst_port']),
                gc.KeyTuple('meta.srcp', data_dict['src_port']),
            ],
            'SwitchIngress.upRecord',
            [
                gc.DataTuple('index', self.next_cnt_index),
            ]
            )

            self.insertTableEntry(
            'SwitchIngress.tbl_DownstreamRecord',
            [
                gc.KeyTuple('hdr.ipv4.dst_addr', data_dict['dst_addr']),
                gc.KeyTuple('hdr.ipv4.protocol', data_dict['protocol']),
                gc.KeyTuple('hdr.ipv4.src_addr', data_dict['src_addr']),
                gc.KeyTuple('meta.pathBF', data_dict['pathBF'],0x00000000),
                gc.KeyTuple('meta.dstp', data_dict['dst_port']),
                gc.KeyTuple('meta.srcp', data_dict['src_port']),
                gc.KeyTuple('$MATCH_PRIORITY',1),
            ],
            'SwitchIngress.dirtyRecord',
            [
                gc.DataTuple('index', self.next_cnt_index),
            ]
            )
            self.counterinuse[self.next_cnt_index] = 0
            self.next_cnt_index += 1
            self.entryinserted.add(self.dict_to_id(data_dict))
        except Exception as reason:
            print(reason)


    def getDirtyindexDigest(self):
        learn_filterd = self.bfrt_info.learn_get('digest_d')
        try:
            digest = digest = self.GRPC_CLIENT.digest_get()
            data_list = learn_filterd.make_data_list(digest)
            if(len(data_list) != 0):
                for data in  data_list:
                    data_dict = data.to_dict()
                    print(data_dict,file=self.logfile)
                    self.indexdirty.insert(data_dict['index'])
        except Exception:
            print('no message d',file=self.logfile)


    def getDigestLoop(self):
        # print('loop')
        self.timecount += 1 
        # print(self.timecount)
        if(self.timecount >= 100):
            # print('get counter')
            self.getCounterloop()
            self.timecount = 0
        if(self.flowinneed <= 0):
            return
        self.getFlowidDigest()
        pass


    def getCounter(self):
        cnt = 0
        flowactivecnt = 0
        print('getting counter')
        down_counter_table = self.bfrt_info.table_get("SwitchIngress.downstreamCnt")
        for k in self.counterinuse.keys():
            # print(k)
            down_resp = down_counter_table.entry_get(self.target,
                [down_counter_table.make_key(
                    [gc.KeyTuple('$COUNTER_INDEX',k)])],
                    {"from_hw":True})
            print(down_resp)
            data_dict = next(down_resp)[0].to_dict()
            print(data_dict,file=self.logfile)
            # print(data_dict)
            countersingle = data_dict['$COUNTER_SPEC_PKTS']
            self.counterinuse[k] = countersingle
            if countersingle != 0:
                flowactivecnt += 1
                cnt += countersingle
        return cnt,flowactivecnt

    
    def getCounterloop(self):
        self.totalcnt,flowactivecnt = self.getCounter()
        self.getFlowinNeed(flowactivecnt)
        if(self.flowinneed >= 0 and self.flagneedflow==False):
            self.insertEntryNeedFlow()


    def getFlowinNeed(self,flowactivecnt):
        pktneed = self.pktmin - self.totalcnt
        pktadded = 0
        if(pktneed<=0):
            self.flowinneed = 0
        else:
            if(flowactivecnt<=self.flowmin):
                self.flowinneed = self.flowmin - flowactivecnt
                pktadded = self.flowinneed * self.pktperflow
            if(pktadded < pktneed):
                self.flowinneed += pktneed/self.pktperflow+1
        
            

    def mainRun(self):
        self.setUp()
        print('setup',file=self.logfile)
        # schedule.every(0.01).seconds.do(self.getDigestLoop)
        # while True:
        #     schedule.run_pending()
        while True:
            self.getDigestLoop()
            time.sleep(0.01)
    
if __name__=="__main__":
    fd = FDcontroller()
    fd.mainRun()