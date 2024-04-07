import Utility.Hash

class _HashPipe:
    def __init__(self, stage_num=3, entry_num=4,hash_func=["MD5","SHA256","SHA1"]) -> None:
        self.stage_num = stage_num
        self.entry_num = entry_num
        self.hash = Utility.Hash._Hash()
        self.hash_func = hash_func
        self.hash_tables = [[[-1, 0 ] for i in range(self.entry_num)] for j in range(self.stage_num)]
    
    def Receive_Packet(self, packet):
        hash_value0 = self.hash.Hash_Function(str(packet.flowID), self.entry_num, self.hash_func[0])
        temp_key = -1
        temp_value = 0
        # Insert in the first stage
        if self.hash_tables[0][hash_value0][0] == packet.flowID:
            self.hash_tables[0][hash_value0][1] += 1
            return
        elif self.hash_tables[0][hash_value0][0] == -1:
            self.hash_tables[0][hash_value0][0] = packet.flowID
            self.hash_tables[0][hash_value0][1] = 1
            return
        else:
            temp_key = self.hash_tables[0][hash_value0][0]
            temp_value =self.hash_tables[0][hash_value0][1]
            self.hash_tables[0][hash_value0][0] = packet.flowID
            self.hash_tables[0][hash_value0][1] = 1
            # Track a rolling minimum
            for i in range(1, self.stage_num):
                hash_valuei = self.hash.Hash_Function(str(temp_key), self.entry_num, self.hash_func[i])
                if self.hash_tables[i][hash_valuei][0] == temp_key:
                    self.hash_tables[i][hash_valuei][1] += temp_value
                    return
                elif self.hash_tables[i][hash_valuei][0] == -1:
                    self.hash_tables[i][hash_valuei][0] = temp_key
                    self.hash_tables[i][hash_valuei][1] = temp_value
                    return
                elif self.hash_tables[i][hash_valuei][1] < temp_value:
                    temp1 = self.hash_tables[i][hash_valuei][0]
                    temp2 = self.hash_tables[i][hash_valuei][1]
                    self.hash_tables[i][hash_valuei][0] = temp_key
                    self.hash_tables[i][hash_valuei][1] = temp_value
                    temp_key = temp1
                    temp_value = temp2


class _Flow_:
    def __init__(self,flowID,packet_time_list) -> None:
        self.flowID = flowID
        self.packet_time_list = packet_time_list

class _Packet_:
    def __init__(self) -> None:
        self.flowID = -1
        self.metadata = None
