#include <core.p4>
#include <tna.p4>

#include "common/headers.p4"
#include "common/util.p4"



const PortId_t LoopBackPort = 164;
const PortId_t OutputPort = 140;
const bit<32> pathand = 0;

struct metadata_t{
    //for stream
    //ports
    bit<16> srcp;
    bit<16> dstp;
    bit<32> cntindex;
    //for hashpipe
    bit<32> pipefid;      //flow id for hashpipe, for test it's dstaddr
    bit<1> DownRecord;  //Downstream Recorded
    bit<32> pktid;      //current packet id()
    bit<32> tempid;     //output id from register
    bit<32> tempcnt;    //temp count
    bit<32> hashindex1;
    bit<32> hashindex2;
    bit<32> hashindex3;
    bit<32> fcurrentcnt;
    bit<1> replaced;        
    bit<1> needflow;        
}

struct pair {
    bit<32>     cnt;
    bit<32>     id;
}

struct digest_dirty {
    ipv4_addr_t src_addr;
    ipv4_addr_t dst_addr;
    bit<8> protocol;
    bit<16> src_port;
    bit<16> dst_port;
    bit<32> index;  //for dirty cells
}

struct digest_flowid {
    ipv4_addr_t src_addr;
    ipv4_addr_t dst_addr;
    bit<8> protocol;
    bit<16> src_port;
    bit<16> dst_port;
    bit<32> pathBF;
}


parser SwitchIngressParser(
    packet_in pkt,
    out header_t hdr,
    out metadata_t meta,
    out ingress_intrinsic_metadata_t ig_intr_md){
        
    TofinoIngressParser() tofino_parser;
    state start{
        tofino_parser.apply(pkt, ig_intr_md);
        transition parse_ethernet;
    }
    state parse_ethernet{
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            0x0800 : parse_ipv4;
            default : accept;
        }
    }

    state parse_ipv4 {
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            17 : parse_udp;
            6 :  parse_tcp;
            default : reject;
        }
    }

    state parse_udp{
        pkt.extract(hdr.udp);
        pkt.extract(hdr.path_t);
        transition accept;
    }

    state parse_tcp{
        pkt.extract(hdr.tcp);
        pkt.extract(hdr.path_t);
        transition accept;
    }

}

control SwitchIngressDeparser(
        packet_out pkt,
        inout header_t hdr,
        in metadata_t meta,
        in ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md) {
    Checksum() ipv4_checksum;
    Digest<digest_dirty>() digest_d;
    Digest<digest_flowid>() digest_f;
    apply {        

        if(ig_dprsr_md.digest_type == 1){
            digest_d.pack({hdr.ipv4.src_addr,hdr.ipv4.dst_addr,hdr.ipv4.protocol,meta.srcp,meta.dstp,meta.cntindex});
        }
        else if(ig_dprsr_md.digest_type == 2 )
        {
            digest_f.pack({hdr.ipv4.src_addr,hdr.ipv4.dst_addr,hdr.ipv4.protocol,meta.srcp,meta.dstp,hdr.path_t.pathBF});
        }
        hdr.ipv4.hdr_checksum = ipv4_checksum.update({
            hdr.ipv4.version,
            hdr.ipv4.ihl,
            hdr.ipv4.diffserv,
            hdr.ipv4.total_len,
            hdr.ipv4.identification,
            hdr.ipv4.flags,
            hdr.ipv4.frag_offset,
            hdr.ipv4.ttl,
            hdr.ipv4.protocol,
            hdr.ipv4.src_addr,
            hdr.ipv4.dst_addr});

         pkt.emit(hdr);
    }
}

control SwitchIngress(
        inout header_t hdr,
        inout metadata_t meta,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md
){

        //table forward
    action forward(PortId_t port){
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
        ig_tm_md.ucast_egress_port = port;
        // ig_tm_md.qid = 0;
    }
    action drop(){
        ig_dprsr_md.drop_ctl = 0x1;
    }
    table table_forward{
        key = {
            hdr.ipv4.dst_addr: exact;
        }
        actions = {
            forward;
            drop;
        }
        const default_action = drop();   
        size = 512;
    }

    Counter<bit<32>,bit<32>>( 1024,CounterType_t.PACKETS) downstreamCnt;
    Counter<bit<32>,bit<32>>( 1024,CounterType_t.PACKETS) upstreamCnt;

    action recorded(bit<32> index)
    {
        hdr.path_t.upRecord = 0;
        meta.cntindex = index;
        // meta.dirtyflag = 0;
        meta.DownRecord = 1;
        downstreamCnt.count(index);
    }


    action dirtyRecord(bit<32> index)
    {
        meta.cntindex = index;
        hdr.path_t.upRecord = 0;
        // meta.dirtyflag = 1;
        ig_dprsr_md.digest_type = 1;
        //if dirty, send digest
    }



    table tbl_DownstreamRecord{
        key = {
            hdr.ipv4.dst_addr: exact;
            hdr.ipv4.src_addr: exact;
            hdr.ipv4.protocol: exact;
            hdr.path_t.upRecord: exact;
            meta.srcp:exact;
            meta.dstp:exact;
            hdr.path_t.pathBF: ternary;
        }
        actions = {
            recorded;
            dirtyRecord;
        }
    }

    action upRecord(bit<32> index){
        hdr.path_t.upRecord = 1;
        upstreamCnt.count(index);

    }

    table tbl_UpstreamRecord{
        key = {
            hdr.ipv4.dst_addr: exact;
            hdr.ipv4.src_addr: exact;
            hdr.ipv4.protocol: exact;
            meta.srcp:exact;
            meta.dstp:exact;
        }
        actions = {
            upRecord;
        }
    }

    action needflows(){
        meta.needflow = 1;
    }
    action notneedflow(){
        meta.needflow = 0;
    }
    table tbl_needflow{
        key = {
            hdr.ethernet.ether_type:exact;
        }
        actions = {
            needflows;
            notneedflow;
        }
    }



    Register<pair,bit<32>> (size=32w4, initial_value={1, 0}) PipeStage1_id_counter;
    Register<bit<32>,bit<32>> (32w4,0) PipeStage1_counterstar;
    
    Register<pair,bit<32>> (size=32w4, initial_value={1, 0}) PipeStage2_id_counter;
    Register<bit<32>,bit<32>> (32w4,0) PipeStage2_counterstar;
    
    Register<pair,bit<32>> (size=32w4, initial_value={1, 0}) PipeStage3_id_counter;
    Register<bit<32>,bit<32>> (32w4,0) PipeStage3_counterstar;

    RegisterAction<pair,bit<32>,bit<32>>(PipeStage1_id_counter) pipe1updateaction = {
        void apply(inout pair value, out bit<32> outid)
        {
            
            if(value.id == meta.tempid){
                value.cnt = value.cnt + 1;
                // outid = hdr.ipv4.dst_addr;
            }
            else{
                outid = value.id;
                value.id = meta.tempid;
                value.cnt  = 1;
            }
        }
    };

    RegisterAction<bit<32>,bit<32>,bit<32>>(PipeStage1_counterstar) pipe1_counteradd_one = {
        void apply(inout bit<32> value,out bit<32> outvalue)
        {
            value = value +1;
            outvalue = value;
        }
    };

    RegisterAction<bit<32>,bit<32>,bit<32>>(PipeStage1_counterstar) pipe1_counter_replace = {
        void apply(inout bit<32> value,out bit<32> outvalue)
        {
            outvalue = value;
            value = 1;
        }
    };

    RegisterAction<pair,bit<32>,bit<32>>(PipeStage2_id_counter) pipe2updateaction = {
        void apply(inout pair value, out bit<32> outid)
        {
            if(value.id == meta.tempid){
                value.cnt = value.cnt + meta.tempcnt;
                outid = value.id;
            }
            else if(value.cnt < meta.tempcnt){
                outid = value.id;
                value.id = meta.tempid;
                value.cnt  = meta.tempcnt;
            }



        }
    };


    RegisterAction<bit<32>,bit<32>,bit<32>>(PipeStage2_counterstar) pipe2_counteradd = {
        void apply(inout bit<32> value,out bit<32> outvalue)
        {
            value = value +meta.tempcnt;
            outvalue = value;
        }
    };

    RegisterAction<bit<32>,bit<32>,bit<32>>(PipeStage2_counterstar) pipe2_counter_replace = {
        void apply(inout bit<32> value,out bit<32> outvalue)
        {
            if(meta.tempcnt>value){
                outvalue = value;
                value = meta.tempcnt;
            }

        }
    };

    RegisterAction<pair,bit<32>,bit<32>>(PipeStage3_id_counter) pipe3updateaction = {
        void apply(inout pair value, out bit<32> outid)
        {
            if(value.id == meta.tempid){
                outid = value.id;
                value.cnt = value.cnt + meta.tempcnt;
            }
            else if(value.cnt < meta.tempcnt){
                outid = value.id;
                value.id = meta.tempid;
                value.cnt  = meta.tempcnt;
            }


        }
    };

    RegisterAction<bit<32>,bit<32>,bit<32>>(PipeStage3_counterstar) pipe3_counteradd = {
        void apply(inout bit<32> value,out bit<32> outvalue)
        {
            value = value +meta.tempcnt;
            outvalue = value;
        }
    };

    RegisterAction<bit<32>,bit<32>,bit<32>>(PipeStage3_counterstar) pipe3_counter_replace = {
        void apply(inout bit<32> value,out bit<32> outvalue)
        {
            if(meta.tempcnt>value){
                outvalue = value;
                value = meta.tempcnt;
            }
        }
    };

    CRCPolynomial<bit<32>>(0x04C11DB7,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32a;
    CRCPolynomial<bit<32>>(0x741B8CD7,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32b;
    
    

    Hash<bit<32>> (HashAlgorithm_t.CRC32) global_hash;
    Hash<bit<32>> (HashAlgorithm_t.CUSTOM,crc32b) hash1;
    Hash<bit<32>>(HashAlgorithm_t.CUSTOM,crc32a) hash2;
    Hash<bit<32>> (HashAlgorithm_t.RANDOM) hash3;


    bit<32> pipe1outid;
    bit<32> pipe2outid;
    bit<32> pipe3outid;

    //hash actions
    action pipehash1(){
        meta.hashindex1 = hash1.get({meta.tempid});
    }
    action pipehash2(){
        meta.hashindex2 = hash2.get({meta.tempid});
    }
    action pipehash3(){
        meta.hashindex3 = hash3.get({meta.tempid});
    }

    apply{
        tbl_needflow.apply();
        table_forward.apply();
        if(hdr.tcp.isValid())
        {
            meta.srcp = hdr.tcp.src_port;
            meta.dstp = hdr.tcp.dst_port;
        }
        else if(hdr.udp.isValid()){
            meta.srcp = hdr.udp.src_port;
            meta.dstp = hdr.udp.dst_port;
        }
        hdr.path_t.pathBF = hdr.path_t.pathBF|pathand;
        
        tbl_DownstreamRecord.apply();
        meta.tempid = global_hash.get({hdr.ipv4.dst_addr,hdr.ipv4.src_addr,hdr.ipv4.protocol,meta.srcp,meta.dstp});
        meta.tempcnt = 0;
        if(meta.DownRecord == 0 ){
            //pipe
            pipehash1();
            pipe1outid = pipe1updateaction.execute((bit<32>)meta.hashindex1[1:0]);
            //outid == 0: hit, add one
            //outid != 0: replace
            if(pipe1outid == 0)
            {
                meta.fcurrentcnt = pipe1_counteradd_one.execute((bit<32>)meta.hashindex1[1:0]);   //currentcnt记录，而tempcnt不变
                meta.replaced = 0;
            }
            else{
                meta.tempid = pipe1outid;
                meta.tempcnt = pipe1_counter_replace.execute((bit<32>)meta.hashindex1[1:0]);
                meta.replaced = 1;
            }


            //outid == 0: continue
            //outid == tempid：hit
            //else: replace
            pipehash2();
            pipe2outid = pipe2updateaction.execute((bit<32>)meta.hashindex2[1:0]);
            if(pipe2outid == meta.tempid &&meta.replaced==0 )
            {
                meta.fcurrentcnt = meta.fcurrentcnt + pipe2_counteradd.execute((bit<32>)meta.hashindex2[1:0]);
                meta.tempcnt = 0;
            }
            else if(pipe2outid == meta.tempid){
                pipe2_counteradd.execute((bit<32>)meta.hashindex2[1:0]);
                meta.tempcnt = 0;
            }
            else if(pipe2outid !=0){                
                meta.tempid = pipe2outid;
                meta.tempcnt = pipe2_counter_replace.execute((bit<32>)meta.hashindex2[1:0]);

            }


                // 
            pipehash3();
            pipe3outid = pipe3updateaction.execute((bit<32>)meta.hashindex3[1:0]);
            if(pipe3outid == meta.tempid && meta.replaced ==0)
            {
                meta.fcurrentcnt = meta.fcurrentcnt + pipe3_counteradd.execute((bit<32>)meta.hashindex3[1:0]);
            }
            else if(pipe3outid == meta.tempid){
                pipe3_counteradd.execute((bit<32>)meta.hashindex3[1:0]);
            }
            else if(pipe2outid !=0){
                meta.tempcnt = pipe3_counter_replace.execute((bit<32>)meta.hashindex3[1:0]);
            }

        }
        if(meta.needflow==1 && meta.fcurrentcnt[11:0] > 8)
            ig_dprsr_md.digest_type = 2;
        tbl_UpstreamRecord.apply();
    }  
}

parser SwitchEgressParser(
        packet_in pkt,
        out header_t hdr,
        out metadata_t meta_eg,
        out egress_intrinsic_metadata_t eg_intr_md) {


    TofinoEgressParser() tofino_eparser;
    state start {
        tofino_eparser.apply(pkt, eg_intr_md);
        transition parse_ethernet;
    }
    state parse_ethernet{
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            ETHERTYPE_IPV4 : parse_ipv4;
            default : accept;
        }
    }

    state parse_ipv4 {
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            17 : parse_udp;
            6 :  parse_tcp;
            default : reject;
        }
    }

    state parse_udp{
        pkt.extract(hdr.udp);
        transition accept;
    }

    state parse_tcp{
        pkt.extract(hdr.tcp);
        transition accept;
    }
}


control SwitchEgress(
    inout header_t hdr,
    inout metadata_t meta_eg,
    in egress_intrinsic_metadata_t eg_intr_md,
    in egress_intrinsic_metadata_from_parser_t eg_intr_md_from_prsr,
    inout egress_intrinsic_metadata_for_deparser_t eg_intr_md_for_dprs,
    inout egress_intrinsic_metadata_for_output_port_t eg_intr_md_for_oport
) {

    apply{
        
    }
}

control SwitchEgressDeparser(
        packet_out pkt,
        inout header_t hdr,
        in metadata_t meta_eg,
        in egress_intrinsic_metadata_for_deparser_t eg_intr_md_for_dprs) {
    Checksum() ipv4_checksum;
    apply {        
        hdr.ipv4.hdr_checksum = ipv4_checksum.update({
            hdr.ipv4.version,
            hdr.ipv4.ihl,
            hdr.ipv4.diffserv,
            hdr.ipv4.total_len,
            hdr.ipv4.identification,
            hdr.ipv4.flags,
            hdr.ipv4.frag_offset,
            hdr.ipv4.ttl,
            hdr.ipv4.protocol,
            hdr.ipv4.src_addr,
            hdr.ipv4.dst_addr});

         pkt.emit(hdr);
    }
}




Pipeline(SwitchIngressParser(),
         SwitchIngress(),
         SwitchIngressDeparser(),
         SwitchEgressParser(),
         SwitchEgress(),
         SwitchEgressDeparser()) pipe;

Switch(pipe) main;