# -*- utf-8 -*-

from ryu.base import app_manager
from ryu.base.app_manager import lookup_service_brick
from ryu.controller.handler import set_ev_cls
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER,CONFIG_DISPATCHER,DEAD_DISPATCHER
from ryu.lib.packet import packet,ethernet
from ryu.topology import event
from ryu.topology.api import get_switch,get_link
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.topology.switches import LLDPPacket
from ryu import cfg
import networkx as nx
import time
import setting
from operator import attrgetter
CONF = cfg.CONF

class MyShortestForwarding(app_manager.RyuApp):
    '''
    class to achive shortest path to forward, based on minimum hop count
    '''
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self,*args,**kwargs):
        super(MyShortestForwarding,self).__init__(*args,**kwargs)

        #set data structor for topo construction
        self.network = nx.DiGraph()        #store the dj graph
        self.paths = {}        #store the shortest path
        self.topology_api_app = self
        # ====================================================================================
        # for network awareness
        self.link_to_port = {}  # (src_dpid,dst_dpid)->(src_port,dst_port)
        self.access_table = {}  # {(sw,port) :[host1_ip]}
        self.switch_port_table = {}  # dpip->port_num
        self.access_ports = {}  # dpid->port_num
        self.interior_ports = {}  # dpid->port_num

        self.graph = nx.DiGraph()
        self.pre_graph = nx.DiGraph()
        self.pre_access_table = {}
        self.pre_link_to_port = {}
        self.shortest_paths = None
        # Start a green thread to discover network resource.
        self.discover_thread = hub.spawn(self._discover)

        # ====================================================================================
        self.datapaths = {}
        self.port_stats = {}
        self.port_speed = {}
        self.stats = {}
        self.port_features = {}
        self.free_bandwidth = {}
        self.capabilities = None

        self.sending_echo_request_interval = 0.05
        self.sw_module = lookup_service_brick('switches')
        self.echo_latency = {}

        self.monitor_thread = hub.spawn(self._monitor)

        self.measure_thread = hub.spawn(self._detector)


    '''
        for all the programme
    '''
    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """
            Record datapath's info
        """
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if not datapath.id in self.datapaths:
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]



    def _detector(self):
        """
            Delay detecting functon.
            Send echo request and calculate link delay periodically
        """
        while(True):
            self._send_echo_request()
            self.create_link_delay()
            try:
                self.awareness.shortest_paths = {}
                self.logger.debug("Refresh the shortest_paths")
            except:
                self.awareness = lookup_service_brick('awareness')
            self.show_delay_statis()
            hub.sleep(setting.DELAY_DETECTING_PERIOD)

    def _send_echo_request(self):
        """
            Send echo request msg to datapath.
        """
        for datapath in self.datapaths.values():
            parser = datapath.ofproto_parser
            echo_req = parser.OFPEchoRequest(datapath,
                                             data=bytes("%.12f" % time.time(), encoding="utf8"))
            datapath.send_msg(echo_req)
            # Important! Don't send echo request together, Because it will
            # generate a lot of echo reply almost in the same time.
            # which will generate a lot of delay of waiting in queue
            # when processing echo reply in echo_reply_handler.

            hub.sleep(self.sending_echo_request_interval)

    def create_link_delay(self):
        """
            Create link delay data, and save it into graph object.
        """
        try:
            for src in self.awareness.graph:
                for dst in self.awareness.graph[src]:
                    if src == dst:
                        self.awareness.graph[src][dst]['delay'] = 0
                        continue
                    delay = self.get_delay(src, dst)
                    self.awareness.graph[src][dst]['delay'] = delay
        except:
            if self.awareness is None:
                self.awareness = lookup_service_brick('awareness')
            return

    def get_delay(self,src,dst):
        """
                Get link delay.
                            Controller
                            |        |
            src echo latency|        |dst echo latency
                            |        |
                       SwitchA-------SwitchB

                        fwd_delay--->
                            <----reply_delay
                delay = (forward delay + reply delay - src datapath's echo latency
        """
        try:
            fwd_delay = self.awareness.graph[src][dst]['lldpdelay']
            print(fwd_delay)
            re_delay = self.awareness.graph[dst][src]['lldpdelay']
            print(re_delay)
            src_latency = self.echo_latency[src]
            dst_latency = self.echo_latency[dst]


            delay = (fwd_delay + re_delay - src_latency - dst_latency) / 2
            return max(delay, 0)
        except:
            return float('inf')


    @set_ev_cls(ofp_event.EventOFPEchoReply,MAIN_DISPATCHER)
    def echo_reply_handler(self,ev):
        """
            Handle the echo reply msg, and get the latency of link.
        """
        msg = ev.msg
        dpid = msg.datapath.id
        now_timestamp = time.time()
        try:
            record_time = eval(ev.msg.data)
            latancy = now_timestamp - record_time
            self.echo_latency[dpid]=latancy
        except:
            return

    @set_ev_cls(ofp_event.EventOFPPacketIn,MAIN_DISPATCHER)
    def packet_in_handler(self,ev):
        print('111')
        msg = ev.msg
        try:
            src_dpid, src_port_no = LLDPPacket.lldp_parse(msg.data)
            dpid = ev.msg.datapath.id
            if self.sw_module is None:
                self.sw_module = lookup_service_brick('switches')

            for port in self.sw_module.ports.keys():
                if src_dpid == port.dpid and src_port_no == port.port_no:
                    delay = self.sw_module.ports[port].delay
                    self._save_lldp_delay(src=src_dpid, dst=dpid,
                                          lldpdelay=delay)
        except LLDPPacket.LLDPUnknownFormat as e:
            return

    def _save_lldp_delay(self, src=0, dst=0, lldpdelay=0):
        try:
            self.awareness.graph[src][dst]['lldpdelay'] = lldpdelay
        except:
            return
    '''
        以下是利用率的测量
    '''
    def _monitor(self):
        '''
            monitor 微程 发送port状态反馈
        '''
        while True:
            self.stats= {}
            for dp in self.datapaths.values():
                self.port_features.setdefault(dp.id,{})
                self._request_stats(dp)
                # refresh
                self.capabilities = None
            hub.sleep(setting.MONITOR_PERIOD)
            if self.stats:
                self.show_stats()

    def _request_stats(self, datapath):
        """
           Sending request msg to datapath
        """
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortDescStatsRequest(datapath, 0)
        datapath.send_msg(req)

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply(self,ev):
        msg = ev.msg
        body = msg.body
        dpid = msg.datapath.id

        self.stats[dpid] = body
        self.free_bandwidth.setdefault(dpid,{})

        for stat in sorted(body, key=attrgetter('port_no')):
            port_no = stat.port_no
            if port_no != ofproto_v1_3.OFPP_LOCAL:
                key = (dpid,port_no)
                value = (stat.tx_bytes, stat.rx_bytes, stat.rx_errors,
                         stat.duration_sec, stat.duration_nsec)
                self._save_stats(self.port_stats, key, value, 5)

                # Get port speed.
                pre = 0
                period = setting.MONITOR_PERIOD
                tmp = self.port_stats[key]
                if len(tmp) > 1:
                    pre = tmp[-2][0] + tmp[-2][1]
                    period = self._get_period(tmp[-1][3], tmp[-1][4],
                                              tmp[-2][3], tmp[-2][4])
                speed = self._get_speed(
                    self.port_stats[key][-1][0] + self.port_stats[key][-1][1],
                    pre, period)
                self._save_stats(self.port_speed, key, speed, 5)
                self._save_freebandwidth(dpid, port_no, speed)
                # print("key", key)
                #
                # print("value", value)

    def _save_freebandwidth(self, dpid, port_no, speed):
        # Calculate free bandwidth of port and save it.
        port_state = self.port_features.get(dpid).get(port_no)
        if port_state:
            capacity = port_state[2]
            curr_bw = self._get_free_bw(capacity, speed)
            self.free_bandwidth[dpid].setdefault(port_no, None)
            self.free_bandwidth[dpid][port_no] = curr_bw
        else:
            self.logger.info("Fail in getting port state")

    def _get_free_bw(self, capacity, speed):
        # BW:Mbit/s
        return max(capacity/10**3 - speed * 8/10**6, 0)

    def _get_speed(self, now, pre, period):
        if period:
            return (now - pre) / (period)
        else:
            return 0

    def _save_stats(self, _dict, key, value, length):
        if key not in _dict:
            _dict[key] = []
        _dict[key].append(value)

        if len(_dict[key]) > length:
            _dict[key].pop(0)

    def _get_time(self, sec, nsec):
        return sec + nsec / (10 ** 9)

    def _get_period(self, n_sec, n_nsec, p_sec, p_nsec):
        return self._get_time(n_sec, n_nsec) - self._get_time(p_sec, p_nsec)


    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def _port_desc_stats_reply_handler(self,ev):
        msg = ev.msg
        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto

        # 便于理解
        config_dict = {ofproto.OFPPC_PORT_DOWN: "Down",
                       ofproto.OFPPC_NO_RECV: "No Recv",
                       ofproto.OFPPC_NO_FWD: "No Farward",
                       ofproto.OFPPC_NO_PACKET_IN: "No Packet-in"}

        state_dict = {ofproto.OFPPS_LINK_DOWN: "Down",
                      ofproto.OFPPS_BLOCKED: "Blocked",
                      ofproto.OFPPS_LIVE: "Live"}

        ports=[]
        for p in msg.body:
            # 以字符串形式存储
            ports.append('port_no=%d hw_addr=%s name=%s config=0x%08x '
                         'state=0x%08x curr=0x%08x advertised=0x%08x '
                         'supported=0x%08x peer=0x%08x curr_speed=%d '
                         'max_speed=%d' %
                         (p.port_no, p.hw_addr,
                          p.name, p.config,
                          p.state, p.curr, p.advertised,
                          p.supported, p.peer, p.curr_speed,
                          p.max_speed))
            if p.config in config_dict:
                config = config_dict[p.config]
            else:
                config = "up"

            if p.state in state_dict:
                state = state_dict[p.state]
            else:
                state = "up"

            port_feature = (config, state, p.curr_speed)
            self.port_features[dpid][p.port_no] = port_feature

    @set_ev_cls(ofp_event.EventOFPPortStatus,MAIN_DISPATCHER)
    def _port_status_hander(self,ev):
        msg = ev.msg
        reason = msg.reason
        port_no = msg.desc.port_no

        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto

        reason_dict = {ofproto.OFPPR_ADD: "added",
                       ofproto.OFPPR_DELETE: "deleted",
                       ofproto.OFPPR_MODIFY: "modified", }
        if reason in reason_dict:
            print("switch%d: port %s %s" % (dpid, reason_dict[reason], port_no))
        else:
            print("switch%d: Illeagal port state %s %s" % (port_no, reason_dict[reason], reason))


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures,CONFIG_DISPATCHER)
    def switch_features_handler(self,ev):
        '''
        manage the initial link between switch and controller
        '''
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        match = ofp_parser.OFPMatch()    #for all packet first arrive, match it successful, send it ro controller
        actions  = [ofp_parser.OFPActionOutput(
                            ofproto.OFPP_CONTROLLER,ofproto.OFPCML_NO_BUFFER
                            )]

        self.add_flow(datapath, 0, match, actions)

    def add_flow(self,datapath,priority,match,actions):
        '''
        fulfil the function to add flow entry to switch
        '''
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        inst = [ofp_parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions)]

        mod = ofp_parser.OFPFlowMod(datapath=datapath,priority=priority,match=match,instructions=inst)

        datapath.send_msg(mod)


    @set_ev_cls(ofp_event.EventOFPPacketIn,MAIN_DISPATCHER)
    def packet_in_handler(self,ev):
        '''
        manage the packet which comes from switch
        '''
        #first get event infomation
        print(222)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        in_port = msg.match['in_port']
        dpid = datapath.id

        #second get ethernet protocol message
        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)

        eth_src = eth_pkt.src     #note: mac info willn`t  change in network
        eth_dst = eth_pkt.dst

        out_port = self.get_out_port(datapath,eth_src,eth_dst,in_port)
        actions = [ofp_parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = ofp_parser.OFPMatch(in_port=in_port,eth_dst=eth_dst)
            self.add_flow(datapath,1,match,actions)

        out = ofp_parser.OFPPacketOut(
                datapath=datapath,buffer_id=msg.buffer_id,in_port=in_port,
                actions=actions,data=msg.data
            )

        datapath.send_msg(out)


    '''
        for network awareness
    '''
    def _discover(self):
        i = 0
        while True:
            if i % 5 == 0:
                self.get_topology(None)
                i = 0
            hub.sleep(setting.DISCOVERY_PERIOD)
            i = i + 1

    @set_ev_cls(event.EventSwitchEnter,[CONFIG_DISPATCHER,MAIN_DISPATCHER])
    # event is not from openflow protocol, is come from switchs` state changed,
    # just like: link to controller at the first time or send packet to controller
    def get_topology(self,ev):
        """
            Get topology info and calculate shortest paths.
        """
        switch_list = get_switch(self.topology_api_app, None)
        self.create_port_map(switch_list)
        self.switches = self.switch_port_table.keys()
        links = get_link(self.topology_api_app, None)
        self.create_interior_links(links)
        self.create_access_ports()
        self.get_graph(self.link_to_port.keys())
        self.shortest_paths = self.all_k_shortest_paths(
            self.graph, weight='weight', k=CONF.k_paths)

    def create_port_map(self, switch_list):
        """
            Create interior_port table and access_port table.
        """
        for sw in switch_list:
            dpid = sw.dp.id
            self.switch_port_table.setdefault(dpid, set())
            self.interior_ports.setdefault(dpid, set())
            self.access_ports.setdefault(dpid, set())

            for p in sw.ports:
                self.switch_port_table[dpid].add(p.port_no)

    def create_interior_links(self, link_list):
        """
            Get links`srouce port to dst port  from link_list,
            link_to_port:(src_dpid,dst_dpid)->(src_port,dst_port)
        """
        for link in link_list:
            src = link.src
            dst = link.dst
            self.link_to_port[
                (src.dpid, dst.dpid)] = (src.port_no, dst.port_no)

            # Find the access ports and interiorior ports
            if link.src.dpid in self.switches:
                self.interior_ports[link.src.dpid].add(link.src.port_no)
            if link.dst.dpid in self.switches:
                self.interior_ports[link.dst.dpid].add(link.dst.port_no)

    def create_access_ports(self):
        """
            Get ports without link into access_ports
        """
        for sw in self.switch_port_table:
            all_port_table = self.switch_port_table[sw]
            interior_port = self.interior_ports[sw]
            self.access_ports[sw] = all_port_table - interior_port

    def get_graph(self, link_list):
        """
            Get Adjacency matrix from link_to_port
        """
        for src in self.switches:
            for dst in self.switches:
                if src == dst:
                    self.graph.add_edge(src, dst, weight=0)
                elif (src, dst) in link_list:
                    self.graph.add_edge(src, dst, weight=1)
        return self.graph

    def k_shortest_paths(self, graph, src, dst, weight='weight', k=1):
        """
            Great K shortest paths of src to dst.
        """
        generator = nx.shortest_simple_paths(graph, source=src,
                                             target=dst, weight=weight)
        shortest_paths = []
        try:
            for path in generator:
                if k <= 0:
                    break
                shortest_paths.append(path)
                k -= 1
            return shortest_paths
        except:
            self.logger.debug("No path between %s and %s" % (src, dst))

    def all_k_shortest_paths(self, graph, weight='weight', k=1):
        """
            Creat all K shortest paths between datapaths.
        """
        _graph = copy.deepcopy(graph)
        paths = {}

        # Find ksp in graph.
        for src in _graph.nodes():
            paths.setdefault(src, {src: [[src] for i in range(k)]})
            for dst in _graph.nodes():
                if src == dst:
                    continue
                paths[src].setdefault(dst, [])
                paths[src][dst] = self.k_shortest_paths(_graph, src, dst,
                                                        weight=weight, k=k)
        return paths
    ######################################################################


    def get_out_port(self,datapath,src,dst,in_port):
        '''
        datapath: is current datapath info
        src,dst: both are the host info
        in_port: is current datapath in_port
        '''
        dpid = datapath.id

        #the first :Doesn`t find src host at graph
        if src not in self.network:
            self.network.add_node(src)
            self.network.add_edge(dpid, src, attr_dict={'port':in_port})
            self.network.add_edge(src, dpid)
            self.paths.setdefault(src, {})

        #second: search the shortest path, from src to dst host
        if dst in self.network:
            if dst not in self.paths[src]:    #if not cache src to dst path,then to find it
                path = nx.shortest_path(self.network,src,dst)
                self.paths[src][dst]=path

            path = self.paths[src][dst]
            next_hop = path[path.index(dpid)+1]
            #print("1ooooooooooooooooooo")
            #print(self.network[dpid][next_hop])
            out_port = self.network[dpid][next_hop]['attr_dict']['port']
            #print("2ooooooooooooooooooo")
            #print(out_port)

            #get path info
            #print("6666666666 find dst")
            print(path)
        else:
            out_port = datapath.ofproto.OFPP_FLOOD    #By flood, to find dst, when dst get packet, dst will send a new back,the graph will record dst info
            #print("8888888888 not find dst")
        return out_port


    def show_stats(self):
        '''
            Show statistics info according to data type.
            type: 'port' 'flow'
        '''
        if setting.TOSHOW is False:
            return

        bodys = self.stats


        print('datapath             port   ''rx-pkts  rx-bytes rx-error '
              'tx-pkts  tx-bytes tx-error  port-speed(B/s)'
              ' current-capacity(Kbps)  '
              'port-stat   link-stat')
        print('----------------   -------- ''-------- -------- -------- '
              '-------- -------- -------- '
              '----------------  ----------------   '
              '   -----------    -----------')
        format = '%016x %8x %8d %8d %8d %8d %8d %8d %8.1f %16d %16s %16s'
        for dpid in bodys.keys():
            for stat in sorted(bodys[dpid], key=attrgetter('port_no')):
                if stat.port_no != ofproto_v1_3.OFPP_LOCAL:
                    print(format % (
                        dpid, stat.port_no,
                        stat.rx_packets, stat.rx_bytes, stat.rx_errors,
                        stat.tx_packets, stat.tx_bytes, stat.tx_errors,
                        abs(self.port_speed[(dpid, stat.port_no)][-1]),
                        self.port_features[dpid][stat.port_no][2],
                        self.port_features[dpid][stat.port_no][0],
                        self.port_features[dpid][stat.port_no][1]))
        print('\n')

    def show_delay_statis(self):
        if setting.TOSHOW and self.awareness is not None:
            self.logger.info("\nsrc   dst      delay")
            self.logger.info("---------------------------")
            for src in self.awareness.graph:
                for dst in self.awareness.graph[src]:
                    delay = self.awareness.graph[src][dst]['delay']
                    self.logger.info("%s<-->%s : %s" % (src, dst, delay))