# conding=utf-8
from __future__ import division
from ryu import cfg
from ryu.base import app_manager
from ryu.base.app_manager import lookup_service_brick
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.topology.switches import Switches
from ryu.topology.switches import LLDPPacket
import networkx as nx
import time
from operator import attrgetter

import setting


CONF = cfg.CONF
class NetworkUtilityDelayDetector(app_manager.RyuApp):
    """
        NetworkDelayDetector is a Ryu app for collecting link delay.
    """

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(NetworkUtilityDelayDetector, self).__init__(*args, **kwargs)
        self.name = 'utility_delay_detector'
        self.sending_echo_request_interval = 0.05
        # Get the active object of swicthes and awareness module.
        # So that this module can use their data.
        self.sw_module = lookup_service_brick('switches')
        self.awareness = lookup_service_brick('awareness')


        self.datapaths = {}
        self.echo_latency = {}
        self.measure_thread = hub.spawn(self._detector)

        self.port_stats = {}
        self.port_speed = {}
        self.flow_stats = {}
        self.flow_speed = {}
        self.stats = {}
        self.port_features = {}
        self.free_bandwidth = {}
        self.graph = None
        self.capabilities = None
        self.best_paths = None
        # Start to green thread to monitor traffic and calculating
        # free bandwidth of links respectively.
        self.monitor_thread = hub.spawn(self._monitor)
        self.save_freebandwidth_thread = hub.spawn(self._save_bw_graph)

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


    def __monitor(self):
        """
            Main entry method of monitoring traffic.
        """
        while CONF.weight == 'delay':
            self.stats['flow'] = {}
            self.stats['port'] = {}
            for dp in self.datapaths.values():
                self.port_features.setdefault(dp.id, {})
                self._request_stats(dp)
                # refresh data.
                self.capabilities = None
                self.best_paths = None
            hub.sleep(setting.MONITOR_PERIOD)
            if self.stats['port']:
                self.show_stat()
                hub.sleep(1)

    def _save_bw_graph(self):
        """
            Save bandwidth data into networkx graph object.
        """
        while CONF.weight == 'delay':
            self.graph = self.create_bw_graph(self.free_bandwidth)
            self.logger.debug("save_freebandwidth")
            hub.sleep(setting.MONITOR_PERIOD)

    def create_bw_graph(self, bw_dict):
        """
            Save bandwidth data into networkx graph object.
        """
        try:
            graph = self.awareness.graph
            link_to_port = self.awareness.link_to_port
            for link in link_to_port:
                (src_dpid, dst_dpid) = link
                (src_port, dst_port) = link_to_port[link]
                if src_dpid in bw_dict and dst_dpid in bw_dict:
                    bw_src = bw_dict[src_dpid][src_port]
                    bw_dst = bw_dict[dst_dpid][dst_port]
                    bandwidth = min(bw_src, bw_dst)
                    # add key:value of bandwidth into graph.
                    graph[src_dpid][dst_dpid]['bandwidth'] = bandwidth
                else:
                    graph[src_dpid][dst_dpid]['bandwidth'] = 0
            return graph
        except:
            self.logger.info("Create bw graph exception")
            if self.awareness is None:
                self.awareness = lookup_service_brick('awareness')
            return self.awareness.graph

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

    def _save_stats(self, _dict, key, value, length):
        if key not in _dict:
            _dict[key] = []
        _dict[key].append(value)

        if len(_dict[key]) > length:
            _dict[key].pop(0)

    def _get_speed(self, now, pre, period):
        if period:
            return (now - pre) / (period)
        else:
            return 0

    def _get_free_bw(self, capacity, speed):
        # BW:Mbit/s
        return max(capacity/10**3 - speed * 8/10**6, 0)

    def _get_time(self, sec, nsec):
        return sec + nsec / (10 ** 9)

    def _get_period(self, n_sec, n_nsec, p_sec, p_nsec):
        return self._get_time(n_sec, n_nsec) - self._get_time(p_sec, p_nsec)

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
    def _port_stats_reply_handler(self, ev):
        """
            Save port's stats info
            Calculate port's speed and save it.
        """
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        self.stats['port'][dpid] = body
        self.free_bandwidth.setdefault(dpid, {})

        for stat in sorted(body, key=attrgetter('port_no')):
            port_no = stat.port_no
            if port_no != ofproto_v1_3.OFPP_LOCAL:
                key = (dpid, port_no)
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

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_stats_reply_handler(self, ev):
        """
            Save port description info.
        """
        msg = ev.msg
        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto

        config_dict = {ofproto.OFPPC_PORT_DOWN: "Down",
                       ofproto.OFPPC_NO_RECV: "No Recv",
                       ofproto.OFPPC_NO_FWD: "No Farward",
                       ofproto.OFPPC_NO_PACKET_IN: "No Packet-in"}

        state_dict = {ofproto.OFPPS_LINK_DOWN: "Down",
                      ofproto.OFPPS_BLOCKED: "Blocked",
                      ofproto.OFPPS_LIVE: "Live"}

        ports = []
        for p in ev.msg.body:
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

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status_handler(self, ev):
        """
            Handle the port status changed event.
        """
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
            print("switch%d: Illeagal port state %s %s"% (dpid, port_no, reason))

    def show_stat(self):
        '''
            Show statistics info according to data type.
            type: 'port' 'flow'
        '''
        if setting.TOSHOW is False:
            return

        bodys = self.stats['port']


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


    '''
        delay detector
    '''

    def _detector(self):
        """
            Delay detecting functon.
            Send echo request and calculate link delay periodically
        """
        while CONF.weight == 'delay':
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
            Seng echo request msg to datapath.
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

    @set_ev_cls(ofp_event.EventOFPEchoReply, MAIN_DISPATCHER)
    def echo_reply_handler(self, ev):
        """
            Handle the echo reply msg, and get the latency of link.
        """
        now_timestamp = time.time()
        try:
            latency = now_timestamp - eval(ev.msg.data)
            self.echo_latency[ev.msg.datapath.id] = latency
        except:
            return

    def get_delay(self, src, dst):
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
            re_delay = self.awareness.graph[dst][src]['lldpdelay']
            src_latency = self.echo_latency[src]
            dst_latency = self.echo_latency[dst]

            delay = (fwd_delay + re_delay - src_latency - dst_latency) / 2
            return max(delay, 0)
        except:
            return float('inf')

    def _save_lldp_delay(self, src=0, dst=0, lldpdelay=0):
        try:
            self.awareness.graph[src][dst]['lldpdelay'] = lldpdelay
        except:
            if self.awareness is None:
                self.awareness = lookup_service_brick('awareness')
            return

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """
            Parsing LLDP packet and get the delay of link.
        """
        msg = ev.msg
        try:
            src_dpid, src_port_no = LLDPPacket.lldp_parse(msg.data)
            dpid = msg.datapath.id
            if self.sw_module is None:
                self.sw_module = lookup_service_brick('switches')

            for port in self.sw_module.ports.keys():
                if src_dpid == port.dpid and src_port_no == port.port_no:
                    delay = self.sw_module.ports[port].delay
                    self._save_lldp_delay(src=src_dpid, dst=dpid,
                                          lldpdelay=delay)
        except LLDPPacket.LLDPUnknownFormat as e:
            return

    def show_delay_statis(self):
        if setting.TOSHOW and self.awareness is not None:
            self.logger.info("\nsrc   dst      delay")
            self.logger.info("---------------------------")
            for src in self.awareness.graph:
                for dst in self.awareness.graph[src]:
                    delay = self.awareness.graph[src][dst]['delay']
                    self.logger.info("%s<-->%s : %s" % (src, dst, delay))