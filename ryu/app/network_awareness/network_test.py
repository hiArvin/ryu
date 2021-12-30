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
import network_awareness
from operator import attrgetter

class NetworkTest(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"network_awareness":network_awareness.NetworkAwareness}
    def __init__(self,*args,**kwargs):
        super(NetworkTest,self).__init__(*args,**kwargs)
        self.graph = nx.Graph()
        self.sw_module = lookup_service_brick('switches')
        self.awareness = kwargs['network_awareness']
        print(self.awareness.link_to_port)
