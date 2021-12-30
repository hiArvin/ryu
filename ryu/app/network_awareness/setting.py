# Common Setting for Network awareness module.


DISCOVERY_PERIOD = 10  # For discovering topology.

MONITOR_PERIOD = 1  # For monitoring traffic

DELAY_DETECTING_PERIOD = 2  # For detecting link delay.

TOSHOW = False  # For showing information in terminal

MAX_CAPACITY = 1000  # Max capacity of link

LINK_CAPACITY = {
    (1, 2): 5,
    (2, 1): 5,
    (2, 3): 5,
    (3, 2): 5,
    (3, 4): 5,
    (4, 3): 5,
    (4, 1): 5,
    (1, 4): 5
}  # Load pre-defined capability
