route_lines = [
    "2a02:898::/32      via 2001:7f8:1::a500:8954:1 on eth1 [PS2 12:46] * (100) [AS8283i]",
    "154.0.154.0/23     unreachable [DIGITALOCEAN7 2017-01-13 from 5.101.110.2] * (100/-) [AS36909i]",
    "                   unreachable [HIVANE 2017-01-11 from 193.17.192.135] (100/-) [AS47583i]",
    "                   via 206.41.110.21 on bond0.895 [transit_as53264_nchc 2016-11-22] (100) [AS29713i]",
    "10.255.30.0/24     blackhole [static1 2017-01-14] * (200)",
    "2001:db8:123::/64  dev eth0 [direct1 10:14:07] * (240)",
    "                   via 2001:db8:123::10 on eth0 [rs1_ipv6 10:14:10] (100) [AS65010i]",
 ]

def test_route_summary(bird):
    for each in route_lines:
        print(bird._re_route_summary().match(each).groupdict())
