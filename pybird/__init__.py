import logging
import re
import socket
from datetime import datetime, timedelta
from subprocess import PIPE, Popen


class PyBird:
    # BIRD reply codes: https://github.com/CZ-NIC/bird/blob/6c11dbcf28faa145cfb7310310a2a261fd4dd1f2/doc/reply_codes
    ignored_field_numbers = (0, 1, 13, 2002, 9001)
    error_fields = (13, 19, 8001, 8002, 8003, 9000, 9001, 9002)
    success_fields = (0, 3, 4, 18, 20)

    def __init__(
        self,
        socket_file,
        hostname=None,
        user=None,
        config_file=None,
        bird_cmd=None,
    ):
        """
        Basic pybird setup.
        Required argument: socket_file: full path to the BIRD control socket.
        """
        self.socket_file = socket_file
        self.hostname = hostname
        self.user = user
        self.config_file = config_file
        if not bird_cmd:
            self.bird_cmd = "birdc"
        else:
            self.bird_cmd = bird_cmd

        self.clean_input_re = re.compile(r"\W+")
        self.field_number_re = re.compile(r"^(\d+)[ -]")
        
        self.routes_field_imported_re = re.compile(r"(\d+) imported")
        self.routes_field_exported_re = re.compile(r"(\d+) exported")
        self.routes_field_filtered_re = re.compile(r"(\d+) filtered")
        self.routes_field_preferred_re = re.compile(r"(\d+) preferred")
        
        # self.routes_field_re = re.compile(r"(\d+) imported,.* (\d+) exported")
        self.log = logging.getLogger(__name__)

    def get_config(self):
        if not self.config_file:
            raise ValueError("config_file is not set")
        return self._read_file(self.config_file)

    def put_config(self, data):
        if not self.config_file:
            raise ValueError("config_file is not set")
        return self._write_file(data, self.config_file)

    def commit_config(self):
        return self.configure()

    def check_config(self):
        """Check configuration without applying it.

        Raise ValueError with the original text of the error,
        return None for success.
        """
        query = "configure check"
        data = self._send_query(query)
        if not self.socket_file:
            return data

        err = self._parse_configure(data)
        if err:
            raise ValueError(err)
        return None

    def get_bird_status(self):
        """Get the status of the BIRD instance. Returns a dict with keys:
        - router_id (string)
        - last_reboot (datetime)
        - last_reconfiguration (datetime)"""
        query = "show status"
        data = self._send_query(query)
        if not self.socket_file:
            return data
        return self._parse_status(data)

    def _parse_status(self, data):
        line_iterator = iter(data.splitlines())
        data = {}

        for line in line_iterator:
            line = line.strip()
            self.log.debug("PyBird: parse status: %s", line)
            (field_number, line) = self._extract_field_number(line)

            if field_number in self.ignored_field_numbers:
                continue

            if field_number == 1000:
                data["version"] = line.split(" ")[1]

            elif field_number == 1011:
                # Parse the status section, which looks like:
                # 1011-Router ID is 195.69.146.34
                # Hostname is bird2-router
                # Current server time is 10-01-2012 10:24:37
                # Last reboot on 03-01-2012 12:46:40
                # Last reconfiguration on 03-01-2012 12:46:40
                data["router_id"] = self._parse_router_status_line(line)

                line = next(line_iterator)
                if line.lstrip().startswith("Hostname is"):
                    data["hostname"] = line.split(" is ")[1]
                    line = next(line_iterator)
                # skip current server time
                self.log.debug("PyBird: parse status: %s", line)

                line = next(line_iterator)
                self.log.debug("PyBird: parse status: %s", line)
                data["last_reboot"] = self._parse_router_status_line(
                    line, parse_date=True
                )

                line = next(line_iterator)
                self.log.debug("PyBird: parse status: %s", line)
                data["last_reconfiguration"] = self._parse_router_status_line(
                    line, parse_date=True
                )

        return data

    def _parse_configure(self, data):
        """
                returns error on error, None on success
        0001 BIRD 1.4.5 ready.
        0002-Reading configuration from /home/grizz/c/20c/tstbird/dev3.conf
        8002 /home/grizz/c/20c/tstbird/dev3.conf, line 3: syntax error

        0001 BIRD 1.4.5 ready.
        0002-Reading configuration from /home/grizz/c/20c/tstbird/dev3.conf
        0020 Configuration OK

        0004 Reconfiguration in progress
        0018 Reconfiguration confirmed
        0003 Reconfigured

        bogus undo:
        0019 Nothing to do

        """

        for line in data.splitlines():
            self.log.debug("PyBird: parse configure: %s", line)
            fieldno, line = self._extract_field_number(line)

            if fieldno == 2:
                if not self.config_file:
                    self.config_file = line.split(" ")[3]

            elif fieldno in self.error_fields:
                return line

            elif fieldno in self.success_fields:
                return None
        raise ValueError("unable to parse configure response")

    def _parse_router_status_line(self, line, parse_date=False):
        """Parse a line like:
            Current server time is 10-01-2012 10:24:37.123
        optionally (if parse_date=True), parse it into a datetime"""
        data = line.strip().split(" ", 3)[-1]
        if parse_date:
            data = data.split(".")[0]
            try:
                return datetime.strptime(data, "%Y-%m-%d %H:%M:%S")
            # old versions of bird used DD-MM-YYYY
            except ValueError:
                return datetime.strptime(data, "%d-%m-%Y %H:%M:%S")
        else:
            return data

    def configure(self, soft=False, timeout=0):
        """
        birdc configure command
        """
        query = "configure"
        data = self._send_query(query)
        if not self.socket_file:
            return data

        err = self._parse_configure(data)
        if err:
            raise ValueError(err)

    def get_routes(self, table=None, prefix=None, peer=None, full=False):
        """
        Get all routes, or optionally for a specific table, prefix or peer.
        """
        query = "show route all"
        if full:
            query += f" all "
        if table:
            query += f" table {table}"
        if prefix:
            query += f" for {prefix}"
        if peer:
            query += f" protocol {peer}"
        data = self._send_query(query)
        return self._parse_route_data(data)

    # deprecated by get_routes_received
    def get_peer_prefixes_announced(self, peer_name):
        """Get prefixes announced by a specific peer, without applying
        filters - i.e. this includes routes which were not accepted"""
        clean_peer_name = self._clean_input(peer_name)
        query = "show route table T_{} all protocol {}".format(
            clean_peer_name, clean_peer_name
        )
        data = self._send_query(query)
        return self._parse_route_data(data)

    def get_routes_received(self, peer=None):
        return self.get_peer_prefixes_announced(peer)

    def get_peer_prefixes_exported(self, peer_name):
        """Get prefixes exported TO a specific peer"""
        clean_peer_name = self._clean_input(peer_name)
        query = "show route all table T_{} export {}".format(
            clean_peer_name, clean_peer_name
        )
        data = self._send_query(query)
        if not self.socket_file:
            return data
        return self._parse_route_data(data)

    def get_peer_prefixes_accepted(self, peer_name):
        """Get prefixes announced by a specific peer, which were also
        accepted by the filters"""
        query = "show route all protocol %s" % self._clean_input(peer_name)
        data = self._send_query(query)
        return self._parse_route_data(data)

    def get_peer_prefixes_rejected(self, peer_name):
        announced = self.get_peer_prefixes_announced(peer_name)
        accepted = self.get_peer_prefixes_accepted(peer_name)

        announced_prefixes = [i["prefix"] for i in announced]
        accepted_prefixes = [i["prefix"] for i in accepted]

        rejected_prefixes = [
            item for item in announced_prefixes if item not in accepted_prefixes
        ]
        rejected_routes = [
            item for item in announced if item["prefix"] in rejected_prefixes
        ]
        return rejected_routes

    def get_prefix_info(self, prefix, peer_name=None):
        """Get route-info for specified prefix"""
        query = "show route for %s all" % prefix
        if peer_name is not None:
            query += " protocol %s" % peer_name
        data = self._send_query(query)
        if not self.socket_file:
            return data
        return self._parse_route_data(data)

    def _parse_route_data(self, data, short=True):
        """Parse a blob like:
        0001 BIRD 1.3.3 ready.
        1007-2a02:898::/32      via 2001:7f8:1::a500:8954:1 on eth1 [PS2 12:46] * (100) [AS8283i]
        1008-   Type: BGP unicast univ
        1012-   BGP.origin: IGP
            BGP.as_path: 8954 8283
            BGP.next_hop: 2001:7f8:1::a500:8954:1 fe80::21f:caff:fe16:e02
            BGP.local_pref: 100
            BGP.community: (8954,620)
        [....]
        0000
        """
        lines = data.splitlines()
        routes = []
        
        bird2route = re.compile(r"([\sa-f0-9\.:\/]+)?(?:unicast|blackhole)\s+\[")
        bird1route = re.compile(r"(?:[a-f0-9\.:\/]+)?(\s+)?(?:via\s([^\s]+)\s+on\s+|\s+dev\s+)([\w\s]+)\[")

        self.log.debug("PyBird: parse route data: lines=%d", len(lines))
        route_summary = dict()
        prev_prefix = None
        counter = -1
        prev_number = 1007
        for line in lines:
            counter += 1
            (number, line) = self._extract_field_number(line)
            
            if number is None:
                number = prev_number
            prev_number = number
            
            if number == 1007:
                if len(route_summary) > 0:
                    routes.append(route_summary)
                    route_summary = {}
                    
                if line == "" or line.startswith("Table"):
                    continue
                
                if  bird2route.match(line):
                   # print("bird 2: ", line, lines[counter+1])
                   route_summary = self._parse_route_summary_bird2([line, lines[counter+1]])

                if bird1route.match(line):
                   # print("bird 1: ", line)
                   route_summary = self._parse_route_summary_bird1(line)
                
                if "prefix" in route_summary:
                    if route_summary["prefix"] is None:
                        route_summary["prefix"] = prev_prefix
                    else:
                        prev_prefix = route_summary["prefix"]

            if number == 1008:
                route_summary.update(self._parse_route_type(line))

            if number == 0:
                routes.append(route_summary)
                
            if not short:
                if number == 1012:
                    if not route_summary:
                        continue
                    
                    data = self._parse_route_detail(line)
                    if data["proto"] in route_summary:
                        route_summary[data["proto"]][data["atribute"]] = data["value"]
                    else:
                        route_summary[data["proto"]] = {}
                        route_summary[data["proto"]][data["atribute"]] = data["value"]

            if number == 8001:
                # network not in table
                return []

        return routes

    def _parse_route_summary_bird1(self, line):
        """Parse a line like:
        2a02:898::/32      via 2001:7f8:1::a500:8954:1 on eth1 [PS2 12:46] * (100) [AS8283i]
        1007-10.0.0.0/24          unicast [rs1_ipv4 10:03:04.485] * (100) [AS65010i]
                via 10.123.123.10 on eth0
        1007-                     unicast [rs2_ipv4 10:03:04.436] (100) [AS65010i]
                via 10.123.123.20 on eth0
        """
        rs = re.compile(
            r"(?P<prefix>[a-f0-9\.:\/]+)?(\s+)?((?:via\s+(?P<peer>[^\s]+)\s+on\s+|\s+dev\s+)(?P<interface>[^\s]+)|(?:\w+)?)?\s+"
            r"\[(?P<source>[^\s]+)\s+(?P<time>[^\]\s]+)(?:\s+from\s+(?P<peer2>[^\s]+))?\]\s+"
            r"(?:(?P<best>[*,!])\s+)?(?:\((?P<preference>[\w\/\-\?]+)\))?(?:\s+\[AS(?P<asn>\d+)[\w\?]\])?"
        )
        match = rs.match(line)
        if not match:
            raise ValueError(f"couldn't parse bird1 line '{line}'")
        # Note that split acts on sections of whitespace - not just single chars
        route = match.groupdict()
        # print(route)

        # python regex doesn't allow group name reuse
        if not route["peer"]:
            route["peer"] = route.pop("peer2")
        else:
            del route["peer2"]
        
        if route["best"] is not None:
            route["best"] = True
        else:
            route["best"] = False
        
        return route

    def _parse_route_summary_bird2(self, line):
        """Parse a line like:
        1007-10.0.0.0/24          unicast [rs1_ipv4 10:03:04.485] * (100) [AS65010i]
                via 10.123.123.10 on eth0
        1007-                     unicast [rs2_ipv4 10:03:04.436] (100) [AS65010i]
                via 10.123.123.20 on eth0
        """
        rs0 = re.compile(
            r"(\s)?(?P<prefix>[a-f0-9\.:\/]+)?(\s+)?(?:unicast|blackhole)\s+"
            r"\[(?P<source>[^\s]+)\s+(?P<time>[^\]\s]+)(?:\s+from\s+(?P<peer2>[^\s]+))?\]"
            r"\s+(?:(?P<best>[*,!])\s+)?(?:\((?P<preference>[\w\/\-\?]+)\))?(?:\s+\[AS(?P<asn>\d+)[\w\?]\])?"
        )
        rs1 = re.compile(r"(?:^\s+via\s+(?P<peer>[a-f0-9\.:\/]+)\s+on\s+|^\s+dev\s+)(?P<interface>[\w]+)")
        
        if len(line) < 2:
            raise ValueError(f"bird2 should have route in 2 lines")
        match0 = rs0.match(line[0])
        match1 = rs1.match(line[1])
        if not match0:
            raise ValueError(f"couldn't parse bird2 line '{line[0]}'")
        if not match1:
            raise ValueError(f"couldn't parse bird2 line '{line[1]}'")
        # Note that split acts on sections of whitespace - not just single chars
        route = match0.groupdict()
        route.update(match1.groupdict())
        
        # print(route)

        # python regex doesn't allow group name reuse
        if not route["peer"]:
            route["peer"] = route.pop("peer2")
        else:
            del route["peer2"]
        
        if route["best"] is not None:
            route["best"] = True
        else:
            route["best"] = False
        
        return route

    def _parse_route_type(self, line):
        """
        Parse a line like:
        1008-	Type: BGP unicast univ
        1008-	Type: device unicast univ
        """
        # match = re.match(r".+Type:\s+(?P<type>\w+)\s+(?P<scope>\w+)\s+(?P<family>\w+)", line)
        rs = re.compile(r"Type:\s+(?P<type>[\w\s]+)")
        match = rs.match(line)
        if not match:
            raise ValueError(f"couldn't parse Type line '{line}'")

        route_type = match.groupdict()

        return route_type

    def _parse_route_detail(self, line):
        """Parse a blob like:
        1012-   BGP.origin: IGP
            BGP.as_path: 8954 8283
            BGP.next_hop: 2001:7f8:1::a500:8954:1 fe80::21f:caff:fe16:e02
            BGP.local_pref: 100
            BGP.community: (8954,620)
        """
        rs = re.compile(
            r"^(?:(?P<proto>[\w]+))\.(?:(?P<atribute>[\w]+))(?:\:[\s]+(?P<value>[\s\w\W]+)?)"
        )
        #print(line)
        match = rs.match(line)
        result = match.groupdict()

        if result["atribute"] == "community":
            # convert (8954,220) (8954,620) to 8954:220 8954:620
            value = result["value"].replace(",", ":").replace("(", "").replace(")", "")
            result["value"] = value
        if result["atribute"] == "ext_community":
            # convert (rt, 1, 199524) to rt:1:199524
            value = result["value"].replace(", ", ":").replace("(", "").replace(")", "")
            result["value"] = value

        return result

    def get_peer_status(self, peer_name=None):
        """Get the status of all peers or a specific peer.

        Optional argument: peer_name: case-sensitive full name of a peer,
        as configured in BIRD.

        If no argument is given, returns a list of peers - each peer represented
        by a dict with fields. See README for a full list.

        If a peer_name argument is given, returns a single peer, represented
        as a dict. If the peer is not found, returns a zero length array.
        """
        if peer_name:
            query = 'show protocols all "%s"' % self._clean_input(peer_name)
        else:
            query = "show protocols all"

        data = self._send_query(query)
        if not self.socket_file:
            return data

        peers = self._parse_peer_data(data=data, data_contains_detail=True)

        if not peer_name:
            return peers

        if len(peers) == 0:
            return []
        elif len(peers) > 1:
            raise ValueError(
                "Searched for a specific peer, but got multiple returned from BIRD?"
            )
        else:
            return peers[0]

    def _parse_peer_data(self, data, data_contains_detail):
        """Parse the data from BIRD to find peer information."""
        lineiterator = iter(data.splitlines())
        peers = []

        peer_summary = None

        for line in lineiterator:
            line = line.strip()
            (field_number, line) = self._extract_field_number(line)

            if field_number in self.ignored_field_numbers:
                continue

            if field_number == 1002:
                peer_summary = self._parse_peer_summary(line)
                if peer_summary["protocol"] != "BGP":
                    peer_summary = None
                    continue

            # If there is no detail section to be expected,
            # we are done.
            if not data_contains_detail:
                peers.append_peer_summary()
                continue

            peer_detail = None
            if field_number == 1006:
                if not peer_summary:
                    # This is not detail of a BGP peer
                    continue

                # A peer summary spans multiple lines, read them all
                peer_detail_raw = []
                while line.strip() != "":
                    peer_detail_raw.append(line)
                    line = next(lineiterator)

                peer_detail = self._parse_peer_detail(peer_detail_raw)

                # Save the summary+detail info in our result
                peer_detail.update(peer_summary)
                peers.append(peer_detail)
                # Do not use this summary again on the next run
                peer_summary = None

        return peers

    def _parse_peer_summary(self, line):
        """Parse the summary of a peer line, like:
        PS1      BGP      T_PS1    start  Jun13       Passive

        Returns a dict with the fields:
            name, protocol, last_change, state, up
            ("PS1", "BGP", "Jun13", "Passive", False)

        """
        elements = line.split()

        try:
            if (
                ":" in elements[5]
            ):  # newer versions include a timestamp before the state
                state = elements[6]
            else:
                state = elements[5]
            up = state.lower() == "established"
        except IndexError:
            state = None
            up = None

        raw_datetime = elements[4]
        last_change = self._calculate_datetime(raw_datetime)

        return {
            "name": elements[0],
            "protocol": elements[1],
            "last_change": last_change,
            "state": state,
            "up": up,
        }

    def _parse_peer_detail(self, peer_detail_raw):
        """Parse the detailed peer information from BIRD, like:

        1006-  Description:    Peering AS8954 - InTouch
          Preference:     100
          Input filter:   ACCEPT
          Output filter:  ACCEPT
          Routes:         24 imported, 23 exported, 0 preferred
          Route change stats:     received   rejected   filtered    ignored   accepted
            Import updates:             50          3          19         0          0
            Import withdraws:            0          0        ---          0          0
            Export updates:              0          0          0        ---          0
            Export withdraws:            0        ---        ---        ---          0
            BGP state:          Established
              Session:          external route-server AS4
              Neighbor AS:      8954
              Neighbor ID:      85.184.4.5
              Neighbor address: 2001:7f8:1::a500:8954:1
              Source address:   2001:7f8:1::a519:7754:1
              Neighbor caps:    refresh AS4
              Route limit:      9/1000
              Hold timer:       112/180
              Keepalive timer:  16/60

        peer_detail_raw must be an array, where each element is a line of BIRD output.

        Returns a dict with the fields, if the peering is up:
            routes_imported, routes_exported, router_id
            and all combinations of:
            [import,export]_[updates,withdraws]_[received,rejected,filtered,ignored,accepted]
            wfor which the value above is not "---"

        """
        result = {}

        route_change_fields = [
            "import updates",
            "import withdraws",
            "export updates",
            "export withdraws",
        ]
        field_map = {
            "description": "description",
            "neighbor id": "router_id",
            "neighbor address": "address",
            "neighbor as": "asn",
            "source address": "source",
            "preference": "preference",
            "input filter": "input_filter",
            "output filter": "output_filter",
            "route limit": "route_limit",
            "hold timer": "hold_timer",
            "keepalive timer": "keepalive_timer"
        }
        lineiterator = iter(peer_detail_raw)

        for line in lineiterator:
            line = line.strip()
            try:
                (field, value) = line.split(":", 1)
            except ValueError:
                # skip lines "Channel ipv4/Channel ipv6"
                continue
            value = value.strip()

            # if field.lower() == "routes":
            #     routes = self.routes_field_re.findall(value)[0]
            #     result["routes_imported"] = int(routes[0])
            #     result["routes_exported"] = int(routes[1])

            if field.lower() == "routes":
                result["routes_imported"] = (
                    int(self.routes_field_imported_re.findall(value)[0])
                    if len(self.routes_field_imported_re.findall(value)) > 0
                    else 0
                )
                result["routes_exported"] = (
                    int(self.routes_field_exported_re.findall(value)[0])
                    if len(self.routes_field_exported_re.findall(value)) > 0
                    else 0
                )
                result["routes_filtered"] = (
                    int(self.routes_field_filtered_re.findall(value)[0])
                    if len(self.routes_field_filtered_re.findall(value)) > 0
                    else 0
                )
                result["routes_preferred"] = (
                    int(self.routes_field_preferred_re.findall(value)[0])
                    if len(self.routes_field_preferred_re.findall(value)) > 0
                    else 0
                )

            if field.lower() in route_change_fields:
                (received, rejected, filtered, ignored, accepted) = value.split()
                key_name_base = field.lower().replace(" ", "_")
                self._parse_route_stats(result, key_name_base + "_received", received)
                self._parse_route_stats(result, key_name_base + "_rejected", rejected)
                self._parse_route_stats(result, key_name_base + "_filtered", filtered)
                self._parse_route_stats(result, key_name_base + "_ignored", ignored)
                self._parse_route_stats(result, key_name_base + "_accepted", accepted)

            if field.lower() in field_map.keys():
                result[field_map[field.lower()]] = value

        return result

    def _parse_route_stats(self, result_dict, key_name, value):
        if value.strip() == "---":
            return
        result_dict[key_name] = int(value)

    def _extract_field_number(self, line):
        """Parse the field type number from a line.
        Line must start with a number, followed by a dash or space.

        Returns a tuple of (field_number, cleaned_line), where field_number
        is None if no number was found, and cleaned_line is the line without
        the field number, if applicable.
        """
        matches = self.field_number_re.findall(line)

        if len(matches):
            field_number = int(matches[0])
            cleaned_line = self.field_number_re.sub("", line).strip("-")
            return (field_number, cleaned_line.strip())
        else:
            return (None, line.strip())

    def _calculate_datetime(self, value, now=None):
        """Turn the BIRD date format into a python datetime."""

        if not now:
            now = datetime.now()

        # Case: YYYY-MM-DD HH:MM:SS
        try:
            return datetime(
                *map(
                    int,
                    (
                        value[:4],
                        value[5:7],
                        value[8:10],
                        value[11:13],
                        value[14:16],
                        value[17:19],
                    ),
                )
            ).strftime('%m/%d/%Y %H:%M:%S')
        except ValueError:
            pass

        # Case: YYYY-MM-DD
        try:
            return datetime(*map(int, (value[:4], value[5:7], value[8:10]))).strftime('%m/%d/%Y %H:%M:%S')
        except ValueError:
            pass

        # Case: HH:mm:ss.nnn or HH:mm or HH:mm:ss timestamp
        try:
            value = value.split(".")[0]  # strip any "".nnn" suffix
            try:
                parsed_value = datetime.strptime(value, "%H:%M")

            except ValueError:
                parsed_value = datetime.strptime(value, "%H:%M:%S")

            result_date = datetime(
                now.year, now.month, now.day, parsed_value.hour, parsed_value.minute
            )

            if now.hour < parsed_value.hour or (
                now.hour == parsed_value.hour and now.minute < parsed_value.minute
            ):
                result_date = result_date - timedelta(days=1)

            return result_date.strftime('%m/%d/%Y %H:%M:%S')
        except ValueError:
            # It's a different format, keep on processing
            pass

        # Case: "Jun13" timestamp
        try:
            parsed = datetime.strptime(value, "%b%d")

            # if now is past the month, it's this year, else last year
            if now.month == parsed.month:
                # bird shows time for same day
                if now.day <= parsed.day:
                    year = now.year - 1
                else:
                    year = now.year

            elif now.month > parsed.month:
                year = now.year

            else:
                year = now.year - 1

            result_date = datetime(year, parsed.month, parsed.day)
            return result_date.strftime('%m/%d/%Y %H:%M:%S')
        except ValueError:
            pass

        # Case: plain year
        try:
            year = int(value)
            return datetime(year, 1, 1).strftime('%m/%d/%Y %H:%M:%S')
        except ValueError:
            raise ValueError("Can not parse datetime: [%s]" % value)

    def _remote_cmd(self, cmd, inp=None):
        to = f"{self.user}@{self.hostname}"
        proc = Popen(
            ["ssh", "-o PasswordAuthentication=no", to, cmd], stdin=PIPE, stdout=PIPE
        )
        res = proc.communicate(input=inp)[0]
        return res

    def _read_file(self, fname):
        if self.hostname:
            cmd = "cat " + fname
            return self._remote_cmd(cmd)
        with open(fname) as fobj:
            return fobj.read()

    def _write_file(self, data, fname):
        if self.hostname:
            cmd = "cat >" + fname
            self._remote_cmd(cmd, inp=data)
            return

        with open(fname, "w") as fobj:
            fobj.write(data)
            return

    def _send_query(self, query):
        self.log.debug("PyBird: query: %s", query)
        if self.hostname:
            return self._remote_query(query)
        return self._socket_query(query)

    def _remote_query(self, query):
        """
        mimic a direct socket connect over ssh
        """
        cmd = f"{self.bird_cmd} -v -s {self.socket_file} '{query}'"
        res = self._remote_cmd(cmd)
        res += b"0000\n"
        return res.decode("utf-8")

    def _socket_query(self, query):
        """Open a socket to the BIRD control socket, send the query and get
        the response.
        """
        if not isinstance(query, bytes):
            query = query.encode("utf-8")
        if not query.endswith(b"\n"):
            query += b"\n"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.socket_file)
        sock.send(query)

        data = []

        while True:
            this_read = sock.recv(1024 * 1024)
            if not this_read:
                raise ValueError("Could not read additional data from BIRD")
            data.append(this_read)
            if len(this_read) > 256:
                tail = this_read[-256:].decode("utf-8")
            else:
                tail = b"".join(data[-2:])[-256:].decode("utf-8")
            if any(
                [
                    tail.find(f"\n{code:04}") != -1
                    for code in self.error_fields + self.success_fields
                ]
            ):
                break

        sock.close()
        return b"".join(data).decode("utf-8")

    def _clean_input(self, inp):
        """Clean the input string of anything not plain alphanumeric chars,
        return the cleaned string."""
        return self.clean_input_re.sub("", inp).strip()
