#!/usr/bin/env python3

import datetime
import json
import os
import pubsub
import random
import time
import threading
from typing import Dict, Any, List, Optional, Union, Tuple

import meshtastic
import meshtastic.serial_interface
import meshtastic.tcp_interface
import openai
import reverse_geocode
import tomli

class BotState:
    """a container for all the bot's runtime state"""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.lock = threading.Lock()
        self.state_keys = [
            'nodedb_info', 'nodedb_positions', 'nodedb_following',
            'message_buffers', 'rate_limits', 'idle_context_messages',
            'neighbors'
        ]
        for key in self.state_keys:
            setattr(self, key, {})

    def save_state(self, key: str) -> None:
        """save a single state attribute to a json file"""
        if not hasattr(self, key):
            return
        with self.lock:
            data = getattr(self, key)
            path = os.path.join(self.config['paths']['data_dir'], f"{key}.json")
            with open(path, 'w') as f:
                json.dump(data, f)
            #print(f"> saved {len(data)} records to {path}")

    def load_state(self, key: str) -> None:
        """load a single state attribute from a json file"""
        with self.lock:
            path = os.path.join(self.config['paths']['data_dir'], f"{key}.json")
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                    setattr(self, key, data)
                    print(f"> loaded {len(data)} records from {path}")

    def save_all(self) -> None:
        """save all state attributes to their respective files"""
        for key in self.state_keys:
            self.save_state(key)

    def load_all(self) -> None:
        """load all state attributes from their respective files"""
        for key in self.state_keys:
            self.load_state(key)

def merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """recursively merge override config into base config"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_config(result[key], value)
        else:
            result[key] = value
    return result

def load_config_file(path: str) -> Dict[str, Any]:
    """load single toml config file"""
    if os.path.exists(path):
        with open(path, "rb") as f:
            return tomli.load(f)
    return {}

def load_config(config_files: List[str] = ["config.default.toml", "config.toml"]) -> Dict[str, Any]:
    """load and merge multiple config files in order"""
    config: Dict[str, Any] = {}
    for file_path in config_files:
        file_config = load_config_file(file_path)
        config = merge_config(config, file_config)
    return config


def create_interface(config: Dict[str, Any]) -> Any:
    """create meshtastic interface based on config"""
    iface_type = config['interface']['type']
    if iface_type == "serial":
        device_path = config['interface']['device']
        print(f"> creating serial interface on {device_path}")
        return meshtastic.serial_interface.SerialInterface(devPath=device_path)
    elif iface_type == "tcp":
        hostname = config['interface']['tcp_hostname']
        port = config['interface']['tcp_port']
        print(f"> creating tcp interface on {hostname}:{port}")
        return meshtastic.tcp_interface.TCPInterface(hostname=hostname, portNumber=port)
    else:
        raise Exception(f"unsupported interface type: {iface_type}")

def format_headers(packet, interface) -> str:
    from_id = packet["fromId"]
    to_id = packet["toId"]

    hops_str = ""
    if 'hopStart' in packet and 'hopLimit' in packet:
        hops_away = packet['hopStart'] - packet['hopLimit']
        hops_str = "[" + str(hops_away) + " hops] "

    from_short, from_long, from_name = find_node_name(interface.meshbot_state.nodedb_info, from_id)
    to_short, to_long, to_name = find_node_name(interface.meshbot_state.nodedb_info, to_id)

    return hops_str + f"from {from_name} [{from_id}] to {to_name} [{to_id}]"

def format_device_metrics(metrics: Dict[str, Any]) -> str:
    """convert device metrics to human-readable format"""
    return "ChUtil: %s, airUtilTx: %s" % (metrics['channelUtilization'], metrics['airUtilTx'])

def format_local_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    """convert local statistics to human-readable format"""
    return stats

def format_env_metrics(env: Dict[str, Any]) -> Dict[str, Any]:
    """convert environmental metrics to human-readable format"""
    return env

def format_date() -> str:
    return str(datetime.datetime.now())

def format_position(position: Dict[str, Any]) -> str:
    """convert position data to coordinate string for display"""
    lat = position["latitude"]
    lon = position["longitude"]
    position_text = f"{lat},{lon}"
    return position_text

def format_neighborinfo(neighborinfo: Dict[str, Any]) -> str:
    return str(neighborinfo["neighbors"])

def format_neighbor_details(data: Dict[str, Any]) -> str:
    """convert neighbor data to a formatted string for display"""
    hops = data.get('hops_away', '?')
    snr = data.get('snr')
    snr_str = f"{snr:.2f}" if snr is not None else "N/A"
    return f"{hops} hops, SNR {snr_str}"

def format_node_name(node_user: Dict[str, Any]) -> str:
    """create display name combining long and short names"""
    node_longname = node_user["longName"]
    node_shortname = node_user["shortName"]
    node_name = f"{node_longname} ({node_shortname})"
    return node_name

def synchronize_location(interface: Any) -> None:
    position = interface.meshbot_state.local_position
    rev = reverse_geocode.get((position['latitude'], position['longitude']))
    interface.meshbot_state.local_location = '%s, %s, %s, %s' % (rev['city'], rev['county'], rev['state'], rev['country'])

def rate_limit_wait_time(interface: Any, target_key: str) -> int:
    """calculate seconds to wait before allowing next message from target"""
    now = time.time()
    state = interface.meshbot_state
    if target_key not in state.rate_limits:
        state.rate_limits[target_key] = []

    config = state.config
    window = config['rate_limiting']['window']
    max_msgs = config['rate_limiting']['max']

    # remove old entries outside the window
    state.rate_limits[target_key] = [
        t for t in state.rate_limits[target_key]
        if now - t < window
    ]

    if len(state.rate_limits[target_key]) >= max_msgs:
        oldest_time = min(state.rate_limits[target_key])
        return int(window - (now - oldest_time))

    state.rate_limits[target_key].append(now)
    return 0

def limit_message_size(text: str, interface: Any) -> str:
    """truncate messages to fit mesh network size constraints"""
    max_length = interface.meshbot_state.config['message']['max_length']
    if len(text) <= max_length:
        return text
    return text[:max_length-5] + "[...]"

def find_node_name(nodedb_info: Dict[str, Any], node_id: str) -> Tuple[str, str, str]:
    """lookup node names from nodedb or fall back to id"""
    if node_id in nodedb_info:
        node_user = nodedb_info[node_id]
        short_name = node_user['shortName']
        long_name = node_user['longName']
        display_name = format_node_name(node_user)
        return short_name, long_name, display_name
    return "", "", node_id

def get_conversation_id(channel: Optional[int], from_id: Optional[str], to_id: Optional[str]) -> str:
    """generate unique identifier for conversation context"""
    if channel is not None:
        return f"channel_{channel}"
    else:
        # sort ids to ensure consistent conversation id for direct messages
        ids = sorted([from_id or '', to_id or ''])
        return f"dm_{ids[0]}_{ids[1]}"

def add_message_to_buffer(interface: Any, conversation_id: str, from_name: str, text: str) -> None:
    """maintain rolling conversation history for llm context"""
    state = interface.meshbot_state
    if conversation_id not in state.message_buffers:
        state.message_buffers[conversation_id] = []

    message = {"from": from_name, "text": text}
    state.message_buffers[conversation_id].append(message)

    buffer_size = state.config['llm']['context_buffer_size']
    if len(state.message_buffers[conversation_id]) > buffer_size:
        state.message_buffers[conversation_id].pop(0)

def get_context_messages(interface, conversation_id):
    """retrieve conversation history for llm context"""
    return interface.meshbot_state.message_buffers.get(conversation_id, [])

def send_llm_request(interface, context_messages=None, base_url="http://localhost:7860/v1", api_key="fake", model=None, max_tokens=None):
    """send conversation context to llm and return response"""
    state = interface.meshbot_state
    config = state.config
    if model is None:
        model = config['llm']['default_model']
    if max_tokens is None:
        max_tokens = config['llm']['default_max_tokens']

    client = openai.OpenAI(api_key=api_key, base_url=base_url)
    system_message = config['llm']['system_prompt'].format(**{
            "longname": state.local_long,
            "shortname": state.local_short,
            "location": state.local_location,
            "metrics": state.local_metrics,
            "date": format_date(),
    })
    messages = [
        {"role": "system", "content": system_message}
    ]
    context_lines = []
    for msg in context_messages:
        context_lines.append(f"{msg['from']}: {msg['text']}")
    context_text = "\n".join(context_lines)
    messages.append({"role": "user", "content": context_text})

    request_params = {"model": model, "messages": messages}
    if max_tokens is not None:
        request_params["max_tokens"] = max_tokens
    print(f"> beginning llm request: {messages}")
    start_time = time.time()
    response = client.chat.completions.create(**request_params)
    duration = time.time() - start_time
    content = response.choices[0].message.content.strip()
    print(f"> completed llm request in {duration:.2f}s: {content}")
    prefix = interface.meshbot_state.local_name + ": "
    if content.startswith(prefix):
        content = content[len(prefix):]
    return content

def mesh_send_direct(to_id, text, interface):
    """send direct message to specific node"""
    text = limit_message_size(text, interface)
    print(f"> sending text direct to user {to_id}: {text}")
    interface.sendText(text, to_id, wantAck=True)

def mesh_send_if_subscribed(to_id, text, interface):
    if interface.meshbot_state.nodedb_following.get(to_id, False) == True:
        mesh_send_direct(to_id, text, interface)

def mesh_send_channel(channel, text, interface):
    """broadcast message to channel"""
    text = limit_message_size(text, interface)
    print(f"> sending text to channel {channel}: {text}")
    interface.sendText(text, "^all", wantAck=True, channelIndex=channel)

def prefix_matched_query(text, prefixes):
    """check if text starts with any command prefix"""
    for pfx in prefixes:
        if text.upper().startswith(pfx.upper()):
            return True, text[len(pfx):]
    return False, text

def handle_text_message(channel, from_id, from_short, from_name, to_id, to_short, to_name, text, interface):
    """process incoming text messages and generate appropriate replies"""
    conversation_id = get_conversation_id(channel, from_id, to_id)

    wait_time = rate_limit_wait_time(interface, f"message_{from_id}")
    if wait_time > 0:
        print(f"> rate limited, skipping message from {from_name} [{from_id}], (wait {wait_time}s)")
        return

    matched, query = prefix_matched_query(text, interface.meshbot_state.name_prefixes)
    add_message_to_buffer(interface, conversation_id, from_name, query)

    config = interface.meshbot_state.config
    unsolicited = random.random()
    if to_id == "^all" and not matched:
        if unsolicited > config['channel']['unsolicited_ratio']:
            print(f"> skipping unprefixed channel message from {from_name} [{from_id}]): {unsolicited} > {config['channel']['unsolicited_ratio']}")
            return

    print(f"> processing query from {from_name} [{from_id}]: {query}")

    llm_matched, llm_query = prefix_matched_query(query, config['llm']['command_prefixes'])
    if config['defaults']['reply_llm'] and query:
        llm_matched = True
        llm_query = query

    reply = config['defaults']['help_reply']

    if query.upper() == "HELP":
        pass

    elif query.upper() == "SYN":
        reply = "ACK"

    elif query.upper() == "FOLLOW":
        reply = "I will now copy your position, telemetry, and range test updates. Send STOP to disable."
        interface.meshbot_state.nodedb_following[from_id] = True
        interface.meshbot_state.save_state('nodedb_following')

    elif query.upper() == "STOP":
        reply = "I will no longer copy on your position, telemetry, and range test updates. Send FOLLOW to resume."
        interface.meshbot_state.nodedb_following[from_id] = False
        interface.meshbot_state.save_state('nodedb_following')

    elif query == "🔔 Alert Bell Character! \x07":
        reply = 'Alert acknowledged at %s!' % format_date()
        if from_id in interface.meshbot_state.nodedb_positions:
            from_position = interface.meshbot_state.nodedb_positions[from_id]
            reply += ' Your last position: ' + format_position(from_position)

    elif query.upper() == "IDLE":
        context_messages = [{'from': from_name, 'text': make_idle_message(interface)}]
        reply = send_llm_request(interface, context_messages)

    elif query.upper() == "SPAM":
        pubsub.pub.sendMessage('meshbot.idle.expired', interface=interface)
        reply = 'Channel message queued.'

    elif query.upper() == "NEIGHBORS":
        state = interface.meshbot_state
        config = state.config
        now = time.time()
        timeout = config['general']['neighbor_timeout']
        neighbor_details = []

        active_neighbors = {
            nid: data for nid, data in state.neighbors.items()
            if now - data.get('timestamp', 0) < timeout
        }

        sorted_neighbors = sorted(active_neighbors.items(), key=lambda item: item[1].get('timestamp', 0), reverse=True)

        for neighbor_id, data in sorted_neighbors:
            if neighbor_id == interface.meshbot_state.local_id:
                continue
            neighbor_name, _, _ = find_node_name(state.nodedb_info, neighbor_id)
            if not neighbor_name:
                continue
            neighbor_details.append(neighbor_name + " (" + format_neighbor_details(data) + ")")

        if neighbor_details:
            reply = "Active neighbors: " + ", ".join(neighbor_details)
        else:
            reply = "No neighbors found."

    elif llm_matched and llm_query:
        context_messages = get_context_messages(interface, conversation_id)
        try:
            reply = send_llm_request(interface, context_messages)
        except:
            print("> llm request failed, responding with help text")

    channel_reply = reply
    if config['channel']['reply_prefix']:
        channel_reply = f"{from_short}, {reply}"

    # send reply and add bot's response to buffer
    if to_id == "^all" and not channel and not config['channel']['reply_direct']:
        mesh_send_channel(0, channel_reply, interface)
    elif to_id == "^all" and channel and not config['channel']['reply_direct']:
        mesh_send_channel(channel, channel_reply, interface)
    else:
        mesh_send_direct(from_id, reply, interface)

    add_message_to_buffer(interface, conversation_id, interface.meshbot_state.local_name, reply)

def mesh_receive(packet, interface):
    """process all incoming mesh packets and route to appropriate handlers"""
    save_packet_log(packet, interface)

    to_channel = packet.get("channel")

    from_id = packet["fromId"]
    to_id = packet["toId"]

    hops_away = -1
    if 'hopStart' in packet and 'hopLimit' in packet:
        hops_away = packet['hopStart'] - packet['hopLimit']

    if hops_away >= 0:
        update = interface.meshbot_state.neighbors.get(from_id, {})
        update['timestamp'] = time.time()
        update['hops_away'] = hops_away
        if 'rxSnr' in packet:
            update['snr'] = packet['rxSnr']
        interface.meshbot_state.neighbors[from_id] = update
        interface.meshbot_state.save_state('neighbors')

    from_short, from_long, from_name = find_node_name(interface.meshbot_state.nodedb_info, from_id)
    to_short, to_long, to_name = find_node_name(interface.meshbot_state.nodedb_info, to_id)

    headers = format_headers(packet, interface)

    #print("> received packet with keys:", packet.keys())
    if "decoded" in packet:
        decoded = packet["decoded"]
        portnum = decoded["portnum"]

        #print("> received message on port", portnum, "with keys:", decoded.keys())
        if portnum == "NODEINFO_APP":
            node_user = decoded["user"]
            node_names = format_node_name(node_user)
            print(f"> received nodeinfo {headers}: {node_names}")
            interface.meshbot_state.nodedb_info[from_id] = filtered_mesh_dict(node_user)
            interface.meshbot_state.save_state('nodedb_info')
            mesh_send_if_subscribed(from_id, "Copy your nodeinfo update at " + format_date() + ". Send STOP to disable.", interface)

        elif portnum == "TEXT_MESSAGE_APP":
            text = decoded["text"]
            print(f"> received text message {headers}: {text}")
            save_message_history(packet, text, interface)
            handle_text_message(to_channel, from_id, from_short, from_name, to_id, to_short, to_name, text, interface)

        elif portnum == "POSITION_APP":
            position = decoded["position"]
            if 'latitude' in position and 'longitude' in position:
                text_position = format_position(position)
                print(f"> received position {headers}: {text_position}")
                interface.meshbot_state.nodedb_positions[from_id] = filtered_mesh_dict(position)
                interface.meshbot_state.save_state('nodedb_positions')
                if from_id == interface.meshbot_state.local_id:
                    interface.meshbot_state.local_position = position
                    synchronize_location(interface)
                    print("> updated local node location:", interface.meshbot_state.local_location)
                else:
                    mesh_send_if_subscribed(from_id, "Copy your position update (" + text_position + ") at " + format_date() + ". Send STOP to disable.", interface)

            else:
                print(f"> received unsupported position {headers}:", position.keys())

        elif portnum == "TELEMETRY_APP":
            telemetry = decoded["telemetry"]
            timestamp = telemetry["time"]
            if "deviceMetrics" in telemetry:
                metrics = telemetry["deviceMetrics"]
                text_metrics = format_device_metrics(metrics)
                print(f"> received device metrics {headers}: {text_metrics}")
            elif "localStats" in telemetry:
                stats = telemetry["localStats"]
                text_stats = format_local_stats(stats)
                #print(f"> received local stats {headers}: {text_stats}")
            elif "environmentMetrics" in telemetry:
                env = telemetry["environmentMetrics"]
                text_env = format_env_metrics(env)
                #print(f"> received environmental metrics {headers}: {text_env}")
                if from_id == interface.meshbot_state.local_id:
                    #print("> updated local environmental metrics:", text_env)
                    interface.meshbot_state.local_metrics = text_env
            else:
                print(f"> received unsupported telemetry {headers}:", telemetry.keys())
            mesh_send_if_subscribed(from_id, "Copy your telemetry update at " + format_date() + ". Send STOP to disable.", interface)

        elif portnum == "ROUTING_APP":
            routing = decoded["routing"]
            #print(f"> received routing message {headers}:", filtered_mesh_dict(routing))

        elif portnum == "TRACEROUTE_APP":
            traceroute = decoded["traceroute"]
            print(f"> received traceroute {headers}:", filtered_mesh_dict(traceroute))

        elif portnum == "RANGE_TEST_APP":
            text = decoded["text"]
            print(f"> received rangetest {headers}: {text}")
            mesh_send_if_subscribed(from_id, "Copy your range test (" + text + ") at " + format_date() + ". Send STOP to disable.", interface)

        elif portnum == "NEIGHBORINFO_APP":
            neighborinfo = decoded["neighborinfo"]
            print(f"> received neighborinfo {headers}:", format_neighborinfo(neighborinfo))

        else:
            print(f"> received unsupported portnum {portnum} {headers}:", decoded.keys())
    else:
        #print(f"> received encrypted packet {headers}")
        pass

def mesh_connection_established(interface: Any) -> None:
    """handle mesh connection events"""
    print("> connection established")

def setup_interface(config: Dict[str, Any], state: "BotState") -> Any:
    """configure interface with bot settings and restore state"""
    interface = create_interface(config)
    interface.meshbot_state = state

    iface_type = config['interface']['type']
    if iface_type == "tcp":
        timeout = config['interface'].get('tcp_timeout', None)
        if timeout is not None:
            print(f"> setting tcp timeout to {timeout}s")
            interface.socket.settimeout(timeout)

    # wait for interface to be ready
    retries = 30
    mni = None
    while retries > 0 and mni is None:
        try:
            mni = interface.getMyNodeInfo()
            if mni is not None:
                break
        except AttributeError:
            pass
        time.sleep(1)
        retries -= 1

    if mni is None:
        raise Exception("failed to get node info from interface after waiting")

    state.local_user = mni['user']
    state.local_id = mni['user']['id']
    state.local_short = mni['user']['shortName']
    state.local_long = mni['user']['longName']
    state.local_name = format_node_name(mni['user'])
    state.name_prefixes = [
        state.local_short + ", ",
        state.local_short + ": ",
        state.local_short + " ",
        state.local_long + ", ",
        state.local_long + ": ",
        state.local_long + " ",
        "/", "@",
    ]
    state.local_location = config['general']['default_location']
    state.local_metrics = config['general']['default_metrics']
    state.neighbors = {}

    state.load_all()

    state.local_position = state.nodedb_positions.get(state.local_id, None)
    if state.local_position:
        synchronize_location(interface)

    state.nodedb_info[state.local_id] = filtered_mesh_dict(state.local_user)

    return interface

def mesh_client_proxy_message(message, interface):
    """handle mqtt proxy messages"""
    print("> received proxy message:", message)

def filtered_mesh_dict(obj):
    """recursively filter out non-JSON-serializable fields from packet data"""
    if isinstance(obj, dict):
        return {k: filtered_mesh_dict(v) for k, v in obj.items() if k not in ('raw', 'payload')}
    elif isinstance(obj, list):
        return [filtered_mesh_dict(item) for item in obj]
    else:
        # For primitive types, try to serialize to check if it's JSON-safe
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

# FIXME: make this functional, pass required elements of packet instead of packet
def save_message_history(packet, text, interface):
    """log text messages to persistent history file"""
    config = interface.meshbot_state.config
    data_dir = config['paths']['data_dir']
    message_history_file = config['paths'].get('message_history', 'message_history.jsonl')
    path = os.path.join(data_dir, message_history_file)

    message_record = {
        "timestamp": time.time(),
        "from": packet["from"],
        "to": packet["to"],
        "text": text,
        "channel": packet.get("channel", None),
        'from_id': packet.get('fromId', None),
        'to_id': packet.get('toId', None),
        "packet_id": packet.get("id", None)
    }
    with open(path, 'a') as f:
        json.dump(message_record, f)
        f.write('\n')

def save_packet_log(packet, interface):
    """save every received packet to the packet log"""
    config = interface.meshbot_state.config
    data_dir = config['paths']['data_dir']
    packet_log_file = config['paths'].get('packet_log', 'packet_log.jsonl')
    path = os.path.join(data_dir, packet_log_file)

    packet_record = {
        "timestamp": time.time(),
        "packet": filtered_mesh_dict(packet)
    }
    with open(path, 'a') as f:
        json.dump(packet_record, f)
        f.write('\n')

def is_idle_hours(interface):
    """check if current time is within configured idle hours"""
    config = interface.meshbot_state.config
    now = datetime.datetime.now()
    current_hour = now.hour
    start_hour = config['channel']['idle_start_hour']
    end_hour = config['channel']['idle_end_hour']
    return start_hour <= current_hour < end_hour

def make_idle_message(interface):
    config = interface.meshbot_state.config
    return '%s, %s.' % (random.choice(config['llm']['idle_prompts']), random.choice(config['llm']['idle_prompt_styles']))

def send_idle_message(interface):
    if not is_idle_hours(interface):
        return
    state = interface.meshbot_state
    state.idle_context_messages.append({'from': 'khimaros (KHMR)', 'text': make_idle_message(interface)})
    message = send_llm_request(interface, state.idle_context_messages)
    mesh_send_channel(0, message, interface)
    state.idle_context_messages.append({'from': state.local_name, 'text': message})

def main() -> None:
    """initialize mesh interface and run message processing loop"""
    config = load_config()
    state = BotState(config)

    if not os.path.exists(config['paths']['data_dir']):
        os.makedirs(config['paths']['data_dir'])

    connection_lost = threading.Event()
    def on_connection_lost(interface: Any):
        """handle mesh disconnection events"""
        if not connection_lost.is_set():
            print("> connection lost, attempting reconnection...")
            connection_lost.set()

    pubsub.pub.subscribe(mesh_connection_established, "meshtastic.connection.established")
    pubsub.pub.subscribe(on_connection_lost, "meshtastic.connection.lost")
    pubsub.pub.subscribe(mesh_client_proxy_message, "meshtastic.mqttclientproxymessage")
    pubsub.pub.subscribe(mesh_receive, "meshtastic.receive")
    pubsub.pub.subscribe(send_idle_message, 'meshbot.idle.expired')

    while True:
        interface = None
        try:
            connection_lost.clear()
            interface = setup_interface(config, state)
            print(f"> received identification for local node: {interface.meshbot_state.local_name} [{interface.meshbot_state.local_id}]")
            print("> local location initialized as:", interface.meshbot_state.local_location)

            while not connection_lost.is_set():
                got_event = connection_lost.wait(timeout=config['channel']['idle_frequency'])
                if not got_event:
                    pubsub.pub.sendMessage('meshbot.idle.expired', interface=interface)

        except KeyboardInterrupt:
            print()
            print("> keyboard interrupt received, saving and exiting...")
            if interface:
                state.save_all()
                interface.close()
            break

        except Exception as e:
            print(f'> received unhandled exception: {e}')
            if interface:
                print("> saving state before reconnecting...")
                state.save_all()
                interface.close()
                time.sleep(1)

            print('> attempting to reconnect in 10 seconds...')
            time.sleep(10)

if __name__ == "__main__":
    main()
