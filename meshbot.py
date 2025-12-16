#!/usr/bin/env python3

import datetime
import json
import os
import pubsub
import random
import signal
import time
import threading
from typing import Dict, Any, List, Optional, Union, Tuple

import meshtastic
import meshtastic.ble_interface
import meshtastic.serial_interface
import meshtastic.tcp_interface
import openai
import reverse_geocode
import tomli


meshbot_state = None


class BotState:
    """a container for all the bot's runtime state"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.lock = threading.Lock()
        self.loaded = False
        self.state_keys = [
            "nodedb_info",
            "nodedb_positions",
            "nodedb_following",
            "message_buffers",
            "rate_limits",
            "idle_context_messages",
            "neighbors",
            "nodedb_traceroutes",
        ]
        for key in self.state_keys:
            if key == "idle_context_messages":
                setattr(self, key, [])
            else:
                setattr(self, key, {})

    def save_state(self, key: str) -> None:
        """save a single state attribute to a json file"""
        if not hasattr(self, key):
            return
        with self.lock:
            data = getattr(self, key)
            path = os.path.join(self.config["paths"]["data_dir"], f"{key}.json")
            with open(path, "w") as f:
                json.dump(data, f)
            # print(f"> saved {len(data)} records to {path}")

    def load_state(self, key: str) -> None:
        """load a single state attribute from a json file"""
        with self.lock:
            path = os.path.join(self.config["paths"]["data_dir"], f"{key}.json")
            if os.path.exists(path):
                with open(path, "r") as f:
                    data = json.load(f)
                    if key == "idle_context_messages" and isinstance(data, dict):
                        data = []
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
        self.loaded = True


def interruptible_wait(shutdown_event: threading.Event, timeout: float) -> bool:
    """wait for timeout seconds while checking shutdown event
    returns True if shutdown was requested, False if timeout elapsed"""
    return shutdown_event.wait(timeout=timeout)


def wait_for_any_event(*events: threading.Event, timeout: float) -> bool:
    """wait for any of the given events to be set, or timeout
    returns True if any event was set, False if timeout elapsed"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if any(event.is_set() for event in events):
            return True
        time.sleep(0.1)
    return False


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


def load_config(
    config_files: List[str] = ["config.default.toml", "config.toml"]
) -> Dict[str, Any]:
    """load and merge multiple config files in order"""
    config: Dict[str, Any] = {}
    for file_path in config_files:
        file_config = load_config_file(file_path)
        config = merge_config(config, file_config)
    return config


def disconnect_ble_device(address: str) -> bool:
    """disconnect a BLE device via bluetoothctl
    returns True if disconnect was successful, False otherwise"""
    if not address:
        return False

    import subprocess
    try:
        result = subprocess.run(
            ["bluetoothctl", "disconnect", address],
            capture_output=True,
            text=True,
            timeout=5
        )
        if "Successful" in result.stdout or result.returncode == 0:
            print(f"> disconnected BLE device {address} via bluetoothctl")
            return True
        else:
            # Device may not be connected
            print(f"> BLE device {address} not connected: {result.stdout.strip() or result.stderr.strip()}")
            return False
    except FileNotFoundError:
        print("> bluetoothctl not found, skipping BlueZ disconnect")
        return False
    except subprocess.TimeoutExpired:
        print("> bluetoothctl disconnect timed out")
        return False
    except Exception as e:
        print(f"> failed to disconnect BLE via bluetoothctl: {e}")
        return False


def create_interface(config: Dict[str, Any]) -> Any:
    """create meshtastic interface based on config"""
    iface_type = config["interface"]["type"]
    if iface_type == "serial":
        device_path = config["interface"]["device"]
        print(f"> creating serial interface on {device_path}")
        return meshtastic.serial_interface.SerialInterface(devPath=device_path)
    elif iface_type == "tcp":
        hostname = config["interface"]["tcp_hostname"]
        port = config["interface"]["tcp_port"]
        print(f"> creating tcp interface on {hostname}:{port}")
        return meshtastic.tcp_interface.TCPInterface(hostname=hostname, portNumber=port)
    elif iface_type == "ble":
        ble_address = config["interface"].get("ble_address", None)
        # Disconnect any existing BlueZ connection before connecting
        if ble_address:
            disconnect_ble_device(ble_address)
            print(f"> creating ble interface on {ble_address}")
        else:
            print("> creating ble interface (scanning for devices)")
        return meshtastic.ble_interface.BLEInterface(address=ble_address if ble_address else None)
    else:
        raise Exception(f"unsupported interface type: {iface_type}")


def format_headers(packet, interface) -> str:
    from_id = packet["fromId"]
    to_id = packet["toId"]

    hops_str = ""
    if "hopStart" in packet and "hopLimit" in packet:
        hops_away = packet["hopStart"] - packet["hopLimit"]
        hops_str = "[" + str(hops_away) + " hops] "

    from_short, from_long, from_name = find_node_name(
        meshbot_state.nodedb_info, from_id
    )
    to_short, to_long, to_name = find_node_name(
        meshbot_state.nodedb_info, to_id
    )
    channel = packet.get("channel", 0)

    return hops_str + f"from {from_name} [{from_id}] to {to_name} [{to_id}] on channel {channel}"


def format_device_metrics(metrics: Dict[str, Any]) -> str:
    """convert device metrics to human-readable format"""
    channel_util = metrics.get("channelUtilization", "N/A")
    air_util_tx = metrics.get("airUtilTx", "N/A")
    return "ChUtil: %s, airUtilTx: %s" % (channel_util, air_util_tx)


def format_local_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    """convert local statistics to human-readable format"""
    return stats


def format_env_metrics(env: Dict[str, Any]) -> Dict[str, Any]:
    """convert environmental metrics to human-readable format"""
    return env


def format_date() -> str:
    return str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))


def format_position(position: Dict[str, Any]) -> str:
    """convert position data to coordinate string for display"""
    lat = position["latitude"]
    lon = position["longitude"]
    position_text = f"{lat},{lon}"
    return position_text


def format_neighborinfo(neighborinfo: Dict[str, Any]) -> str:
    """convert neighborinfo data to a formatted string for display"""
    if "neighbors" in neighborinfo:
        return str(neighborinfo["neighbors"])
    else:
        # Return all available keys if neighbors key is missing
        return f"(no neighbors key, available: {list(neighborinfo.keys())})"


def format_neighbor_details(data: Dict[str, Any]) -> str:
    """convert neighbor data to a formatted string for display"""
    hops = data.get("hops_away", "?")
    #snr = data.get("snr")
    #snr_str = f"{snr:.2f}" if snr is not None else "N/A"
    #return f"{hops} hops, SNR {snr_str}"
    return f"{hops} hops"


def format_node_name(node_user: Dict[str, Any]) -> str:
    """create display name combining long and short names"""
    node_longname = node_user["longName"]
    node_shortname = node_user["shortName"]
    node_name = f"{node_longname} ({node_shortname})"
    return node_name


def synchronize_location(interface: Any) -> None:
    position = meshbot_state.local_position
    rev = reverse_geocode.get((position["latitude"], position["longitude"]))
    meshbot_state.local_location = "%s, %s, %s, %s" % (
        rev["city"],
        rev["county"],
        rev["state"],
        rev["country"],
    )


def rate_limit_wait_time(interface: Any, target_key: str) -> int:
    """calculate seconds to wait before allowing next message from target"""
    now = time.time()
    state = meshbot_state
    if target_key not in state.rate_limits:
        state.rate_limits[target_key] = []

    config = state.config
    window = config["rate_limiting"]["window"]
    max_msgs = config["rate_limiting"]["max"]

    # remove old entries outside the window
    state.rate_limits[target_key] = [
        t for t in state.rate_limits[target_key] if now - t < window
    ]

    if len(state.rate_limits[target_key]) >= max_msgs:
        oldest_time = min(state.rate_limits[target_key])
        return int(window - (now - oldest_time))

    state.rate_limits[target_key].append(now)
    return 0


def limit_message_size(text: str, interface: Any) -> str:
    """truncate messages to fit mesh network size constraints"""
    max_length = meshbot_state.config["message"]["max_length"]
    if len(text) <= max_length:
        return text
    return text[: max_length - 5] + "[...]"


def find_node_name(nodedb_info: Dict[str, Any], node_id: str) -> Tuple[str, str, str]:
    """lookup node names from nodedb or fall back to id"""
    if node_id in nodedb_info:
        node_user = nodedb_info[node_id]
        short_name = node_user["shortName"]
        long_name = node_user["longName"]
        display_name = format_node_name(node_user)
        return short_name, long_name, display_name
    return "", "", node_id


def strip_local_names_from_response(content: str, interface: Any) -> str:
    """strip long/short/combined local names from beginning of llm response"""
    state = meshbot_state
    response_prefixes = [
        prefix for prefix in state.name_prefixes if prefix not in ["/", "@"]
    ]

    while True:
        stripped_any = False
        for prefix in response_prefixes:
            if content.startswith(prefix):
                content = content[len(prefix) :].lstrip()
                stripped_any = True
                break
        if not stripped_any:
            break

    return content


def get_conversation_id(
    channel: Optional[int], from_id: Optional[str], to_id: Optional[str]
) -> str:
    """generate unique identifier for conversation context"""
    if channel is not None:
        return f"channel_{channel}"
    else:
        # sort ids to ensure consistent conversation id for direct messages
        ids = sorted([from_id or "", to_id or ""])
        return f"dm_{ids[0]}_{ids[1]}"


def add_message_to_buffer(
    interface: Any, conversation_id: str, from_name: str, text: str
) -> None:
    """maintain rolling conversation history for llm context"""
    state = meshbot_state
    if conversation_id not in state.message_buffers:
        state.message_buffers[conversation_id] = []

    message = {"from": from_name, "text": text}
    state.message_buffers[conversation_id].append(message)

    buffer_size = state.config["llm"]["context_buffer_size"]
    if len(state.message_buffers[conversation_id]) > buffer_size:
        state.message_buffers[conversation_id].pop(0)


def get_context_messages(interface, conversation_id):
    """retrieve conversation history for llm context"""
    return meshbot_state.message_buffers.get(conversation_id, [])


def send_llm_request(
    interface,
    context_messages=None,
    api_base=None,
    api_key=None,
    model=None,
    max_tokens=None,
    max_reasoning_tokens=None,
):
    """send conversation context to llm and return response"""
    state = meshbot_state
    config = state.config
    if api_base is None:
        api_base = config["llm"]["api_base"]
    if api_key is None:
        api_key = config["llm"]["api_key"]
    if model is None:
        model = config["llm"]["default_model"]
    if max_tokens is None:
        max_tokens = config["llm"].get("default_max_tokens", None)
    if max_reasoning_tokens is None:
        max_reasoning_tokens = config["llm"].get("default_max_reasoning_tokens", None)

    client = openai.OpenAI(api_key=api_key, base_url=api_base)
    system_message = config["llm"]["system_prompt"].format(
        **{
            "longname": state.local_long,
            "shortname": state.local_short,
            "location": state.local_location,
            "metrics": state.local_metrics,
            "date": format_date(),
        }
    )
    messages = [{"role": "system", "content": system_message}]
    context_lines = []
    for msg in context_messages:
        context_lines.append(f"{msg['from']}: {msg['text']}")
    context_text = "\n".join(context_lines)
    messages.append({"role": "user", "content": context_text})

    request_params = {"model": model, "messages": messages}

    # If max_reasoning_tokens is set, use max_completion_tokens (answer + reasoning budget)
    # Otherwise just use max_tokens (total output limit)
    if max_reasoning_tokens is not None and max_tokens is not None:
        max_completion_tokens = max_tokens + max_reasoning_tokens
        request_params["max_completion_tokens"] = max_completion_tokens
        print(f"> using max_completion_tokens={max_completion_tokens} (answer={max_tokens} + reasoning={max_reasoning_tokens})")
    elif max_tokens is not None:
        request_params["max_tokens"] = max_tokens
        print(f"> using max_tokens={max_tokens} (total output)")

    enable_streaming = config["llm"].get("enable_streaming", False)
    request_params["stream"] = enable_streaming

    print(f"> beginning llm request (streaming={enable_streaming}): {messages}")
    start_time = time.time()

    if enable_streaming:
        # Handle streaming response
        stream = client.chat.completions.create(**request_params)
        content = ""
        for chunk in stream:
            if chunk.choices and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta

                # Skip reasoning/thinking content
                # Some models use delta.reasoning_content or similar fields
                if hasattr(delta, 'reasoning_content') or hasattr(delta, 'thinking_content'):
                    continue

                # Only process regular content, skip if role indicates reasoning
                if hasattr(delta, 'role') and delta.role in ['reasoning', 'thinking']:
                    continue

                # Extract regular content
                if hasattr(delta, 'content') and delta.content:
                    content += delta.content
                    #print(delta.content, end="", flush=True)

        #print()  # newline after streaming
        duration = time.time() - start_time
        content = content.strip()
    else:
        # Handle non-streaming response
        response = client.chat.completions.create(**request_params)
        duration = time.time() - start_time
        content = response.choices[0].message.content.strip()

    content = strip_local_names_from_response(content, interface)
    print(f"> completed llm request in {duration:.2f}s: {content}")
    return content


def mesh_send_direct(to_id, text, interface, reply_id=None):
    """send direct message to specific node"""
    text = limit_message_size(text, interface)
    print(f"> sending text direct to user {to_id}: {text}")
    kwargs = {"wantAck": True}
    if reply_id is not None:
        kwargs["replyId"] = reply_id
    interface.sendText(text, to_id, **kwargs)


def mesh_send_if_subscribed(to_id, text, interface):
    if meshbot_state.nodedb_following.get(to_id, False) == True:
        mesh_send_direct(to_id, text, interface)


def request_traceroute(node_id: str, interface: Any) -> bool:
    """request a traceroute to a specific node, returns True if sent"""
    state = meshbot_state
    config = state.config

    if not config.get("traceroute", {}).get("enabled", False):
        return False

    # Skip None or invalid node IDs
    if not node_id or node_id == "None":
        return False

    # Convert node_id string to numeric destination
    # node_id format is like "!abcd1234" - we need the numeric version
    try:
        if node_id.startswith("!"):
            dest = int(node_id[1:], 16)
        else:
            dest = int(node_id, 16)
    except (ValueError, AttributeError):
        #print(f"> traceroute: invalid node_id format: {node_id}")
        return False

    _, _, node_name = find_node_name(state.nodedb_info, node_id)

    # Use the channel we last heard this node on
    neighbor_data = state.neighbors.get(node_id, {})
    channel = neighbor_data.get("channel", 0)
    print(f"> sending traceroute to {node_name} [{node_id}] on channel {channel}")

    try:
        interface.sendTraceRoute(dest, hopLimit=7, channelIndex=channel)

        # Initialize traceroute record if it doesn't exist
        if node_id not in state.nodedb_traceroutes:
            state.nodedb_traceroutes[node_id] = {}
        state.nodedb_traceroutes[node_id]["last_request"] = time.time()
        state.save_state("nodedb_traceroutes")
        return True
    except Exception as e:
        print(f"> traceroute request failed: {e}")
        return False


def get_stale_nodes_for_traceroute(interface: Any) -> List[str]:
    """get list of node IDs that need tracerouting (stale or never traced)"""
    state = meshbot_state
    config = state.config

    if not config.get("traceroute", {}).get("enabled", False):
        return []

    now = time.time()
    stale_timeout = config["traceroute"].get("stale_timeout", 1800)
    neighbor_timeout = config["general"].get("neighbor_timeout", 14400)
    stale_nodes = []

    # Check all known neighbors
    for node_id, neighbor_data in state.neighbors.items():
        # Skip invalid node IDs
        if not node_id or node_id == "None":
            continue

        # Skip our own node
        if node_id == state.local_id:
            continue

        # Skip nodes that haven't been seen recently
        last_seen = neighbor_data.get("timestamp", 0)
        if now - last_seen > neighbor_timeout:
            continue

        # Check if we have a recent traceroute for this node
        traceroute_data = state.nodedb_traceroutes.get(node_id, {})
        last_completed = traceroute_data.get("last_completed", 0)

        # If never traced or traceroute is stale, add to list
        if last_completed == 0 or (now - last_completed) > stale_timeout:
            stale_nodes.append(node_id)

    return stale_nodes


def check_traceroute_timeout(interface: Any) -> None:
    """check if pending traceroute has timed out and mark as completed"""
    state = meshbot_state
    config = state.config
    request_timeout = config["traceroute"].get("request_timeout", 45)
    now = time.time()

    pending_node = getattr(state, "traceroute_pending_node", None)
    pending_time = getattr(state, "traceroute_pending_time", 0)

    if pending_node and (now - pending_time) >= request_timeout:
        _, _, node_name = find_node_name(state.nodedb_info, pending_node)
        print(f"> traceroute to {node_name} [{pending_node}] timed out after {request_timeout}s")

        # Mark as completed (failed) so we move on
        if pending_node not in state.nodedb_traceroutes:
            state.nodedb_traceroutes[pending_node] = {}
        state.nodedb_traceroutes[pending_node]["last_completed"] = now
        state.nodedb_traceroutes[pending_node]["last_timeout"] = now
        state.save_state("nodedb_traceroutes")

        # Clear pending state and set last_traceroute_completed for rate limiting
        state.traceroute_pending_node = None
        state.traceroute_pending_time = 0
        state.last_traceroute_completed = now


def process_traceroute_queue(interface: Any) -> None:
    """check for stale nodes and send traceroute if rate limit allows"""
    state = meshbot_state
    config = state.config

    if not config.get("traceroute", {}).get("enabled", False):
        return

    # First check if pending traceroute has timed out
    check_traceroute_timeout(interface)

    # Don't send new request if one is pending
    if getattr(state, "traceroute_pending_node", None):
        return

    # Check rate limit from last completed traceroute
    now = time.time()
    rate_limit = config["traceroute"].get("rate_limit", 60)
    last_completed = getattr(state, "last_traceroute_completed", 0)
    time_since_last = now - last_completed
    if time_since_last < rate_limit:
        # Rate limited - log occasionally
        last_rate_log = getattr(state, "traceroute_rate_log_time", 0)
        if now - last_rate_log >= 30:
            state.traceroute_rate_log_time = now
            wait_time = int(rate_limit - time_since_last)
            print(f"> traceroute: rate limited, waiting {wait_time}s before next request")
        return

    stale_nodes = get_stale_nodes_for_traceroute(interface)
    if not stale_nodes:
        return

    # Send traceroute to first stale node
    # Set pending state BEFORE sending - response callback may fire during sendTraceRoute
    node_id = stale_nodes[0]
    state.traceroute_pending_node = node_id
    state.traceroute_pending_time = now
    if not request_traceroute(node_id, interface):
        # Request failed, clear pending state
        state.traceroute_pending_node = None
        state.traceroute_pending_time = 0


def process_traceroute_queue_async(interface: Any) -> None:
    """start traceroute processing in background thread if appropriate

    Handles the case where sendTraceRoute blocks indefinitely by:
    1. Checking timeout in main thread first (clears pending_node)
    2. If no pending_node and old thread is stuck, abandon it and start new one
    3. Only one active request at a time (tracked by pending_node, not thread)
    """
    state = meshbot_state
    config = state.config

    if not config.get("traceroute", {}).get("enabled", False):
        return

    # Don't process traceroutes if connection is lost
    connection_lost = getattr(state, "connection_lost", None)
    if connection_lost and connection_lost.is_set():
        return

    # Always check for timeouts first (in main thread, not blocking)
    check_traceroute_timeout(interface)

    # Get current state for logging
    pending_node = getattr(state, "traceroute_pending_node", None)
    traceroute_thread = getattr(state, "traceroute_thread", None)
    thread_alive = traceroute_thread and traceroute_thread.is_alive()

    # Get queue info
    stale_nodes = get_stale_nodes_for_traceroute(interface)
    stale_count = len(stale_nodes)

    # Check rate limit status
    now = time.time()
    rate_limit = config["traceroute"].get("rate_limit", 60)
    last_completed = getattr(state, "last_traceroute_completed", 0)
    time_since_last = int(now - last_completed) if last_completed > 0 else -1
    rate_limit_ok = (now - last_completed) >= rate_limit or last_completed == 0

    # Log status every 10 seconds (approximately)
    last_log_time = getattr(state, "traceroute_last_log_time", 0)
    if now - last_log_time >= 10:
        state.traceroute_last_log_time = now
        if pending_node:
            pending_time = getattr(state, "traceroute_pending_time", 0)
            pending_duration = int(now - pending_time)
            _, _, pending_name = find_node_name(state.nodedb_info, pending_node)
            #print(f"> traceroute status: pending={pending_name} ({pending_duration}s), queue={stale_count}, thread_alive={thread_alive}, rate_limit_ok={rate_limit_ok} (last={time_since_last}s)")
        elif stale_count > 0:
            #print(f"> traceroute status: pending=none, queue={stale_count}, thread_alive={thread_alive}, rate_limit_ok={rate_limit_ok} (last={time_since_last}s)")
            pass

    # If there's a pending request, don't start a new thread
    # (even if old thread is stuck - we wait for timeout to clear it)
    if pending_node:
        return

    # Check if a traceroute thread is already running
    # If it's running but no pending_node, it means it timed out - abandon it
    if thread_alive:
        # Old thread is stuck/abandoned - track it for cleanup and start a new one
        print(f"> traceroute: abandoning stuck thread, starting new one")
        if hasattr(state, "abandoned_threads"):
            state.abandoned_threads.append(traceroute_thread)

    # Start new daemon thread for traceroute processing
    thread = threading.Thread(target=process_traceroute_queue, args=(interface,), name="traceroute", daemon=True)
    thread.start()
    state.traceroute_thread = thread


def mesh_send_channel(channel, text, interface, reply_id=None):
    """broadcast message to channel"""
    text = limit_message_size(text, interface)
    print(f"> sending text to channel {channel}: {text}")
    kwargs = {"wantAck": True, "channelIndex": channel}
    if reply_id is not None:
        kwargs["replyId"] = reply_id
    interface.sendText(text, "^all", **kwargs)


def prefix_matched_query(text, prefixes):
    """check if text starts with any command prefix"""
    for pfx in prefixes:
        if text.upper().startswith(pfx.upper()):
            return True, text[len(pfx) :]
    return False, text


def handle_text_message(
    channel, from_id, from_short, from_name, to_id, to_short, to_name, text, interface, packet_id=None, from_num=None
):
    """process incoming text messages and generate appropriate replies"""
    conversation_id = get_conversation_id(channel, from_id, to_id)

    wait_time = rate_limit_wait_time(interface, f"message_{from_id}")
    if wait_time > 0:
        print(
            f"> rate limited, skipping message from {from_name} [{from_id}], (wait {wait_time}s)"
        )
        return

    matched, query = prefix_matched_query(text, meshbot_state.name_prefixes)
    add_message_to_buffer(interface, conversation_id, from_name, query)

    config = meshbot_state.config
    unsolicited = random.random()
    if to_id == "^all" and not matched:
        # Use channel-specific ratio if available, otherwise use global ratio
        ratio_map = config["channel"].get("unsolicited_ratio_map", {})
        ratio = ratio_map.get(str(channel), config["channel"]["unsolicited_ratio"])
        if unsolicited > ratio:
            print(
                f"> skipping unprefixed channel message from {from_name} [{from_id}] on channel {channel}: {unsolicited} > {ratio}"
            )
            return
    print(f"> processing query from {from_name} [{from_id}]: {query}")

    llm_matched, llm_query = prefix_matched_query(
        query, config["llm"]["command_prefixes"]
    )
    if config["defaults"]["reply_llm"] and query:
        llm_matched = True
        llm_query = query

    reply = config["defaults"]["help_reply"]

    if query.upper() == "HELP":
        pass

    elif query.upper() == "SYN":
        reply = "ACK"

    elif query.upper() == "FOLLOW":
        reply = "I will now copy your position, telemetry, and range test updates. Send STOP to disable."
        meshbot_state.nodedb_following[from_id] = True
        meshbot_state.save_state("nodedb_following")

    elif query.upper() == "STOP":
        reply = "I will no longer copy on your position, telemetry, and range test updates. Send FOLLOW to resume."
        meshbot_state.nodedb_following[from_id] = False
        meshbot_state.save_state("nodedb_following")

    elif query == "🔔 Alert Bell Character! \x07":
        reply = "Alert acknowledged at %s!" % format_date()
        if from_id in meshbot_state.nodedb_positions:
            from_position = meshbot_state.nodedb_positions[from_id]
            reply += " Your last position: " + format_position(from_position)

    elif query.upper() == "IDLE":
        context_messages = [{"from": from_name, "text": make_idle_message(interface)}]
        reply = send_llm_request(interface, context_messages)

    elif query.upper() == "SPAM":
        pubsub.pub.sendMessage("meshbot.idle.expired", interface=interface)
        reply = "Channel message queued."

    elif query.upper() == "NEIGHBORS":
        state = meshbot_state
        config = state.config
        now = time.time()
        timeout = config["general"]["neighbor_timeout"]
        neighbor_details = []

        active_neighbors = {
            nid: data
            for nid, data in state.neighbors.items()
            if now - data.get("timestamp", 0) < timeout
        }

        sorted_neighbors = sorted(
            active_neighbors.items(),
            key=lambda item: item[1].get("timestamp", 0),
            reverse=True,
        )

        for neighbor_id, data in sorted_neighbors:
            if neighbor_id == meshbot_state.local_id:
                continue
            neighbor_name, _, _ = find_node_name(state.nodedb_info, neighbor_id)
            if not neighbor_name:
                continue
            neighbor_details.append(
                neighbor_name + " (" + format_neighbor_details(data) + ")"
            )

        if neighbor_details:
            reply = ", ".join(neighbor_details)
        else:
            reply = "No neighbors found."

    elif llm_matched and llm_query:
        context_messages = get_context_messages(interface, conversation_id)
        try:
            reply = send_llm_request(interface, context_messages)
        except:
            print("> llm request failed, responding with help text")

    channel_reply = reply
    if config["channel"]["reply_prepend_shortname"]:
        channel_reply = f"{from_short}, {reply}"

    # send reply and add bot's response to buffer
    # use numeric ID for DMs to maintain thread continuity
    reply_to = from_num if from_num is not None else from_id
    if to_id == "^all" and not channel and not config["channel"]["reply_direct"]:
        mesh_send_channel(0, channel_reply, interface, reply_id=packet_id)
    elif to_id == "^all" and channel and not config["channel"]["reply_direct"]:
        mesh_send_channel(channel, channel_reply, interface, reply_id=packet_id)
    else:
        mesh_send_direct(reply_to, reply, interface, reply_id=packet_id)

    add_message_to_buffer(
        interface, conversation_id, meshbot_state.local_name, reply
    )


def mesh_receive(packet, interface):
    """process all incoming mesh packets and route to appropriate handlers"""
    save_packet_log(packet, interface)

    # Update last packet time for health monitoring
    if meshbot_state is not None:
        meshbot_state.last_packet_time = time.time()

    # Skip processing if interface not fully initialized
    if meshbot_state is None or not hasattr(meshbot_state, "local_id"):
        print("> skipping packet, interface not fully initialized")
        return

    channel = packet.get("channel", 0)

    from_id = packet["fromId"]
    to_id = packet["toId"]

    hops_away = -1
    if "hopStart" in packet and "hopLimit" in packet:
        hops_away = packet["hopStart"] - packet["hopLimit"]

    if hops_away >= 0:
        update = meshbot_state.neighbors.get(from_id, {})
        update["timestamp"] = time.time()
        update["hops_away"] = hops_away
        update["channel"] = channel
        if "rxSnr" in packet:
            update["snr"] = packet["rxSnr"]
        meshbot_state.neighbors[from_id] = update
        meshbot_state.save_state("neighbors")

    from_short, from_long, from_name = find_node_name(
        meshbot_state.nodedb_info, from_id
    )
    to_short, to_long, to_name = find_node_name(
        meshbot_state.nodedb_info, to_id
    )

    headers = format_headers(packet, interface)

    # print("> received packet with keys:", packet.keys())
    if "decoded" in packet:
        decoded = packet["decoded"]
        portnum = decoded["portnum"]

        # print("> received message on port", portnum, "with keys:", decoded.keys())
        if portnum == "NODEINFO_APP":
            node_user = decoded["user"]
            node_names = format_node_name(node_user)
            print(f"> received nodeinfo {headers}: {node_names}")
            meshbot_state.nodedb_info[from_id] = filtered_mesh_dict(node_user)
            meshbot_state.save_state("nodedb_info")
            mesh_send_if_subscribed(
                from_id,
                "Copy your nodeinfo update at "
                + format_date()
                + ". Send STOP to disable.",
                interface,
            )

        elif portnum == "TEXT_MESSAGE_APP":
            text = decoded["text"]
            print(f"> received text message {headers}: {text}")
            save_message_history(packet, text, interface)
            handle_text_message(
                channel,
                from_id,
                from_short,
                from_name,
                to_id,
                to_short,
                to_name,
                text,
                interface,
                packet_id=packet.get("id"),
                from_num=packet.get("from"),
            )

        elif portnum == "POSITION_APP":
            position = decoded["position"]
            if "latitude" in position and "longitude" in position:
                text_position = format_position(position)
                print(f"> received position {headers}: {text_position}")
                meshbot_state.nodedb_positions[from_id] = filtered_mesh_dict(
                    position
                )
                meshbot_state.save_state("nodedb_positions")
                if from_id == meshbot_state.local_id:
                    meshbot_state.local_position = position
                    synchronize_location(interface)
                    print(
                        "> updated local node location:",
                        meshbot_state.local_location,
                    )
                else:
                    mesh_send_if_subscribed(
                        from_id,
                        "Copy your position update ("
                        + text_position
                        + ") at "
                        + format_date()
                        + ". Send STOP to disable.",
                        interface,
                    )

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
                # print(f"> received local stats {headers}: {text_stats}")
            elif "environmentMetrics" in telemetry:
                env = telemetry["environmentMetrics"]
                text_env = format_env_metrics(env)
                # print(f"> received environmental metrics {headers}: {text_env}")
                if from_id == meshbot_state.local_id:
                    # print("> updated local environmental metrics:", text_env)
                    meshbot_state.local_metrics = text_env
            else:
                print(f"> received unsupported telemetry {headers}:", telemetry.keys())
            mesh_send_if_subscribed(
                from_id,
                "Copy your telemetry update at "
                + format_date()
                + ". Send STOP to disable.",
                interface,
            )

        elif portnum == "ROUTING_APP":
            routing = decoded["routing"]
            # print(f"> received routing message {headers}:", filtered_mesh_dict(routing))

        elif portnum == "TRACEROUTE_APP":
            traceroute = decoded["traceroute"]
            traceroute_data = filtered_mesh_dict(traceroute)
            print(f"> received traceroute {headers}:", traceroute_data)

            # Save traceroute result to persistent state
            state = meshbot_state
            now = time.time()

            # Format route with node names for logging
            route_nodes = traceroute.get("route", [])
            route_names = []
            for node_num in route_nodes:
                node_id = f"!{node_num:08x}"
                _, _, name = find_node_name(state.nodedb_info, node_id)
                route_names.append(name)

            route_back_nodes = traceroute.get("routeBack", [])
            route_back_names = []
            for node_num in route_back_nodes:
                node_id = f"!{node_num:08x}"
                _, _, name = find_node_name(state.nodedb_info, node_id)
                route_back_names.append(name)

            route_str = " -> ".join(route_names) if route_names else "(direct)"
            route_back_str = " -> ".join(route_back_names) if route_back_names else "(direct)"
            print(f"> traceroute path to {from_id}: {route_str}")
            print(f"> traceroute path back from {from_id}: {route_back_str}")

            # Store in nodedb_traceroutes
            if from_id not in state.nodedb_traceroutes:
                state.nodedb_traceroutes[from_id] = {}
            state.nodedb_traceroutes[from_id]["last_response"] = now
            state.nodedb_traceroutes[from_id]["last_completed"] = now
            state.nodedb_traceroutes[from_id]["route"] = traceroute_data.get("route", [])
            state.nodedb_traceroutes[from_id]["route_names"] = route_names
            state.nodedb_traceroutes[from_id]["route_back"] = traceroute_data.get("routeBack", [])
            state.nodedb_traceroutes[from_id]["route_back_names"] = route_back_names
            state.nodedb_traceroutes[from_id]["snr_towards"] = traceroute_data.get("snrTowards", [])
            state.nodedb_traceroutes[from_id]["snr_back"] = traceroute_data.get("snrBack", [])
            state.save_state("nodedb_traceroutes")

            # Clear pending state and update rate limit timestamp
            pending_node = getattr(state, "traceroute_pending_node", None)
            if pending_node == from_id:
                state.traceroute_pending_node = None
                state.traceroute_pending_time = 0
                state.last_traceroute_completed = now

            # Save to traceroute log file
            save_traceroute_log(from_id, traceroute_data, route_names, route_back_names, interface)

        elif portnum == "RANGE_TEST_APP":
            text = decoded["text"]
            print(f"> received rangetest {headers}: {text}")
            mesh_send_if_subscribed(
                from_id,
                "Copy your range test ("
                + text
                + ") at "
                + format_date()
                + ". Send STOP to disable.",
                interface,
            )

        elif portnum == "NEIGHBORINFO_APP":
            neighborinfo = decoded["neighborinfo"]
            print(
                f"> received neighborinfo {headers}:", format_neighborinfo(neighborinfo)
            )

        else:
            print(
                f"> received unsupported portnum {portnum} {headers}:", decoded.keys()
            )
    else:
        # print(f"> received encrypted packet {headers}")
        pass


def mesh_connection_established(interface: Any) -> None:
    """handle mesh connection events"""
    print("> connection established")


def cleanup_abandoned_threads(state: "BotState") -> None:
    """try to join any abandoned threads that have finished"""
    if not hasattr(state, "abandoned_threads") or not state.abandoned_threads:
        return

    still_alive = []
    for thread in state.abandoned_threads:
        if thread.is_alive():
            # Try a brief join to see if it finishes
            thread.join(timeout=0.1)
            if thread.is_alive():
                still_alive.append(thread)
            else:
                print(f"> cleaned up finished thread: {thread.name}")
        else:
            print(f"> cleaned up finished thread: {thread.name}")

    state.abandoned_threads = still_alive


def check_connection_health(config: Dict[str, Any], state: "BotState") -> bool:
    """check if connection appears healthy based on packet activity
    returns True if healthy, False if stale"""
    health_timeout = config["interface"].get("health_check_timeout", 0)
    if health_timeout <= 0:
        return True

    last_packet = getattr(state, "last_packet_time", None)
    if last_packet is None:
        # No packets received yet - still healthy if we just connected
        return True

    elapsed = time.time() - last_packet
    if elapsed > health_timeout:
        print(f"> connection health check failed: no packets for {elapsed:.0f}s (timeout: {health_timeout}s)")
        return False

    return True


def close_interface(interface: Any, config: Dict[str, Any], shutdown_event: threading.Event) -> None:
    """close interface with BLE-specific handling for reliability"""
    iface_type = config["interface"]["type"]

    def do_close():
        try:
            interface.close()
        except Exception as e:
            print(f"> error in close: {e}")

    # Use a thread with timeout to avoid hanging
    close_thread = threading.Thread(target=do_close, daemon=True)
    close_thread.start()

    # Wait for close to complete (max 5 seconds for BLE, 10 for others)
    timeout = 5.0 if iface_type == "ble" else 10.0
    interval = 0.1
    elapsed = 0.0
    while close_thread.is_alive() and elapsed < timeout and not shutdown_event.is_set():
        close_thread.join(timeout=interval)
        elapsed += interval

    if close_thread.is_alive():
        if elapsed >= timeout:
            print(f"> interface close timed out after {timeout:.0f}s")
        # Thread is daemon, will be killed on process exit
    else:
        print("> interface closed")


def setup_interface(config: Dict[str, Any], state: "BotState", shutdown_event: threading.Event) -> Any:
    """configure interface with bot settings and restore state"""
    interface = create_interface(config)

    iface_type = config["interface"]["type"]
    if iface_type == "tcp":
        timeout = config["interface"].get("tcp_timeout", None)
        if timeout is not None:
            print(f"> setting tcp timeout to {timeout}s")
            interface.socket.settimeout(timeout)

    # wait for interface to be ready
    retries = 30
    mni = None
    while retries > 0 and mni is None and not shutdown_event.is_set():
        try:
            mni = interface.getMyNodeInfo()
            if mni is not None:
                break
        except AttributeError:
            pass
        if interruptible_wait(shutdown_event, 1.0):
            break
        retries -= 1

    if mni is None:
        if shutdown_event.is_set():
            raise Exception("shutdown requested during interface setup")
        raise Exception("failed to get node info from interface after waiting")

    state.local_user = mni["user"]
    state.local_id = mni["user"]["id"]
    state.local_short = mni["user"]["shortName"]
    state.local_long = mni["user"]["longName"]
    state.local_name = format_node_name(mni["user"])
    state.name_prefixes = [
        state.local_short + ", ",
        state.local_short + ": ",
        state.local_short + " ",
        state.local_long + ", ",
        state.local_long + ": ",
        state.local_long + " ",
        state.local_name + ", ",
        state.local_name + ": ",
        state.local_name + " ",
        "/",
        "@",
    ]
    state.local_location = config["general"]["default_location"]
    state.local_metrics = config["general"]["default_metrics"]
    state.neighbors = {}
    state.last_traceroute_request = 0

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
        return {
            k: filtered_mesh_dict(v)
            for k, v in obj.items()
            if k not in ("raw", "payload")
        }
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
    config = meshbot_state.config
    data_dir = config["paths"]["data_dir"]
    message_history_file = config["paths"].get(
        "message_history", "message_history.jsonl"
    )
    path = os.path.join(data_dir, message_history_file)

    message_record = {
        "timestamp": time.time(),
        "from": packet["from"],
        "to": packet["to"],
        "text": text,
        "channel": packet.get("channel", None),
        "from_id": packet.get("fromId", None),
        "to_id": packet.get("toId", None),
        "packet_id": packet.get("id", None),
    }
    with open(path, "a") as f:
        json.dump(message_record, f)
        f.write("\n")


def save_packet_log(packet, interface):
    """save every received packet to the packet log"""
    if meshbot_state is None:
        return
    config = meshbot_state.config
    data_dir = config["paths"]["data_dir"]
    packet_log_file = config["paths"].get("packet_log", "packet_log.jsonl")
    path = os.path.join(data_dir, packet_log_file)

    packet_record = {"timestamp": time.time(), "packet": filtered_mesh_dict(packet)}
    with open(path, "a") as f:
        json.dump(packet_record, f)
        f.write("\n")


def save_traceroute_log(node_id: str, traceroute_data: Dict[str, Any], route_names: List[str], route_back_names: List[str], interface: Any) -> None:
    """save traceroute result to persistent log file"""
    config = meshbot_state.config
    data_dir = config["paths"]["data_dir"]
    traceroute_log_file = config["paths"].get("traceroute_log", "traceroute_log.jsonl")
    path = os.path.join(data_dir, traceroute_log_file)

    state = meshbot_state
    _, _, node_name = find_node_name(state.nodedb_info, node_id)

    traceroute_record = {
        "timestamp": time.time(),
        "node_id": node_id,
        "node_name": node_name,
        "route": traceroute_data.get("route", []),
        "route_names": route_names,
        "route_back": traceroute_data.get("routeBack", []),
        "route_back_names": route_back_names,
        "snr_towards": traceroute_data.get("snrTowards", []),
        "snr_back": traceroute_data.get("snrBack", []),
    }
    with open(path, "a") as f:
        json.dump(traceroute_record, f)
        f.write("\n")


def is_idle_hours(interface):
    """check if current time is within configured idle hours"""
    config = meshbot_state.config
    now = datetime.datetime.now()
    current_hour = now.hour
    start_hour = config["channel"]["idle_start_hour"]
    end_hour = config["channel"]["idle_end_hour"]
    return start_hour <= current_hour < end_hour


def make_idle_message(interface):
    config = meshbot_state.config
    return "%s, %s." % (
        random.choice(config["llm"]["idle_prompts"]),
        random.choice(config["llm"]["idle_prompt_styles"]),
    )


def send_idle_message(interface):
    if not is_idle_hours(interface):
        return
    state = meshbot_state
    state.idle_context_messages.append(
        {"from": "khimaros (KHMR)", "text": make_idle_message(interface)}
    )
    message = send_llm_request(interface, state.idle_context_messages)
    mesh_send_channel(0, message, interface)
    state.idle_context_messages.append({"from": state.local_name, "text": message})


def main() -> None:
    """initialize mesh interface and run message processing loop"""
    global meshbot_state
    config = load_config()
    state = BotState(config)
    meshbot_state = state

    if not os.path.exists(config["paths"]["data_dir"]):
        os.makedirs(config["paths"]["data_dir"])

    shutdown_requested = threading.Event()
    connection_lost = threading.Event()
    force_exit_requested = threading.Event()

    # Store connection_lost in state so other functions can check it
    state.connection_lost = connection_lost
    # Track abandoned threads for cleanup
    state.abandoned_threads = []

    def signal_handler(signum, frame):
        """handle shutdown signals gracefully, force exit on second signal"""
        if shutdown_requested.is_set():
            print("\n> second interrupt received, forcing immediate exit...")
            force_exit_requested.set()
            os._exit(1)
        print("\n> shutdown requested, please wait...")
        shutdown_requested.set()

    # register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    def on_connection_lost(interface: Any):
        """handle mesh disconnection events"""
        if not connection_lost.is_set():
            print("> connection lost!")
            connection_lost.set()

    # load state before attempting to connect
    state.load_all()

    # register pubsub handlers after state is initialized
    pubsub.pub.subscribe(
        mesh_connection_established, "meshtastic.connection.established"
    )
    pubsub.pub.subscribe(on_connection_lost, "meshtastic.connection.lost")
    pubsub.pub.subscribe(mesh_client_proxy_message, "meshtastic.mqttclientproxymessage")
    pubsub.pub.subscribe(mesh_receive, "meshtastic.receive")
    pubsub.pub.subscribe(send_idle_message, "meshbot.idle.expired")

    while not shutdown_requested.is_set():
        interface = None
        try:
            connection_lost.clear()
            interface = setup_interface(config, state, shutdown_requested)
            print(
                f"> received identification for local node: {meshbot_state.local_name} [{meshbot_state.local_id}]"
            )
            print(
                "> local location initialized as:",
                meshbot_state.local_location,
            )

            last_idle_time = time.time()
            last_health_check = time.time()
            idle_interval = config["channel"]["idle_frequency"]

            # Initialize last_packet_time to now so we don't immediately fail health check
            state.last_packet_time = time.time()

            while not shutdown_requested.is_set() and not connection_lost.is_set():
                # Wait up to 1 second, checking for shutdown or connection loss
                if wait_for_any_event(shutdown_requested, connection_lost, timeout=1.0):
                    break

                # Check connection health and cleanup threads every 10 seconds
                if time.time() - last_health_check >= 10:
                    last_health_check = time.time()
                    cleanup_abandoned_threads(state)
                    if not check_connection_health(config, state):
                        print("> triggering reconnection due to stale connection")
                        connection_lost.set()
                        break

                # Check if it's time to send idle message
                if idle_interval and time.time() - last_idle_time >= idle_interval:
                    pubsub.pub.sendMessage("meshbot.idle.expired", interface=interface)
                    last_idle_time = time.time()

                # Process traceroute queue for stale nodes (async to avoid blocking on meshtastic calls)
                process_traceroute_queue_async(interface)

        except Exception as e:
            print(f"> received unhandled exception: {e}")
            # Always save state if loaded, regardless of interface state
            if state.loaded:
                print("> saving state before reconnecting...")
                state.save_all()
            if interface:
                try:
                    close_interface(interface, config, shutdown_requested)
                except Exception as close_err:
                    print(f"> error closing interface: {close_err}")

            if not shutdown_requested.is_set():
                print("> attempting to reconnect in 10 seconds...")
                interruptible_wait(shutdown_requested, 10)

    # clean shutdown
    print()
    print("> saving and exiting...")
    # Save state first, before attempting to close interface (which may hang)
    if state.loaded:
        print("> saving state...")
        state.save_all()
        print("> state saved")
    if interface:
        print("> closing interface...")
        close_interface(interface, config, shutdown_requested)


if __name__ == "__main__":
    main()
