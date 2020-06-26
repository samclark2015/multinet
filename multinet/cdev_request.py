import traceback
from collections.abc import Iterable
from itertools import groupby
from operator import itemgetter
from threading import Lock
from typing import *

from cad_io import cns3 as cns
from cad_io.cdev import tags
from cad_io.cdev.clip import ClipConnection, ClipData, ClipPacket

from .request import Callback, Entry, Request, MultinetError


class CDEVRequest(Request):
    """Used to make requests to a CDEV server."""

    def __init__(self):
        """
        Arguments:
        	server (str): name of server to call
        """
        super().__init__()
        self._trans_idx_lock = Lock()
        self._trans_idx = 1

        self._asyncs = dict()
        self._async_conn = None

    def _get_trans_idx(self):
        with self._trans_idx_lock:
            idx = self._trans_idx
            self._trans_idx += 1
            return idx

    def _send(
        self,
        conn: ClipConnection,
        message: str,
        request_data: dict = None,
        context: dict = None,
        device: Union[str, List[str]] = None,
        trans_idx=None,
        cancel_trans_idx=None,
        expecting_data=True,
    ) -> ClipPacket:
        """Internal method to send data to server"""
        if isinstance(device, str):
            device = [device]
        elif isinstance(device, Iterable):
            device = list(device)
        elif device is not None:
            raise Exception("'device' must either be str or list")
        packet = ClipPacket()
        trans_idx = trans_idx if trans_idx is not None else self._get_trans_idx()
        packet.set_trans_index(trans_idx)
        if cancel_trans_idx is not None:
            packet.set_cancel_trans_index(cancel_trans_idx)
        if request_data:
            data = ClipData()
            for key in request_data:
                data.insert(tags.get_tag(key), request_data[key])
            packet.set_request_data(data)
        if context:
            data = ClipData()
            for key in context:
                data.insert(tags.get_tag(key), context[key])
            packet.set_context(data)
        packet.set_device_list(device)
        packet.set_message(message)

        conn.send(packet)
        if expecting_data:
            response = conn.receive()
            return response

    def send(
        self,
        server,
        message,
        request_data: dict = None,
        context: dict = None,
        device: Union[str, List[str]] = None,
    ):
        """
        Send a message

        Arguments:
        	message (str): message to send to server

        Keyword arguments:
        	request_data (dict): request data to send with message
            device (Union[str, List[str]]): device or list of devices to send message
        """
        with ClipConnection(server) as conn:
            resp = self._send(conn, message, request_data, context, device)
            return resp.request_data

    def get(
        self, *entries: Entry, timestamp=True, ppm_user=1, **kwargs
    ) -> Dict[Entry, Any]:
        """
        Get property from device

        Arguments:
        	device (str): device or list of devices on which to retrieve the property
        	prop (str): property to fetch
        """
        entries = self._unpack_entries(*entries)
        responses = dict()
        for server, entries in entries.items():
            with ClipConnection(server) as conn:
                for ent_name, ent_data in entries.items():
                    device = ent_data[0]
                    prop = ent_data[1]
                    tag = ent_data[2]
                    message = f"get {prop}"
                    resp = self._send(
                        conn, message, device=device, context=dict(ppmuser=ppm_user),
                    )
                    responses[ent_name] = resp.request_data[tag]
                    if timestamp and tags.TIMESTAMP in resp.request_data:
                        responses[(device, prop, "timestamp")] = resp.request_data[
                            tags.TIMESTAMP
                        ]
        return responses

    def get_meta(self, *entries: Entry, **kwargs):
        entries = self._unpack_entries(*entries)
        responses = dict()
        for server, entries in entries.items():
            with ClipConnection(server) as conn:
                for ent_name, ent_data in entries.items():
                    device, prop = ent_data[:2]
                    message = f"getMeta {prop}"
                    resp = self._send(conn, message, device=device,)
                    responses[ent_name] = {
                        tags.key_to_tag(k): v for k, v in resp.request_data.items()
                    }
        return responses

    def set(self, *entries: Entry, ppm_user=1, **kwargs) -> Optional[MultinetError]:
        """
        Set value of property on device

        Arguments:
        	device (str): device or list of devices on which to retrieve the property
        	prop (str): property to fetch
        	val (any): new value of property
        """
        entries = self._unpack_entries(*entries, is_set=True)
        errors = []
        for server, entries in entries.items():
            with ClipConnection(server) as conn:
                for ent_key, ent_data in entries.items():
                    device = ent_data[0]
                    prop = ent_data[1]
                    tag = ent_data[2]
                    value = ent_data[3]
                    message = f"set {prop}"
                    resp = self._send(
                        conn,
                        message,
                        device=device,
                        request_data={tags.key_to_tag(tag): value},
                        context=dict(ppmuser=ppm_user),
                    )
                    if resp.completion_code is not None:
                        errors.append(f"Failed to set {ent_key}")
        return MultinetError("; ".join(errors)) if errors else None

    def _async_handler(self, resp: ClipPacket):
        try:
            async_data = self._asyncs[resp._trans_index]
            callback = async_data["callback"]
            ppm_user = async_data["ppm"]
            entry = async_data["entry"]
            tag = async_data["tag"]
            timestamp = async_data["timestamp"]
            response = {entry: resp.request_data[tag]}
            if timestamp and tags.TIMESTAMP in resp.request_data:
                response[(*entry[:2], "timestamp")] = resp.request_data[tags.TIMESTAMP]
            callback(response, ppm_user)
        except Exception:  # pylint: disable=broad-except
            traceback.print_exc()

    def get_async(
        self, callback: Callback, *entries: Entry, ppm_user=1, timestamp=True, **kwargs
    ):
        """
        Registers async handler and requests updates from server

        Arguments:
        	device (str): device or list of devices on which to retrieve the property
        	prop (str): property to fetch
        	callback (callable): method or function to call on receipt of data

        Returns:
        	idx (int): transaction index (pass to cancel_async method)
        """
        entries = self._unpack_entries(*entries)
        for server, entries in entries.items():
            conn = ClipConnection(server)
            conn.open(on_receive=self._async_handler)
            for ent_name, ent_data in entries.items():
                device, prop = ent_data[:2]
                trans_idx = self._get_trans_idx()
                message = f"monitorOn {prop}"
                self._asyncs[trans_idx] = {
                    "trans_idx": trans_idx,
                    "callback": callback,
                    "entry": ent_name,
                    "conn": conn,
                    "server": server,
                    "ppm": ppm_user,
                    "timestamp": timestamp,
                    "tag": ent_data[-1],
                }
                self._send(
                    conn,
                    message,
                    device=device,
                    trans_idx=trans_idx,
                    context=dict(ppmuser=ppm_user),
                    expecting_data=False,
                )

    def cancel_async(self):
        for conn, entries in groupby(self._asyncs.values(), itemgetter("conn")):
            for entry in entries:
                tid = entry["trans_idx"]
                device = entry["entry"][0]
                prop = entry["entry"][1]
                message = f"monitorOff {prop}"
                self._send(
                    conn,
                    message,
                    cancel_trans_idx=tid,
                    device=device,
                    expecting_data=False,
                )
            conn.close()

    def set_history(self, enabled):
        pass

    def _unpack_entries(self, *entries, is_set=False) -> Dict[str, Dict[Entry, Entry]]:
        entries = [tuple(ent) for ent in entries]
        requests = {}
        entries = [(cns.cnslookup(ent[0]).string3, ent) for ent in entries]
        entries = groupby(entries, itemgetter(0))
        for server, group_entries in entries:
            group_requests: Dict[Entry, Entry] = {}
            for _, entry in group_entries:
                device = entry[0]
                param_name = entry[1]
                prop_name = (
                    tags.get_tag(entry[2])
                    if not is_set and len(entry) > 2 or is_set and len(entry) > 3
                    else tags.VALUE
                )
                if is_set:
                    value = entry[-1]
                    group_requests[entry] = (device, param_name, prop_name, value)
                else:
                    group_requests[entry] = (device, param_name, prop_name)
            requests[server] = group_requests
        return requests


if __name__ == "__main__":
    import time

    req = CDEVRequest()
    req.get_async(print, ("simple.cdev", "sinM"))
    time.sleep(5)
    req.cancel_async()
