"""Small gRPC transport layer shared by the coordinator and hospital nodes.

Protobuf Struct keeps this teaching project easy to read while still giving us
 gRPC deadlines, HTTP/2 connections, and a clear service boundary.
"""

from concurrent import futures

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct


SERVICE_NAME = "hospital.HospitalService"


def _to_struct(value):
    message = Struct()
    message.update(value or {})
    return message


def _from_struct(message):
    return MessageToDict(message, preserving_proto_field_name=True)


def _method_path(method_name):
    return f"/{SERVICE_NAME}/{method_name}"


def start_grpc_server(host, port, service_functions):
    """Start a hospital gRPC server and return it to the caller."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))

    def make_handler(function):
        def handle(request, context):
            try:
                return _to_struct(function(**_from_struct(request)))
            except (TypeError, ValueError):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid RPC request.")

        return grpc.unary_unary_rpc_method_handler(
            handle,
            request_deserializer=Struct.FromString,
            response_serializer=Struct.SerializeToString,
        )

    handlers = {
        "Ping": make_handler(lambda: {"online": service_functions["ping_hospital"]()}),
        "GetHospitalStatus": make_handler(service_functions["get_hospital_status"]),
        "AdjustClock": make_handler(service_functions["adjust_clock"]),
        "CoordinatorHeartbeat": make_handler(service_functions["receive_coordinator_heartbeat"]),
        "GetCoordinatorHeartbeatStatus": make_handler(
            lambda: service_functions["get_coordinator_heartbeat_status"]()
        ),
        "GetBedAvailability": make_handler(service_functions["get_bed_availability"]),
        "Election": make_handler(service_functions["election_message"]),
        "RingElection": make_handler(service_functions["ring_election_message"]),
        "Coordinator": make_handler(service_functions["announce_coordinator"]),
        "GetElectionState": make_handler(service_functions["get_election_state"]),
        "ReserveBed": make_handler(service_functions["reserve_bed"]),
        "ReleaseBed": make_handler(service_functions["release_bed"]),
    }
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler(SERVICE_NAME, handlers),)
    )
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server


class HospitalRpcClient:
    """A short-lived, deadline-aware client for one hospital node."""

    def __init__(self, host, port, timeout=2):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.timeout = timeout

    def call(self, method_name, **payload):
        rpc = self.channel.unary_unary(
            _method_path(method_name),
            request_serializer=Struct.SerializeToString,
            response_deserializer=Struct.FromString,
        )
        return _from_struct(rpc(_to_struct(payload), timeout=self.timeout))

    def close(self):
        self.channel.close()


class RemoteHospitalObject:
    """Proxy for invoking operations on one hospital's remote object."""

    def __init__(self, hospital, client):
        self.hospital_id = hospital["hospital_id"]
        self._client = client

    def invoke(self, operation, **payload):
        if operation not in {
            "Ping",
            "GetHospitalStatus",
            "AdjustClock",
            "CoordinatorHeartbeat",
            "GetCoordinatorHeartbeatStatus",
            "GetBedAvailability",
            "Election",
            "RingElection",
            "Coordinator",
            "GetElectionState",
            "ReserveBed",
            "ReleaseBed",
        }:
            raise ValueError(f"Unsupported remote hospital operation: {operation}")
        return self._client.call(operation, **payload)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
