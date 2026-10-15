import argparse
import json
import os
import threading
import time

import grpc

from beacon_protocol import (
    monitor_coordinator_heartbeats,
    send_coordinator_heartbeats,
    send_hospital_beacons,
)
from grpc_service import HospitalRpcClient, start_grpc_server
from hospital_registry import load_hospital_config
from message_service import start_message_consumer
from mongo_service import MongoStore
from ring_election import run_ring_election
from rpc_service import build_hospital_service


hospital_state = {
    "hospital_id": "",
    "hospital_name": "",
    "status": "ONLINE",
    "beds": {"ICU": 0, "Emergency": 0, "General": 0},
}

message_log = []
mongo_store = None


def create_hospital_data(hospital_id, hospital_name, beds):
    """Initialize the in-memory hospital state for one node."""
    persisted = mongo_store.load_or_create_hospital(hospital_id, hospital_name, beds)
    hospital_state["hospital_id"] = hospital_id
    hospital_state["hospital_name"] = hospital_name
    hospital_state["status"] = persisted.get("status", "ONLINE")
    hospital_state["beds"] = persisted.get("beds", beds)


def append_message(message):
    """Store a simple message for the local hospital log."""
    entry = {"timestamp": time.strftime("%H:%M:%S"), "message": message}
    message_log.append(entry)
    mongo_store.append_event(hospital_state["hospital_id"], message)


def call_peer_rpc(hospital, method, **payload):
    client = HospitalRpcClient(hospital["host"], hospital["rpc_port"])
    try:
        return client.call(method, **payload)
    finally:
        client.close()


def run_automatic_ring_election(hospitals, service):
    functions = service["functions"]
    state = functions["get_election_state"]()
    initiator_id = hospital_state["hospital_id"]
    epoch = state["epoch"] + 1

    def send_ring_message(target_id, candidate_ids, election_epoch, initiator):
        target = next(item for item in hospitals if item["hospital_id"] == target_id)
        try:
            return call_peer_rpc(
                target,
                "RingElection",
                candidate_ids=candidate_ids,
                epoch=election_epoch,
                initiator_id=initiator,
            )
        except (grpc.RpcError, OSError) as exc:
            print(f"Ring election could not reach {target_id}: {exc}", flush=True)
            return {"accepted": False, "message": str(exc)}

    def announce(target_id, leader_id, election_epoch):
        if target_id == initiator_id:
            return functions["announce_coordinator"](
                leader_id,
                election_epoch,
                "RING",
            )
        target = next(item for item in hospitals if item["hospital_id"] == target_id)
        try:
            return call_peer_rpc(
                target,
                "Coordinator",
                leader_id=leader_id,
                epoch=election_epoch,
                algorithm="RING",
            )
        except (grpc.RpcError, OSError) as exc:
            print(f"Ring election announcement to {target_id} failed: {exc}", flush=True)
            return {"accepted": False, "message": str(exc)}

    result = run_ring_election(
        [item["hospital_id"] for item in hospitals],
        send_ring_message,
        announce,
        initiator_id,
        epoch,
    )
    print(
        f"Coordinator heartbeat timed out; Ring election epoch {epoch} "
        f"elected {result['leader_id']}.",
        flush=True,
    )


def handle_coordinator_timeout(hospitals, service, status):
    try:
        run_automatic_ring_election(hospitals, service)
    except (OSError, RuntimeError, ValueError, grpc.RpcError) as exc:
        print(
            f"Coordinator heartbeat timed out after "
            f"{status['heartbeat_age_seconds']} seconds; Ring election failed: {exc}",
            flush=True,
        )


def start_server(host, port, hospital_id, hospital_name, beds):
    """Start an independent hospital node with gRPC and RabbitMQ."""
    global mongo_store
    mongo_store = MongoStore()
    mongo_store.wait_until_ready()
    create_hospital_data(hospital_id, hospital_name, beds)
    hospitals = load_hospital_config()
    service = build_hospital_service(
        hospital_state,
        host,
        port,
        mongo_store,
        [item["hospital_id"] for item in hospitals],
    )

    server = start_grpc_server(host, port, service["functions"])
    start_message_consumer(hospital_id, append_message)
    beacon_stop = threading.Event()
    hospital_beacon_thread = threading.Thread(
        target=send_hospital_beacons,
        args=(
            os.getenv("COORDINATOR_URL", "http://localhost:5000"),
            service["functions"]["create_beacon"],
            beacon_stop,
        ),
        name=f"{hospital_id}-coordinator-beacon",
        daemon=True,
    )
    hospital_beacon_thread.start()
    peer_beacon_thread = threading.Thread(
        target=send_coordinator_heartbeats,
        args=(
            hospitals,
            service["functions"]["create_coordinator_heartbeat"],
            lambda target, beacon: call_peer_rpc(
                target,
                "CoordinatorHeartbeat",
                **beacon,
            ),
            beacon_stop,
        ),
        name=f"{hospital_id}-elected-coordinator-beacon",
        daemon=True,
    )
    peer_beacon_thread.start()
    election_monitor_thread = threading.Thread(
        target=monitor_coordinator_heartbeats,
        args=(
            service["functions"]["get_coordinator_heartbeat_status"],
            lambda status: handle_coordinator_timeout(hospitals, service, status),
            beacon_stop,
        ),
        name=f"{hospital_id}-coordinator-failure-monitor",
        daemon=True,
    )
    election_monitor_thread.start()

    print(f"Hospital {hospital_id} started with gRPC on {host}:{port} and RabbitMQ events")
    print("Hospital liveness beacons enabled (every 2 seconds)")
    print("Coordinator heartbeat monitoring enabled (8-second timeout)")
    print(json.dumps(hospital_state, indent=2))
    try:
        server.wait_for_termination()
    finally:
        beacon_stop.set()
        hospital_beacon_thread.join(timeout=3)
        peer_beacon_thread.join(timeout=3)
        election_monitor_thread.join(timeout=3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a hospital node")
    parser.add_argument("--hospital-id", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--icu", type=int, default=0)
    parser.add_argument("--emergency", type=int, default=0)
    parser.add_argument("--general", type=int, default=0)
    args = parser.parse_args()

    default_beds = {
        "ICU": args.icu,
        "Emergency": args.emergency,
        "General": args.general,
    }

    start_server(args.host, args.port, args.hospital_id, args.name, default_beds)
