import json
import os
import threading
import time
import uuid

import grpc
import requests
from flask import Flask, Response, jsonify, request
from flask_cors import CORS

from beacon_protocol import HospitalBeaconRegistry, send_coordinator_heartbeats
from consultation_registry import ConsultationRegistry
from analytics.hdfs_storage import HdfsStorageError, archive_analytics_records
from analytics.streaming_common import aggregate_records
from clock_sync import ClockAdjustment, calculate_berkeley_adjustments, estimate_clock_offset
from deadlock import detect_deadlocks
from election import run_bully_election
from grpc_service import HospitalRpcClient, RemoteHospitalObject
from hospital_registry import load_hospital_config
from lamport_clock import LamportClock
from message_service import build_message, send_message
from mongo_service import MongoStore
from mutex import CentralizedMutex, VALID_POLICIES
from ring_election import run_ring_election
from vector_clock import VectorClock

app = Flask(__name__)
CORS(app)

SYSTEM_LOGS = []
SYSTEM_LOG_LOCK = threading.Lock()
MONGO_STORE = MongoStore()
COORDINATOR_CLOCK = LamportClock(
    "COORDINATOR",
    MONGO_STORE.next_lamport_timestamp,
)
COORDINATOR_VECTOR_CLOCK = VectorClock(
    "COORDINATOR",
    MONGO_STORE.next_vector_clock,
)
CONSULTATION_REGISTRY = ConsultationRegistry()
HOSPITAL_BEACONS = HospitalBeaconRegistry()
COORDINATOR_CLOCK_ADJUSTMENT = ClockAdjustment()
ELECTION_LOCK = threading.Lock()
ELECTION_STATE = {
    "status": "NO_LEADER",
    "leader_id": None,
    "epoch": 0,
    "events": [],
    "acknowledged_nodes": [],
}
RESERVATION_MUTEX = CentralizedMutex()


def add_log(message):
    """Add a communication event to the in-memory log list."""
    timestamp = time.strftime("%H:%M:%S")
    lamport_timestamp = COORDINATOR_CLOCK.tick()
    vector_timestamp = COORDINATOR_VECTOR_CLOCK.tick()
    with SYSTEM_LOG_LOCK:
        SYSTEM_LOGS.append({
            "timestamp": timestamp,
            "message": message,
            "lamport_timestamp": lamport_timestamp,
            "vector_clock": vector_timestamp,
        })
    MONGO_STORE.append_log(
        "COORDINATOR",
        message,
        lamport_timestamp,
        vector_timestamp,
    )


def get_hospital_config():
    """Return the configured hospital list from the central registry."""
    return load_hospital_config()


def remote_hospital(hospital):
    """Create a remote object reference bound to the hospital's gRPC endpoint."""
    client = HospitalRpcClient(hospital["host"], hospital["rpc_port"])
    return RemoteHospitalObject(hospital, client)


def ping_hospital(hospital):
    """Check whether a hospital node is reachable using gRPC with a deadline."""
    try:
        with remote_hospital(hospital) as remote:
            result = remote.invoke("Ping")
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC ping_hospital() | Response: {result}")
        beacon = HOSPITAL_BEACONS.get_status(hospital["hospital_id"])
        return bool(result.get("online")) and beacon["beacon_status"] == "ONLINE"
    except Exception as exc:
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC ping_hospital() | Timeout/Failure: {exc}")
        return False


def get_hospital_status(hospital):
    """Ask a hospital for its local state using gRPC."""
    try:
        outgoing_clock = COORDINATOR_CLOCK.tick()
        outgoing_vector = COORDINATOR_VECTOR_CLOCK.tick()
        coordinator_sent_ms = COORDINATOR_CLOCK_ADJUSTMENT.now_ms()
        with remote_hospital(hospital) as remote:
            status = remote.invoke(
                "GetHospitalStatus",
                lamport_timestamp=outgoing_clock,
                vector_timestamp=outgoing_vector,
            )
        coordinator_received_ms = COORDINATOR_CLOCK_ADJUSTMENT.now_ms()
        heartbeat_status = status.get("coordinator_heartbeat")
        clock_sample = status.get("clock_sample")
        if clock_sample is not None:
            status["clock_sync"] = estimate_clock_offset(
                coordinator_sent_ms,
                clock_sample["hospital_received_ms"],
                clock_sample["hospital_sent_ms"],
                coordinator_received_ms,
            )
        if "lamport_timestamp" in status:
            COORDINATOR_CLOCK.receive(status["lamport_timestamp"])
        if "vector_clock" in status:
            COORDINATOR_VECTOR_CLOCK.receive(status["vector_clock"])
        status["coordinator_heartbeat"] = heartbeat_status
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC get_hospital_status() | Response: {status}")
        return status
    except Exception as exc:
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC get_hospital_status() | Failure: {exc}")
        return None


def get_hospital_availability(hospital):
    """Ask a hospital how many beds of each type are available using gRPC."""
    try:
        outgoing_clock = COORDINATOR_CLOCK.tick()
        outgoing_vector = COORDINATOR_VECTOR_CLOCK.tick()
        with remote_hospital(hospital) as remote:
            result = remote.invoke(
                "GetBedAvailability",
                lamport_timestamp=outgoing_clock,
                vector_timestamp=outgoing_vector,
            )
        if "lamport_timestamp" in result:
            COORDINATOR_CLOCK.receive(result["lamport_timestamp"])
        if "vector_clock" in result:
            COORDINATOR_VECTOR_CLOCK.receive(result["vector_clock"])
        availability = result.get("beds", {})
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC get_bed_availability() | Response: {availability}")
        return availability
    except Exception as exc:
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC get_bed_availability() | Failure: {exc}")
        return None


def call_hospital_rpc(hospital, method, **payload):
    """Call a hospital RPC and always release its channel."""
    with remote_hospital(hospital) as remote:
        return remote.invoke(method, **payload)


@app.route("/api/election/status", methods=["GET"])
def api_election_status():
    """Return the most recently elected coordinator role."""
    with ELECTION_LOCK:
        return jsonify(dict(ELECTION_STATE))


def mutex_policy_payload():
    lock_state = RESERVATION_MUTEX.status()
    policy = lock_state["policy"]
    return {
        "policy": policy,
        "choices": sorted(VALID_POLICIES),
        "lock_state": lock_state,
        "description": (
            "FAIR orders reservation critical sections by Lamport timestamp."
            if policy == "FAIR"
            else "PRIORITY allows Critical and High requests to move ahead of Normal requests."
        ),
    }


@app.route("/api/mutex/policy", methods=["GET"])
def api_mutex_policy():
    """Return the active centralized reservation-lock ordering policy."""
    return jsonify(mutex_policy_payload())


@app.route("/api/mutex/policy", methods=["POST"])
def api_set_mutex_policy():
    """Switch between Lamport-FIFO fairness and urgency-first reservation ordering."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or payload.get("policy") not in VALID_POLICIES:
        return jsonify({
            "success": False,
            "message": "Policy must be FAIR or PRIORITY.",
        }), 400
    policy = RESERVATION_MUTEX.set_policy(payload["policy"])
    add_log(f"Coordinator | Reservation mutual-exclusion policy changed to {policy}.")
    return jsonify({"success": True, **mutex_policy_payload()})


@app.route("/api/election/start", methods=["POST"])
def api_start_election():
    """Run a Bully election over the currently reachable hospital RPC nodes."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400

    with ELECTION_LOCK:
        configured = {
            hospital["hospital_id"]: hospital
            for hospital in get_hospital_config()
        }
        live_ids = sorted(
            hospital_id
            for hospital_id, hospital in configured.items()
            if ping_hospital(hospital)
        )
        if not live_ids:
            ELECTION_STATE.update({
                "status": "NO_LEADER",
                "leader_id": None,
                "events": ["No reachable hospital can participate in the election."],
                "acknowledged_nodes": [],
            })
            return jsonify({
                "success": False,
                "message": "No reachable hospital can participate in the election.",
                **ELECTION_STATE,
            }), 503

        initiator_id = payload.get("initiator_id", live_ids[0])
        if initiator_id not in live_ids:
            return jsonify({
                "success": False,
                "message": "The election initiator must be a reachable hospital.",
            }), 400

        try:
            observed_epochs = [
                call_hospital_rpc(configured[hospital_id], "GetElectionState").get("epoch", 0)
                for hospital_id in live_ids
            ]
            if any(
                isinstance(node_epoch, bool)
                or not isinstance(node_epoch, (int, float))
                or node_epoch < 0
                or isinstance(node_epoch, float) and not node_epoch.is_integer()
                for node_epoch in observed_epochs
            ):
                raise ValueError("A hospital returned an invalid election epoch.")
            epoch = max([ELECTION_STATE["epoch"], *(int(value) for value in observed_epochs)]) + 1
            result = run_bully_election(
                live_ids,
                send_election=lambda caller, target, election_epoch: call_hospital_rpc(
                    configured[target],
                    "Election",
                    caller_id=caller,
                    epoch=election_epoch,
                ),
                announce_coordinator=lambda target, leader, election_epoch: call_hospital_rpc(
                    configured[target],
                    "Coordinator",
                    leader_id=leader,
                    epoch=election_epoch,
                ),
                initiator_id=initiator_id,
                epoch=epoch,
            )
        except (OSError, RuntimeError, ValueError, grpc.RpcError) as exc:
            ELECTION_STATE.update({
                "status": "FAILED",
                "events": [f"Election failed: {exc}"],
                "acknowledged_nodes": [],
            })
            add_log(f"Coordinator | Bully election failed: {exc}")
            return jsonify({
                "success": False,
                "message": "The election could not complete. Check the hospital RPC nodes.",
                **ELECTION_STATE,
            }), 503

        all_nodes_acknowledged = set(result["acknowledged_nodes"]) == set(live_ids)
        result["status"] = "LEADER_ELECTED" if all_nodes_acknowledged else "PARTIAL_ANNOUNCEMENT"
        ELECTION_STATE.update(result)
        ELECTION_STATE["success"] = all_nodes_acknowledged
        for event in result["events"]:
            add_log(f"Bully election epoch {epoch} | {event}")

        return jsonify(dict(ELECTION_STATE)), 200 if all_nodes_acknowledged else 503


@app.route("/api/election/ring-start", methods=["POST"])
def api_start_ring_election():
    """Run a Ring election using the reachable hospital RPC nodes."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400

    with ELECTION_LOCK:
        configured = {
            hospital["hospital_id"]: hospital
            for hospital in get_hospital_config()
        }
        live_ids = [
            hospital_id
            for hospital_id, hospital in configured.items()
            if ping_hospital(hospital)
        ]
        if not live_ids:
            ELECTION_STATE.update({
                "status": "NO_LEADER",
                "leader_id": None,
                "events": ["No reachable hospital can participate in the election."],
                "acknowledged_nodes": [],
            })
            return jsonify({
                "success": False,
                "message": "No reachable hospital can participate in the election.",
                **ELECTION_STATE,
            }), 503

        initiator_id = payload.get("initiator_id", live_ids[0])
        if initiator_id not in live_ids:
            return jsonify({
                "success": False,
                "message": "The election initiator must be a reachable hospital.",
            }), 400

        try:
            observed_epochs = [
                call_hospital_rpc(configured[hospital_id], "GetElectionState").get("epoch", 0)
                for hospital_id in live_ids
            ]
            if any(
                isinstance(node_epoch, bool)
                or not isinstance(node_epoch, (int, float))
                or node_epoch < 0
                or isinstance(node_epoch, float) and not node_epoch.is_integer()
                for node_epoch in observed_epochs
            ):
                raise ValueError("A hospital returned an invalid election epoch.")
            epoch = max([ELECTION_STATE["epoch"], *(int(value) for value in observed_epochs)]) + 1

            def send_ring_message(target_id, candidate_ids, election_epoch, ring_initiator):
                try:
                    return call_hospital_rpc(
                        configured[target_id],
                        "RingElection",
                        candidate_ids=candidate_ids,
                        epoch=election_epoch,
                        initiator_id=ring_initiator,
                    )
                except (grpc.RpcError, OSError) as exc:
                    return {"accepted": False, "message": str(exc)}

            def announce(target_id, leader_id, election_epoch):
                try:
                    return call_hospital_rpc(
                        configured[target_id],
                        "Coordinator",
                        leader_id=leader_id,
                        epoch=election_epoch,
                        algorithm="RING",
                    )
                except (grpc.RpcError, OSError) as exc:
                    return {"accepted": False, "message": str(exc)}

            result = run_ring_election(
                live_ids,
                send_ring_message,
                announce,
                initiator_id=initiator_id,
                epoch=epoch,
            )
        except (OSError, RuntimeError, ValueError, grpc.RpcError) as exc:
            ELECTION_STATE.update({
                "status": "FAILED",
                "events": [f"Ring election failed: {exc}"],
                "acknowledged_nodes": [],
            })
            add_log(f"Coordinator | Ring election failed: {exc}")
            return jsonify({
                "success": False,
                "message": "The Ring election could not complete. Check the hospital RPC nodes.",
                **ELECTION_STATE,
            }), 503

        completed = (
            not result["failures"]
            and set(result["acknowledged_nodes"]) == set(result["participants"])
        )
        result["status"] = "LEADER_ELECTED" if completed else "PARTIAL_ANNOUNCEMENT"
        ELECTION_STATE.update(result)
        ELECTION_STATE["success"] = completed
        for event in result["events"]:
            add_log(f"Ring election epoch {epoch} | {event}")

        return jsonify(dict(ELECTION_STATE)), 200 if completed else 503


def analyze_deadlock_transactions(transactions):
    try:
        return detect_deadlocks(transactions)
    except ValueError as exc:
        return None, str(exc)


@app.route("/api/deadlock/check", methods=["POST"])
def api_check_deadlock():
    """Run edge-chasing detection against a supplied set of held and requested resources."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400
    analysis = analyze_deadlock_transactions(payload.get("transactions"))
    if isinstance(analysis, tuple):
        return jsonify({"success": False, "message": analysis[1]}), 400
    return jsonify({"success": True, **analysis})


@app.route("/api/deadlock/demo", methods=["POST"])
def api_deadlock_demo():
    """Create opposite-order hospital transfer waits and resolve their deadlock."""
    transfers = [
        {
            "transaction_id": "TRANSFER-001",
            "lamport_timestamp": COORDINATOR_CLOCK.tick(),
            "holds": ["H001:ICU"],
            "waits_for": ["H002:ICU"],
        },
        {
            "transaction_id": "TRANSFER-002",
            "lamport_timestamp": COORDINATOR_CLOCK.tick(),
            "holds": ["H002:ICU"],
            "waits_for": ["H001:ICU"],
        },
    ]
    analysis = analyze_deadlock_transactions(transfers)
    if isinstance(analysis, tuple):
        return jsonify({"success": False, "message": analysis[1]}), 500

    victim = analysis.get("victim")
    if victim:
        add_log(
            f"Edge-chasing detected a transfer deadlock; aborted youngest "
            f"transaction {victim['transaction_id']} at L{victim['lamport_timestamp']}."
        )
    return jsonify({"success": True, **analysis})


def reserve_bed_at_hospital(hospital, bed_type, patient_id, priority=None):
    """Request a bed reservation from one selected hospital using gRPC."""
    try:
        outgoing_clock = COORDINATOR_CLOCK.tick()
        outgoing_vector = COORDINATOR_VECTOR_CLOCK.tick()
        reservation_payload = {
            "bed_type": bed_type,
            "patient_id": patient_id,
            "lamport_timestamp": outgoing_clock,
            "vector_timestamp": outgoing_vector,
        }
        if priority is not None:
            reservation_payload["priority"] = priority
        with remote_hospital(hospital) as remote:
            result = remote.invoke("ReserveBed", **reservation_payload)
        if "lamport_timestamp" in result:
            COORDINATOR_CLOCK.receive(result["lamport_timestamp"])
        if "vector_clock" in result:
            COORDINATOR_VECTOR_CLOCK.receive(result["vector_clock"])
        add_log(
            f"Coordinator -> Hospital {hospital['hospital_id']} | RPC reserve_bed("
            f"'{bed_type}', '{patient_id}') | Response: {result}"
        )
        return result
    except Exception as exc:
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC reserve_bed() | Failure: {exc}")
        return {"success": False, "message": "RPC failed during reservation."}


def release_bed_at_hospital(hospital, bed_type):
    """Release a hospital bed manually using gRPC."""
    try:
        outgoing_clock = COORDINATOR_CLOCK.tick()
        outgoing_vector = COORDINATOR_VECTOR_CLOCK.tick()
        with remote_hospital(hospital) as remote:
            result = remote.invoke(
                "ReleaseBed",
                bed_type=bed_type,
                lamport_timestamp=outgoing_clock,
                vector_timestamp=outgoing_vector,
            )
        if "lamport_timestamp" in result:
            COORDINATOR_CLOCK.receive(result["lamport_timestamp"])
        if "vector_clock" in result:
            COORDINATOR_VECTOR_CLOCK.receive(result["vector_clock"])
        add_log(
            f"Coordinator -> Hospital {hospital['hospital_id']} | RPC release_bed("
            f"'{bed_type}') | Response: {result}"
        )
        return result
    except Exception as exc:
        add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | RPC release_bed() | Failure: {exc}")
        return {"success": False, "message": "RPC failed during deallocation."}


def get_all_hospital_statuses():
    """Collect the status of all configured hospitals using the RPC protocol."""
    hospitals = []
    for hospital in get_hospital_config():
        status = get_hospital_status(hospital)
        beacon = HOSPITAL_BEACONS.get_status(hospital["hospital_id"])
        if status is None:
            hospitals.append({
                "hospital_id": hospital["hospital_id"],
                "hospital_name": hospital["hospital_name"],
                "status": "OFFLINE",
                "beds": {"ICU": 0, "Emergency": 0, "General": 0},
                "rpc_status": "OFFLINE",
                "lamport_timestamp": None,
                **beacon,
                "clock_sync": None,
                "coordinator_heartbeat": None,
                "election_state": None,
                "clock_adjustment_ms": None,
            })
        elif status["status"] != "ONLINE" or beacon["beacon_status"] != "ONLINE":
            hospitals.append({
                "hospital_id": status["hospital_id"],
                "hospital_name": status["hospital_name"],
                "status": "OFFLINE",
                "beds": {"ICU": 0, "Emergency": 0, "General": 0},
                "rpc_status": "ONLINE",
                "lamport_timestamp": status.get("lamport_timestamp"),
                **beacon,
                "clock_sync": status.get("clock_sync"),
                "clock_adjustment_ms": status.get("clock_adjustment_ms", 0),
                "coordinator_heartbeat": status.get("coordinator_heartbeat"),
                "election_state": status.get("election_state"),
            })
        else:
            hospitals.append({
                "hospital_id": status["hospital_id"],
                "hospital_name": status["hospital_name"],
                "status": status["status"],
                "beds": status["beds"],
                "rpc_status": "ONLINE",
                "lamport_timestamp": status.get("lamport_timestamp"),
                **beacon,
                "clock_sync": status.get("clock_sync"),
                "clock_adjustment_ms": status.get("clock_adjustment_ms", 0),
                "coordinator_heartbeat": status.get("coordinator_heartbeat"),
                "election_state": status.get("election_state"),
            })
    return hospitals


def select_best_hospital(bed_type, hospital_statuses, priority="Normal"):
    """Route urgent requests to spare capacity and normal requests by best fit."""
    valid_hospitals = []
    for hospital in hospital_statuses:
        if hospital["status"] == "OFFLINE":
            continue
        availability = hospital["beds"].get(bed_type, 0)
        if availability > 0:
            valid_hospitals.append({
                "hospital_id": hospital["hospital_id"],
                "hospital_name": hospital["hospital_name"],
                "available_beds": availability,
                "beds": hospital["beds"],
            })

    if not valid_hospitals:
        return None

    if priority in ("Critical", "High"):
        best_hospital = max(valid_hospitals, key=lambda item: item["available_beds"])
        reason = f"selected the largest available {bed_type} capacity for a {priority.lower()}-priority request"
    else:
        best_hospital = min(valid_hospitals, key=lambda item: item["available_beds"])
        reason = f"used best-fit routing for a normal-priority {bed_type} request"
    add_log(f"Coordinator | Selected {best_hospital['hospital_name']} because it {reason}.")
    return best_hospital


def notify_hospital_messages(hospital, message_type, details):
    """Send a simple HTTP JSON message to a hospital node for message-oriented communication."""
    message = build_message(message_type, sender="COORDINATOR", **details)
    sent = send_message(hospital["host"], hospital["http_port"], message)
    add_log(f"Coordinator -> Hospital {hospital['hospital_id']} | Message: {message_type} | Sent: {sent}")
    return sent


@app.route("/api/health", methods=["GET"])
def api_health():
    """Return a simple coordinator health check."""
    return jsonify({"status": "ok", "message": "Coordinator is running."})


@app.route("/api/emergency/consultation-complete", methods=["POST"])
def api_complete_emergency_consultation():
    """Record the receiving hospital for a WebRTC-completed emergency consultation."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400
    request_id = payload.get("request_id")
    hospital_id = payload.get("hospital_id")
    if not isinstance(request_id, str) or not request_id.strip():
        return jsonify({"success": False, "message": "Request ID is required."}), 400
    if not isinstance(hospital_id, str) or not hospital_id.strip():
        return jsonify({"success": False, "message": "Hospital ID is required."}), 400

    emergency_request = MONGO_STORE.get_emergency_request(request_id.strip())
    if not emergency_request:
        return jsonify({"success": False, "message": "Emergency request not found or expired."}), 404
    expected_hospital_id = emergency_request.get("selected_hospital", {}).get("hospital_id")
    if expected_hospital_id != hospital_id.strip():
        return jsonify({"success": False, "message": "Consultation hospital does not match the selected hospital."}), 403
    if emergency_request.get("status") != "AWAITING_CONSULTATION":
        return jsonify({"success": False, "message": "This request is no longer awaiting consultation."}), 409
    try:
        CONSULTATION_REGISTRY.mark_completed(request_id.strip(), hospital_id.strip())
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    return jsonify({"success": True, "request_id": request_id.strip(), "hospital_id": hospital_id.strip()})


@app.route("/api/hospitals", methods=["GET"])
def api_hospitals():
    """Return the configuration list of hospitals for the frontend."""
    return jsonify(get_hospital_config())


@app.route("/api/hospitals/beacon", methods=["POST"])
def api_hospital_beacon():
    """Record a heartbeat from a configured hospital node."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400

    hospital_id = payload.get("hospital_id")
    hospital = next(
        (item for item in get_hospital_config() if item["hospital_id"] == hospital_id),
        None,
    )
    if hospital is None:
        return jsonify({"success": False, "message": "Unknown hospital node."}), 404
    if payload.get("hospital_name") != hospital["hospital_name"]:
        return jsonify({"success": False, "message": "Hospital identity does not match the registry."}), 400

    try:
        COORDINATOR_CLOCK.receive(payload.get("lamport_timestamp"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    beacon = HOSPITAL_BEACONS.record(hospital_id, hospital["hospital_name"])
    return jsonify({"success": True, **beacon})


@app.route("/api/hospitals/status", methods=["GET"])
def api_hospital_status():
    """Return the live hospital data as seen by the coordinator."""
    statuses = get_all_hospital_statuses()
    health = {
        "total_hospitals": len(statuses),
        "online_hospitals": sum(1 for item in statuses if item["status"] == "ONLINE"),
        "offline_hospitals": sum(1 for item in statuses if item["status"] == "OFFLINE"),
        "hospitals": statuses,
    }
    return jsonify(health)


@app.route("/api/clocks/berkeley-sync", methods=["POST"])
def api_berkeley_clock_sync():
    """Synchronize reachable hospital clocks toward the cluster-wide mean."""
    configured = {
        hospital["hospital_id"]: hospital
        for hospital in get_hospital_config()
    }
    statuses = get_all_hospital_statuses()
    clock_offsets = {"COORDINATOR": 0.0}
    participants = {}
    skipped = []

    for status in statuses:
        hospital_id = status["hospital_id"]
        if status["status"] != "ONLINE":
            skipped.append({
                "hospital_id": hospital_id,
                "reason": "Hospital is offline.",
            })
            continue
        clock_sync = status.get("clock_sync")
        if not isinstance(clock_sync, dict) or "estimated_offset_ms" not in clock_sync:
            skipped.append({
                "hospital_id": hospital_id,
                "reason": "Clock offset estimate is unavailable.",
            })
            continue

        clock_offsets[hospital_id] = clock_sync["estimated_offset_ms"]
        participants[hospital_id] = status

    if not participants:
        return jsonify({
            "success": False,
            "message": "No online hospital has a usable clock offset estimate.",
            "skipped": skipped,
        }), 503

    try:
        sync = calculate_berkeley_adjustments(clock_offsets)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 500

    coordinator_target_ms = (
        COORDINATOR_CLOCK_ADJUSTMENT.get_ms()
        + sync["adjustments_ms"]["COORDINATOR"]
    )
    adjustments = []
    failures = []

    for hospital_id, status in participants.items():
        target_adjustment_ms = (
            status.get("clock_adjustment_ms", 0)
            + sync["adjustments_ms"][hospital_id]
        )
        try:
            result = call_hospital_rpc(
                configured[hospital_id],
                "AdjustClock",
                clock_adjustment_ms=target_adjustment_ms,
            )
            if not result.get("success"):
                raise RuntimeError(result.get("message", "Hospital rejected the adjustment."))
            adjustments.append({
                "hospital_id": hospital_id,
                "estimated_offset_ms": status["clock_sync"]["estimated_offset_ms"],
                "adjustment_ms": sync["adjustments_ms"][hospital_id],
                "clock_adjustment_ms": result["clock_adjustment_ms"],
            })
        except Exception as exc:
            failures.append({
                "hospital_id": hospital_id,
                "message": str(exc),
            })

    COORDINATOR_CLOCK_ADJUSTMENT.set_ms(coordinator_target_ms)
    add_log(
        "Berkeley clock synchronization | "
        f"average offset {sync['average_offset_ms']} ms | "
        f"{len(adjustments)}/{len(participants)} hospital clocks adjusted."
    )
    return jsonify({
        "success": not failures,
        "message": (
            "Berkeley clock synchronization completed."
            if not failures
            else "Berkeley clock synchronization completed with unreachable or rejected nodes."
        ),
        "average_offset_ms": sync["average_offset_ms"],
        "coordinator_adjustment_ms": sync["adjustments_ms"]["COORDINATOR"],
        "coordinator_clock_adjustment_ms": COORDINATOR_CLOCK_ADJUSTMENT.get_ms(),
        "adjustments": adjustments,
        "skipped": skipped,
        "failures": failures,
    })


@app.route("/api/global-state/snapshots", methods=["GET"])
def api_global_state_snapshots():
    """Return recent persisted best-effort views of hospital local state."""
    return jsonify({"snapshots": MONGO_STORE.latest_global_snapshots()})


@app.route("/api/global-state/snapshots", methods=["POST"])
def api_capture_global_state():
    """Capture and persist each configured hospital's state as observed by the coordinator."""
    hospitals = get_all_hospital_statuses()
    snapshot = {
        "snapshot_id": uuid.uuid4().hex,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "coordinator_lamport_timestamp": COORDINATOR_CLOCK.tick(),
        "consistency": "BEST_EFFORT",
        "hospitals": hospitals,
    }
    stored_snapshot = MONGO_STORE.create_global_snapshot(snapshot)
    add_log(
        f"Coordinator | Captured global hospital state {snapshot['snapshot_id']} "
        f"at L{snapshot['coordinator_lamport_timestamp']}."
    )
    return jsonify({"snapshot": stored_snapshot}), 201


@app.route("/api/logs", methods=["GET"])
def api_logs():
    """Return the communication log for the demo UI."""
    return jsonify({"logs": MONGO_STORE.latest_logs()})


@app.route("/api/analytics/summary", methods=["GET"])
def api_analytics_summary():
    """Aggregate records locally or through the configured on-demand function."""
    records = MONGO_STORE.analytics_records()
    function_url = os.getenv("ANALYTICS_FUNCTION_URL")
    if function_url:
        try:
            response = requests.post(
                function_url,
                json={"records": records},
                timeout=(5, 60),
            )
            response.raise_for_status()
            function_result = response.json()
            summary = function_result.get("summary") if isinstance(function_result, dict) else None
            if not isinstance(summary, dict):
                raise ValueError("The analytics function returned an invalid summary.")
        except (requests.RequestException, ValueError):
            return jsonify({
                "success": False,
                "message": "The configured serverless analytics function could not return a valid report.",
            }), 502
        engine = "Knative serverless function"
    else:
        summary = aggregate_records(records)
        engine = "Local Hadoop Streaming-compatible MapReduce"
    return jsonify({
        "summary": summary,
        "record_count": len(records),
        "engine": engine,
    })


@app.route("/api/analytics/hdfs/export", methods=["POST"])
def api_analytics_hdfs_export():
    """Write de-identified analytics records to the configured HDFS cluster."""
    records = MONGO_STORE.analytics_records()
    try:
        archive = archive_analytics_records(records)
    except HdfsStorageError as exc:
        status_code = 503 if not os.getenv("HDFS_WEBHDFS_URL") else 502
        return jsonify({"success": False, "message": str(exc)}), status_code
    return jsonify({
        "success": True,
        "record_count": len(records),
        **archive,
    }), 201


@app.route("/api/analytics/export", methods=["GET"])
def api_analytics_export():
    """Export the de-identified JSON-lines input used by the Hadoop Streaming job."""
    records = MONGO_STORE.analytics_records()
    body = "".join(f"{json.dumps(record)}\n" for record in records)
    return Response(
        body,
        mimetype="application/x-ndjson",
        headers={"Content-Disposition": 'attachment; filename="hospital-analytics.ndjson"'},
    )


@app.route("/api/emergency/request", methods=["POST"])
@app.route("/api/emergency/plan", methods=["POST"])
def api_plan_emergency_request():
    """Find a suitable hospital but wait for staff consultation before reserving."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400
    patient_id = payload.get("patient_id")
    patient_name = payload.get("patient_name", "")
    bed_type = payload.get("bed_type")
    priority = payload.get("priority", "Normal")

    if not isinstance(patient_id, str) or not patient_id.strip():
        return jsonify({"success": False, "message": "Patient ID is required."}), 400
    if not isinstance(patient_name, str) or not isinstance(priority, str):
        return jsonify({"success": False, "message": "Patient name and priority must be text."}), 400
    patient_id = patient_id.strip()
    patient_name = patient_name.strip()
    priority = priority.strip()
    if priority not in ("Critical", "High", "Normal"):
        return jsonify({"success": False, "message": "Priority must be Critical, High, or Normal."}), 400
    if not isinstance(bed_type, str) or bed_type.strip() not in ["ICU", "Emergency", "General"]:
        return jsonify({"success": False, "message": "Invalid bed type."}), 400
    bed_type = bed_type.strip()

    hospital_statuses = []
    for hospital in get_hospital_config():
        online = ping_hospital(hospital)
        availability = get_hospital_availability(hospital) if online else None
        beacon = HOSPITAL_BEACONS.get_status(hospital["hospital_id"])
        is_online = availability is not None and beacon["beacon_status"] == "ONLINE"
        hospital_statuses.append({
            "hospital_id": hospital["hospital_id"],
            "hospital_name": hospital["hospital_name"],
            "status": "ONLINE" if is_online else "OFFLINE",
            "beds": availability if is_online else {"ICU": 0, "Emergency": 0, "General": 0},
            **beacon,
            **HOSPITAL_BEACONS.get_status(hospital["hospital_id"]),
        })

    best_hospital = select_best_hospital(bed_type, hospital_statuses, priority)
    if best_hospital is None:
        response = {
            "success": False,
            "message": f"No online hospital can provide a {bed_type} bed right now.",
            "comparison": hospital_statuses,
        }
        return jsonify(response)

    request_id = uuid.uuid4().hex
    response = {
        "success": True,
        "status": "CONSULTATION_REQUIRED",
        "request_id": request_id,
        "patient_id": patient_id,
        "patient_name": patient_name or f"Patient {patient_id}",
        "bed_type": bed_type,
        "priority": priority,
        "selected_hospital": {
            "hospital_id": best_hospital["hospital_id"],
            "hospital_name": best_hospital["hospital_name"],
            "available_beds": best_hospital["available_beds"],
        },
        "reason": (
            f"Selected the largest available {bed_type} capacity for a {priority.lower()}-priority request."
            if priority in ("Critical", "High")
            else f"Selected by best-fit routing to preserve larger-capacity hospitals for urgent cases."
        ),
        "comparison": hospital_statuses,
        "message": f"{best_hospital['hospital_name']} has been selected for consultation. No bed is reserved yet.",
    }
    MONGO_STORE.create_emergency_request({
        **response,
        "status": "AWAITING_CONSULTATION",
    })
    return jsonify(response)


@app.route("/api/emergency/confirm", methods=["POST"])
def api_confirm_emergency_request():
    """Reserve the planned bed after the receiving hospital consultation."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400
    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not request_id.strip():
        return jsonify({"success": False, "message": "Request ID is required."}), 400
    request_id = request_id.strip()
    emergency_request = MONGO_STORE.get_emergency_request(request_id)
    hospital_id = (
        emergency_request.get("selected_hospital", {}).get("hospital_id")
        if emergency_request else None
    )
    if not CONSULTATION_REGISTRY.has_completed(request_id, hospital_id):
        return jsonify({
            "success": False,
            "message": "Complete a connected consultation with the selected hospital before reserving the bed.",
        }), 409

    emergency_request = MONGO_STORE.claim_emergency_request(request_id)
    if emergency_request is None:
        existing_request = MONGO_STORE.get_emergency_request(request_id)
        if existing_request and existing_request.get("status") == "COMPLETED":
            return jsonify(existing_request["result"])
        if existing_request:
            if existing_request.get("status") == "CANCELLED":
                return jsonify({"success": False, "message": "This emergency request was cancelled."}), 409
            return jsonify({
                "success": False,
                "message": "This request is already being confirmed.",
            }), 409
        return jsonify({"success": False, "message": "Emergency request not found or expired."}), 404

    hospital_id = emergency_request["selected_hospital"]["hospital_id"]
    bed_type = emergency_request["bed_type"]
    patient_id = emergency_request["patient_id"]
    hospital_config = next(
        (hospital for hospital in get_hospital_config() if hospital["hospital_id"] == hospital_id),
        None,
    )
    if hospital_config is None or not ping_hospital(hospital_config):
        response = {
            "success": False,
            "message": "The selected hospital is offline. Start a new request to find another hospital.",
        }
        MONGO_STORE.complete_emergency_request(request_id, response, status="AWAITING_CONSULTATION")
        return jsonify(response)

    priority = emergency_request["priority"]
    with RESERVATION_MUTEX.acquire(
        request_id,
        priority,
        COORDINATOR_CLOCK.tick(),
    ):
        availability = get_hospital_availability(hospital_config)
        if not availability or availability.get(bed_type, 0) <= 0:
            response = {
                "success": False,
                "message": f"No {bed_type} beds remain at {hospital_config['hospital_name']}. Start a new request to select another hospital.",
            }
            reservation = None
        else:
            reservation = reserve_bed_at_hospital(
                hospital_config,
                bed_type,
                patient_id,
                priority=priority,
            )
            response = None
            if not reservation.get("success"):
                response = {
                    "success": False,
                    "message": reservation.get("message", "Reservation failed."),
                }

    if response is not None:
        MONGO_STORE.complete_emergency_request(request_id, response, status="AWAITING_CONSULTATION")
        return jsonify(response)

    if reservation is None:
        raise RuntimeError("Reservation lock exited without a reservation result.")

    notify_hospital_messages(hospital_config, "BED_UPDATE", {
        "hospital_id": hospital_id,
        "bed_type": bed_type,
        "available_beds": reservation.get("remaining_beds", 0),
    })
    response = {
        "success": True,
        "status": "COMPLETED",
        "request_id": request_id,
        "patient_id": patient_id,
        "bed_type": bed_type,
        "priority": emergency_request["priority"],
        "selected_hospital": {
            "hospital_id": hospital_id,
            "hospital_name": hospital_config["hospital_name"],
            "remaining_beds": reservation.get("remaining_beds", 0),
        },
        "message": f"Bed allocated successfully at {hospital_config['hospital_name']}.",
    }
    MONGO_STORE.complete_emergency_request(request_id, response, status="COMPLETED")
    return jsonify(response)


@app.route("/api/emergency/cancel", methods=["POST"])
def api_cancel_emergency_request():
    """Cancel a request awaiting consultation without reserving a bed."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400
    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not request_id.strip():
        return jsonify({"success": False, "message": "Request ID is required."}), 400
    request_id = request_id.strip()

    cancelled_request = MONGO_STORE.cancel_emergency_request(request_id)
    if cancelled_request is not None:
        return jsonify({"success": True, "message": "Request cancelled. No bed was reserved."})

    existing_request = MONGO_STORE.get_emergency_request(request_id)
    if existing_request and existing_request.get("status") == "CANCELLED":
        return jsonify({"success": True, "message": "Request cancelled. No bed was reserved."})
    if existing_request:
        return jsonify({
            "success": False,
            "message": "This request can no longer be cancelled.",
        }), 409
    return jsonify({"success": False, "message": "Emergency request not found or expired."}), 404


@app.route("/api/messages", methods=["POST"])
def api_messages():
    """A simple HTTP endpoint for message-based communication between hospital nodes."""
    data = request.get_json(silent=True) or {}
    message_type = data.get("message_type", "UNKNOWN")
    add_log(f"Message received | Type: {message_type} | Data: {data}")
    return jsonify({"status": "received", "message_type": message_type})


@app.route("/api/hospitals/<hospital_id>/patients", methods=["GET"])
def api_hospital_patient_history(hospital_id):
    """List confirmed placements belonging to the requested hospital."""
    hospital_config = next(
        (hospital for hospital in get_hospital_config() if hospital["hospital_id"] == hospital_id),
        None,
    )
    if hospital_config is None:
        return jsonify({"success": False, "message": "Hospital not found."}), 404
    return jsonify({
        "success": True,
        "patients": MONGO_STORE.hospital_patient_history(hospital_id),
    })


@app.route("/api/bed/deallocate", methods=["POST"])
def api_deallocate_bed():
    """Discharge a confirmed patient and release that placement's bed."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "message": "A JSON object is required."}), 400
    hospital_id = payload.get("hospital_id")
    request_id = payload.get("request_id")
    if not isinstance(hospital_id, str) or not hospital_id.strip():
        return jsonify({"success": False, "message": "Hospital ID is required."}), 400
    if not isinstance(request_id, str) or not request_id.strip():
        return jsonify({"success": False, "message": "Request ID is required."}), 400
    hospital_id = hospital_id.strip()
    request_id = request_id.strip()
    hospital_config = next((h for h in get_hospital_config() if h["hospital_id"] == hospital_id), None)
    if hospital_config is None:
        return jsonify({"success": False, "message": "Hospital not found."}), 404

    existing = MONGO_STORE.get_emergency_request(request_id)
    if not existing or existing.get("selected_hospital", {}).get("hospital_id") != hospital_id:
        return jsonify({"success": False, "message": "Patient placement not found for this hospital."}), 404
    if existing.get("status") == "DISCHARGED":
        return jsonify({"success": True, "message": "This patient has already been discharged."})

    claimed = MONGO_STORE.claim_patient_discharge(request_id, hospital_id)
    if claimed is None:
        return jsonify({
            "success": False,
            "message": "This patient placement is already being discharged or is no longer active.",
        }), 409

    bed_type = claimed.get("bed_type")
    if bed_type not in ["ICU", "Emergency", "General"]:
        MONGO_STORE.finish_patient_discharge(request_id, "COMPLETED")
        return jsonify({"success": False, "message": "Patient placement has an invalid bed type."}), 409

    result = release_bed_at_hospital(hospital_config, bed_type)
    if result.get("success"):
        MONGO_STORE.finish_patient_discharge(request_id, "DISCHARGED")
        return jsonify({
            "success": True,
            "message": f"{claimed.get('patient_name') or claimed.get('patient_id')} was discharged from {hospital_config['hospital_name']}.",
            "hospital_id": hospital_id,
            "bed_type": bed_type,
            "request_id": request_id,
        })

    MONGO_STORE.finish_patient_discharge(request_id, "COMPLETED")
    return jsonify({
        "success": False,
        "message": result.get("message", "Deallocation failed."),
    }), 502


if __name__ == "__main__":
    MONGO_STORE.wait_until_ready()
    coordinator_beacon_stop = threading.Event()

    def make_coordinator_beacon():
        return {
            "coordinator_id": "COORDINATOR",
            "epoch": 0,
            "lamport_timestamp": COORDINATOR_CLOCK.tick(),
        }

    def send_coordinator_beacon(hospital, beacon):
        try:
            return call_hospital_rpc(
                hospital,
                "CoordinatorHeartbeat",
                **beacon,
            )
        except (grpc.RpcError, OSError) as exc:
            return {"accepted": False, "message": str(exc)}

    coordinator_beacon_thread = threading.Thread(
        target=send_coordinator_heartbeats,
        args=(
            get_hospital_config(),
            make_coordinator_beacon,
            send_coordinator_beacon,
            coordinator_beacon_stop,
        ),
        name="coordinator-heartbeat-sender",
        daemon=True,
    )
    coordinator_beacon_thread.start()
    try:
        app.run(host="0.0.0.0", port=5000, debug=False)
    finally:
        coordinator_beacon_stop.set()
        coordinator_beacon_thread.join(timeout=3)
