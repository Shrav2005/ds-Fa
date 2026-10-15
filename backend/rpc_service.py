from threading import Lock
import time

from beacon_protocol import COORDINATOR_BEACON_TIMEOUT_SECONDS
from clock_sync import ClockAdjustment
from election import order_process_ids
from lamport_clock import LamportClock
from vector_clock import VectorClock

VALID_BED_TYPES = ["ICU", "Emergency", "General"]


def build_hospital_service(hospital_state, host, port, mongo_store=None, ring_members=None):
    """Create the local operations exposed by a hospital's gRPC service."""
    reservation_lock = Lock()
    election_lock = Lock()
    election_state = {"leader_id": None, "epoch": 0, "algorithm": None}
    coordinator_last_seen = time.monotonic()
    coordinator_beacon_id = None
    hospital_id = hospital_state["hospital_id"]
    ring_members = set(ring_members or [hospital_id])
    clock = LamportClock(
        hospital_id,
        mongo_store.next_lamport_timestamp if mongo_store else None,
    )
    vector_clock = VectorClock(
        hospital_id,
        mongo_store.next_vector_clock if mongo_store else None,
    )
    clock_adjustment = ClockAdjustment()

    def get_hospital_status(lamport_timestamp=0, vector_timestamp=None):
        """Return the hospital identity and current local bed state."""
        hospital_received_ms = clock_adjustment.now_ms()
        event_clock = clock.receive(lamport_timestamp)
        event_vector = vector_clock.receive(
            {} if vector_timestamp is None else vector_timestamp
        )
        persisted = mongo_store.get_hospital(hospital_state["hospital_id"]) if mongo_store else None
        beds = persisted.get("beds", {}) if persisted else hospital_state["beds"]
        hospital_sent_ms = clock_adjustment.now_ms()
        return {
            "hospital_id": hospital_state["hospital_id"],
            "hospital_name": hospital_state["hospital_name"],
            "status": hospital_state["status"],
            "beds": dict(beds),
            "lamport_timestamp": event_clock,
            "vector_clock": event_vector,
            "clock_adjustment_ms": clock_adjustment.get_ms(),
            "election_state": get_election_state(),
            "coordinator_heartbeat": get_coordinator_heartbeat_status(),
            "clock_sample": {
                "hospital_received_ms": hospital_received_ms,
                "hospital_sent_ms": hospital_sent_ms,
            },
        }

    def adjust_clock(clock_adjustment_ms):
        """Set the simulated wall-clock adjustment supplied by Berkeley sync."""
        adjustment = clock_adjustment.set_ms(clock_adjustment_ms)
        return {
            "success": True,
            "hospital_id": hospital_id,
            "clock_adjustment_ms": adjustment,
        }

    def get_bed_availability(lamport_timestamp=0, vector_timestamp=None):
        """Expose the current local bed counts to other nodes through RPC."""
        status = get_hospital_status(lamport_timestamp, vector_timestamp)
        return {
            "beds": status["beds"],
            "lamport_timestamp": status["lamport_timestamp"],
            "vector_clock": status["vector_clock"],
        }

    def create_beacon():
        """Create a heartbeat containing the hospital's identity and logical clock."""
        return {
            "hospital_id": hospital_id,
            "hospital_name": hospital_state["hospital_name"],
            "lamport_timestamp": clock.tick(),
        }

    def ping_hospital():
        """Simple connectivity check used by the coordinator before selection."""
        return True

    def election_message(caller_id, epoch):
        """Acknowledge an election request from a lower-priority process."""
        if (
            not isinstance(caller_id, str)
            or not caller_id
            or isinstance(epoch, bool)
            or not isinstance(epoch, (int, float))
            or isinstance(epoch, float) and not epoch.is_integer()
        ):
            return {"answer": False, "hospital_id": hospital_id}
        epoch = int(epoch)
        if epoch < 1:
            return {"answer": False, "hospital_id": hospital_id}
        with election_lock:
            if epoch > election_state["epoch"]:
                election_state.update({
                    "epoch": epoch,
                    "leader_id": None,
                    "algorithm": None,
                })
        return {"answer": True, "hospital_id": hospital_id}

    def ring_election_message(candidate_ids, initiator_id, epoch):
        """Append this hospital to a candidate token circulating around the ring."""
        if (
            not isinstance(candidate_ids, list)
            or any(not isinstance(candidate, str) or not candidate for candidate in candidate_ids)
            or not candidate_ids
            or len(set(candidate_ids)) != len(candidate_ids)
            or any(candidate not in ring_members for candidate in candidate_ids)
            or not isinstance(initiator_id, str)
            or not initiator_id
            or initiator_id not in ring_members
            or hospital_id in candidate_ids
            or candidate_ids[0] != initiator_id
            or isinstance(epoch, bool)
            or not isinstance(epoch, (int, float))
            or isinstance(epoch, float) and not epoch.is_integer()
        ):
            return {"accepted": False, "hospital_id": hospital_id}
        epoch = int(epoch)
        if epoch < 1:
            return {"accepted": False, "hospital_id": hospital_id}
        with election_lock:
            if epoch < election_state["epoch"]:
                return {
                    "accepted": False,
                    "hospital_id": hospital_id,
                    "epoch": election_state["epoch"],
                }
            if epoch > election_state["epoch"]:
                election_state.update({
                    "epoch": epoch,
                    "leader_id": None,
                    "algorithm": None,
                })
        return {
            "accepted": True,
            "hospital_id": hospital_id,
            "candidate_ids": [*candidate_ids, hospital_id],
        }

    def announce_coordinator(leader_id, epoch, algorithm="BULLY"):
        """Record a coordinator announcement if it is not from an older election."""
        if (
            not isinstance(leader_id, str)
            or not leader_id
            or algorithm not in ("BULLY", "RING")
            or isinstance(epoch, bool)
            or not isinstance(epoch, (int, float))
            or isinstance(epoch, float) and not epoch.is_integer()
        ):
            return {"accepted": False, "hospital_id": hospital_id}
        epoch = int(epoch)
        if epoch < 1:
            return {"accepted": False, "hospital_id": hospital_id}
        with election_lock:
            if epoch < election_state["epoch"]:
                return {
                    "accepted": False,
                    "hospital_id": hospital_id,
                    "epoch": election_state["epoch"],
                }
            current_leader = election_state["leader_id"]
            if (
                epoch == election_state["epoch"]
                and current_leader
                and order_process_ids([current_leader, leader_id])[-1] != leader_id
            ):
                return {
                    "accepted": False,
                    "hospital_id": hospital_id,
                    "epoch": election_state["epoch"],
                    "leader_id": current_leader,
                }
            election_state.update({
                "leader_id": leader_id,
                "epoch": epoch,
                "algorithm": algorithm,
            })
        return {
            "accepted": True,
            "hospital_id": hospital_id,
            "epoch": epoch,
            "leader_id": leader_id,
            "algorithm": algorithm,
        }

    def receive_coordinator_heartbeat(coordinator_id, epoch, lamport_timestamp=0):
        """Record a heartbeat only from the active coordinator epoch."""
        nonlocal coordinator_last_seen, coordinator_beacon_id
        if (
            not isinstance(coordinator_id, str)
            or not coordinator_id
            or isinstance(epoch, bool)
            or not isinstance(epoch, (int, float))
            or isinstance(epoch, float) and not epoch.is_integer()
        ):
            return {"accepted": False, "hospital_id": hospital_id}
        epoch = int(epoch)
        if epoch < 0:
            return {"accepted": False, "hospital_id": hospital_id}
        clock.receive(lamport_timestamp)

        with election_lock:
            if epoch < election_state["epoch"]:
                return {
                    "accepted": False,
                    "hospital_id": hospital_id,
                    "epoch": election_state["epoch"],
                }
            active_leader = election_state["leader_id"]
            if epoch == 0 and coordinator_id != "COORDINATOR":
                return {"accepted": False, "hospital_id": hospital_id}
            if epoch > 0 and coordinator_id != active_leader:
                return {"accepted": False, "hospital_id": hospital_id}
            coordinator_last_seen = time.monotonic()
            coordinator_beacon_id = coordinator_id
        return {"accepted": True, "hospital_id": hospital_id, "epoch": epoch}

    def get_coordinator_heartbeat_status():
        age = max(0, int(time.monotonic() - coordinator_last_seen))
        with election_lock:
            coordinator_id = coordinator_beacon_id
            leader_id = election_state["leader_id"]
            epoch = election_state["epoch"]
        if age > COORDINATOR_BEACON_TIMEOUT_SECONDS:
            beacon_status = "STALE"
        else:
            beacon_status = "ONLINE" if coordinator_id else "WAITING"
        return {
            "coordinator_id": coordinator_id,
            "beacon_status": beacon_status,
            "heartbeat_age_seconds": age,
            "leader_id": leader_id,
            "epoch": epoch,
        }

    def create_coordinator_heartbeat():
        """Create a heartbeat only when this hospital is the elected coordinator."""
        state = get_election_state()
        if state["leader_id"] != hospital_id:
            return None
        return {
            "coordinator_id": hospital_id,
            "epoch": state["epoch"],
            "lamport_timestamp": clock.tick(),
        }

    def get_election_state():
        with election_lock:
            return {
                "hospital_id": hospital_id,
                "leader_id": election_state["leader_id"],
                "epoch": election_state["epoch"],
                "algorithm": election_state["algorithm"],
            }

    def reserve_bed(
        bed_type,
        patient_id,
        lamport_timestamp=0,
        vector_timestamp=None,
        priority="Normal",
    ):
        """Decrease the requested bed count only if a bed is available."""
        if not patient_id:
            return {"success": False, "message": "Patient ID is missing."}

        if bed_type not in VALID_BED_TYPES:
            return {"success": False, "message": "Invalid bed type."}
        if priority not in ("Critical", "High", "Normal"):
            return {"success": False, "message": "Invalid reservation priority."}

        with reservation_lock:
            event_clock = clock.receive(lamport_timestamp)
            event_vector = vector_clock.receive(
                {} if vector_timestamp is None else vector_timestamp
            )
            if mongo_store:
                reservation_args = (
                    hospital_id,
                    hospital_state["hospital_name"],
                    bed_type,
                    patient_id,
                    event_clock,
                    event_vector,
                )
                remaining_beds = mongo_store.reserve_bed(
                    *reservation_args,
                    **({"priority": priority} if priority != "Normal" else {}),
                )
                if remaining_beds is None:
                    return {
                        "success": False,
                        "message": f"No {bed_type} beds available at {hospital_state['hospital_name']}.",
                        "lamport_timestamp": event_clock,
                        "node_id": hospital_id,
                        "vector_clock": event_vector,
                    }
                hospital_state["beds"][bed_type] = remaining_beds
                return {
                    "success": True,
                    "message": f"Reservation successful for patient {patient_id}.",
                    "remaining_beds": remaining_beds,
                    "priority": priority,
                    "lamport_timestamp": event_clock,
                    "node_id": hospital_id,
                    "vector_clock": event_vector,
                }

            current_count = hospital_state["beds"].get(bed_type, 0)
            if current_count <= 0:
                return {
                    "success": False,
                    "message": f"No {bed_type} beds available at {hospital_state['hospital_name']}.",
                    "lamport_timestamp": event_clock,
                    "node_id": hospital_id,
                    "vector_clock": event_vector,
                }

            hospital_state["beds"][bed_type] = current_count - 1
            return {
                "success": True,
                "message": f"Reservation successful for patient {patient_id}.",
                "remaining_beds": hospital_state["beds"][bed_type],
                "lamport_timestamp": event_clock,
                "node_id": hospital_id,
                "vector_clock": event_vector,
            }

    def release_bed(bed_type, lamport_timestamp=0, vector_timestamp=None):
        """Return one bed to the hospital when a patient is discharged."""
        if bed_type not in VALID_BED_TYPES:
            return {"success": False, "message": "Invalid bed type."}

        with reservation_lock:
            event_clock = clock.receive(lamport_timestamp)
            event_vector = vector_clock.receive(
                {} if vector_timestamp is None else vector_timestamp
            )
            if mongo_store:
                remaining_beds = mongo_store.release_bed(
                    hospital_id,
                    hospital_state["hospital_name"],
                    bed_type,
                    event_clock,
                    event_vector,
                )
                if remaining_beds is None:
                    return {
                        "success": False,
                        "message": "Hospital state was not found.",
                        "lamport_timestamp": event_clock,
                        "node_id": hospital_id,
                        "vector_clock": event_vector,
                    }
                hospital_state["beds"][bed_type] = remaining_beds
                return {
                    "success": True,
                    "message": f"Released one {bed_type} bed.",
                    "lamport_timestamp": event_clock,
                    "node_id": hospital_id,
                    "vector_clock": event_vector,
                }

            hospital_state["beds"][bed_type] = hospital_state["beds"].get(bed_type, 0) + 1
            return {
                "success": True,
                "message": f"Released one {bed_type} bed.",
                "lamport_timestamp": event_clock,
                "node_id": hospital_id,
                "vector_clock": event_vector,
            }

    def get_status_info():
        """Used by the coordinator to gather a compact hospital state."""
        return get_hospital_status()

    return {
        "host": host,
        "port": port,
        "functions": {
            "ping_hospital": ping_hospital,
            "get_hospital_status": get_hospital_status,
            "adjust_clock": adjust_clock,
            "get_bed_availability": get_bed_availability,
            "create_beacon": create_beacon,
            "election_message": election_message,
            "ring_election_message": ring_election_message,
            "announce_coordinator": announce_coordinator,
            "get_election_state": get_election_state,
            "receive_coordinator_heartbeat": receive_coordinator_heartbeat,
            "get_coordinator_heartbeat_status": get_coordinator_heartbeat_status,
            "create_coordinator_heartbeat": create_coordinator_heartbeat,
            "reserve_bed": reserve_bed,
            "release_bed": release_bed,
            "get_status_info": get_status_info,
        },
    }
