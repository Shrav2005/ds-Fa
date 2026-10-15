import os
import time
from datetime import datetime, timezone

from pymongo import MongoClient, ReturnDocument


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "distributed_hospital")


def _timestamp_isoformat(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


class MongoStore:
    """Small persistence boundary shared by hospital nodes and the coordinator."""

    def __init__(self, uri=MONGO_URI, database=MONGO_DATABASE):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.database = self.client[database]
        self.hospitals = self.database.hospitals
        self.reservations = self.database.reservations
        self.events = self.database.events
        self.logs = self.database.logs
        self.emergency_requests = self.database.emergency_requests
        self.logical_clocks = self.database.logical_clocks
        self.global_snapshots = self.database.global_snapshots

    def wait_until_ready(self, retries=30, delay=2):
        """Wait for MongoDB to accept connections during container startup."""
        for attempt in range(retries):
            try:
                self.client.admin.command("ping")
                return
            except Exception:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)

    def load_or_create_hospital(self, hospital_id, hospital_name, beds):
        """Create initial state once, then always return the persisted state."""
        self.hospitals.update_one(
            {"_id": hospital_id},
            {
                "$setOnInsert": {
                    "hospital_id": hospital_id,
                    "hospital_name": hospital_name,
                    "status": "ONLINE",
                    "beds": dict(beds),
                    "created_at": datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )
        return self.hospitals.find_one({"_id": hospital_id})

    def get_hospital(self, hospital_id):
        return self.hospitals.find_one({"_id": hospital_id})

    def next_lamport_timestamp(self, node_id, observed_timestamp):
        """Atomically advance a node's persistent clock beyond an observed message."""
        clock = self.logical_clocks.find_one_and_update(
            {"_id": node_id},
            [{
                "$set": {
                    "clock": {
                        "$add": [
                            {"$max": [{"$ifNull": ["$clock", 0]}, observed_timestamp]},
                            1,
                        ],
                    },
                },
            }],
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(clock["clock"])

    def next_vector_clock(self, node_id, observed_vector):
        """Atomically merge an observed vector and advance this node's component."""
        updates = {}
        for component, counter in observed_vector.items():
            current = {"$ifNull": [f"$vector.{component}", 0]}
            merged = {"$max": [current, counter]}
            updates[component] = (
                {"$add": [merged, 1]} if component == node_id else merged
            )
        clock = self.logical_clocks.find_one_and_update(
            {"_id": f"VECTOR:{node_id}"},
            [{
                "$set": {
                    "vector": {
                        "$mergeObjects": [
                            {"$ifNull": ["$vector", {}]},
                            updates,
                        ],
                    },
                },
            }],
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return {key: int(value) for key, value in clock["vector"].items()}

    def reserve_bed(
        self,
        hospital_id,
        hospital_name,
        bed_type,
        patient_id,
        lamport_timestamp=0,
        vector_clock=None,
        priority="Normal",
    ):
        """Atomically decrement a bed and record the reservation."""
        hospital = self.hospitals.find_one_and_update(
            {"_id": hospital_id, f"beds.{bed_type}": {"$gt": 0}},
            {"$inc": {f"beds.{bed_type}": -1}},
            return_document=ReturnDocument.AFTER,
        )
        if hospital is None:
            return None

        self.reservations.insert_one({
            "hospital_id": hospital_id,
            "hospital_name": hospital_name,
            "patient_id": patient_id,
            "bed_type": bed_type,
            "action": "RESERVED",
            "priority": priority,
            "lamport_timestamp": lamport_timestamp,
            "vector_clock": dict(vector_clock or {}),
            "node_id": hospital_id,
            "created_at": datetime.now(timezone.utc),
        })
        return hospital["beds"][bed_type]

    def release_bed(
        self,
        hospital_id,
        hospital_name,
        bed_type,
        lamport_timestamp=0,
        vector_clock=None,
    ):
        """Atomically increment a bed and record the release."""
        hospital = self.hospitals.find_one_and_update(
            {"_id": hospital_id},
            {"$inc": {f"beds.{bed_type}": 1}},
            return_document=ReturnDocument.AFTER,
        )
        if hospital is None:
            return None

        self.reservations.insert_one({
            "hospital_id": hospital_id,
            "hospital_name": hospital_name,
            "bed_type": bed_type,
            "action": "RELEASED",
            "lamport_timestamp": lamport_timestamp,
            "vector_clock": dict(vector_clock or {}),
            "node_id": hospital_id,
            "created_at": datetime.now(timezone.utc),
        })
        return hospital["beds"][bed_type]

    def append_event(self, hospital_id, message):
        self.events.insert_one({
            "hospital_id": hospital_id,
            "message": message,
            "created_at": datetime.now(timezone.utc),
        })

    def append_log(self, source, message, lamport_timestamp=0, vector_clock=None):
        self.logs.insert_one({
            "source": source,
            "message": message,
            "lamport_timestamp": lamport_timestamp,
            "vector_clock": dict(vector_clock or {}),
            "node_id": source,
            "created_at": datetime.now(timezone.utc),
        })

    def latest_logs(self, limit=30):
        logs = list(self.logs.find().sort("created_at", -1).limit(limit))
        reservations = list(self.reservations.find().sort("created_at", -1).limit(limit))
        entries = []
        for item in logs:
            entries.append({
                "timestamp": item["created_at"].astimezone().strftime("%H:%M:%S"),
                "message": item["message"],
                "lamport_timestamp": item.get("lamport_timestamp"),
                "node_id": item.get("node_id", item.get("source", "COORDINATOR")),
                "vector_clock": item.get("vector_clock"),
                "created_at": item["created_at"],
            })
        for item in reservations:
            patient = f" for patient {item['patient_id']}" if item.get("patient_id") else ""
            priority = f" · {item['priority']} priority" if item.get("priority") else ""
            entries.append({
                "timestamp": item["created_at"].astimezone().strftime("%H:%M:%S"),
                "message": (
                    f"{item['action']} {item['bed_type']} bed{patient} at "
                    f"{item['hospital_name']}{priority}"
                ),
                "lamport_timestamp": item.get("lamport_timestamp"),
                "node_id": item.get("node_id", item.get("hospital_id", "HOSPITAL")),
                "vector_clock": item.get("vector_clock"),
                "created_at": item["created_at"],
            })
        entries.sort(key=lambda item: (
            item["lamport_timestamp"] is not None,
            item["lamport_timestamp"] or 0,
            item["node_id"],
            item["created_at"],
        ))
        return [
            {
                key: item[key]
                for key in ("timestamp", "message", "lamport_timestamp", "node_id", "vector_clock")
            }
            for item in entries[-limit:]
        ]

    def create_global_snapshot(self, snapshot):
        """Persist a coordinator-observed view of hospital local states."""
        snapshot = dict(snapshot)
        snapshot["captured_at"] = datetime.now(timezone.utc)
        self.global_snapshots.insert_one(snapshot)
        snapshot.pop("_id", None)
        snapshot["captured_at"] = snapshot["captured_at"].isoformat()
        return snapshot

    def latest_global_snapshots(self, limit=5):
        """Return recent snapshots without exposing MongoDB-specific document fields."""
        documents = self.global_snapshots.find().sort("captured_at", -1).limit(limit)
        snapshots = []
        for document in documents:
            snapshots.append({
                "snapshot_id": document["snapshot_id"],
                "captured_at": document["captured_at"].isoformat(),
                "coordinator_lamport_timestamp": document["coordinator_lamport_timestamp"],
                "consistency": document["consistency"],
                "hospitals": document["hospitals"],
            })
        return snapshots

    def analytics_records(self):
        """Return de-identified request and reservation dimensions for batch analytics."""
        records = []
        request_projection = {
            "_id": 0,
            "bed_type": 1,
            "selected_hospital.hospital_name": 1,
            "created_at": 1,
        }
        for document in self.emergency_requests.find({}, request_projection):
            hospital = document.get("selected_hospital") or {}
            created_at = document.get("created_at")
            records.append({
                "record_type": "request",
                "bed_type": document.get("bed_type"),
                "hospital_name": hospital.get("hospital_name"),
                "created_at": _timestamp_isoformat(created_at),
            })

        reservation_projection = {
            "_id": 0,
            "hospital_name": 1,
            "bed_type": 1,
            "created_at": 1,
        }
        for document in self.reservations.find({"action": "RESERVED"}, reservation_projection):
            created_at = document.get("created_at")
            records.append({
                "record_type": "reservation",
                "hospital_name": document.get("hospital_name"),
                "bed_type": document.get("bed_type"),
                "created_at": _timestamp_isoformat(created_at),
            })
        return records

    def create_emergency_request(self, request_data):
        request_data = dict(request_data)
        request_data["created_at"] = datetime.now(timezone.utc)
        return self.emergency_requests.insert_one(request_data).inserted_id

    def complete_emergency_request(self, request_id, result, status="COMPLETED"):
        query = {"request_id": request_id}
        if not isinstance(request_id, str):
            query = {"_id": request_id}
        timestamp_field = "completed_at" if status == "COMPLETED" else "updated_at"
        self.emergency_requests.update_one(
            query,
            {
                "$set": {
                    "status": status,
                    "result": result,
                    timestamp_field: datetime.now(timezone.utc),
                },
            },
        )

    def get_emergency_request(self, request_id):
        return self.emergency_requests.find_one({"request_id": request_id})

    def hospital_patient_history(self, hospital_id):
        """Return confirmed placements for one hospital without MongoDB fields."""
        documents = self.emergency_requests.find({
            "selected_hospital.hospital_id": hospital_id,
            "status": {"$in": ["COMPLETED", "DISCHARGING", "DISCHARGED"]},
        }).sort("created_at", -1)
        return [{
            "request_id": document["request_id"],
            "patient_id": document.get("patient_id"),
            "patient_name": document.get("patient_name"),
            "bed_type": document.get("bed_type"),
            "priority": document.get("priority"),
            "status": document.get("status"),
            "created_at": _timestamp_isoformat(document.get("created_at")),
        } for document in documents]

    def claim_patient_discharge(self, request_id, hospital_id):
        """Claim an active placement for discharge at its selected hospital."""
        return self.emergency_requests.find_one_and_update(
            {
                "request_id": request_id,
                "selected_hospital.hospital_id": hospital_id,
                "status": "COMPLETED",
            },
            {"$set": {"status": "DISCHARGING"}},
            return_document=ReturnDocument.BEFORE,
        )

    def finish_patient_discharge(self, request_id, status):
        if status not in ("COMPLETED", "DISCHARGED"):
            raise ValueError("Discharge status must be COMPLETED or DISCHARGED.")
        self.emergency_requests.update_one(
            {"request_id": request_id, "status": "DISCHARGING"},
            {"$set": {
                "status": status,
                "updated_at": datetime.now(timezone.utc),
            }},
        )

    def claim_emergency_request(self, request_id):
        return self.emergency_requests.find_one_and_update(
            {"request_id": request_id, "status": "AWAITING_CONSULTATION"},
            {"$set": {"status": "RESERVING"}},
            return_document=ReturnDocument.BEFORE,
        )

    def cancel_emergency_request(self, request_id):
        return self.emergency_requests.find_one_and_update(
            {"request_id": request_id, "status": "AWAITING_CONSULTATION"},
            {
                "$set": {
                    "status": "CANCELLED",
                    "updated_at": datetime.now(timezone.utc),
                },
            },
            return_document=ReturnDocument.BEFORE,
        )