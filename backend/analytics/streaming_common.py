"""Shared map and reduce operations for local reports and Hadoop Streaming."""

from collections import Counter
from datetime import datetime, timezone


def map_record(record):
    """Emit category/dimension counters without exporting patient identifiers."""
    record_type = record.get("record_type")
    emissions = []
    if record_type == "request":
        bed_type = record.get("bed_type")
        if bed_type:
            emissions.append((f"requests_by_bed_type|{bed_type}", 1))
        hospital_name = record.get("hospital_name")
        if hospital_name:
            emissions.append((f"requests_by_hospital|{hospital_name}", 1))
        created_at = record.get("created_at")
        if created_at:
            timestamp = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise ValueError("Request analytics timestamps must include a timezone.")
            timestamp = timestamp.astimezone(timezone.utc)
            emissions.append((f"requests_by_hour|{timestamp.hour:02d}:00", 1))
    elif record_type == "reservation":
        hospital_name = record.get("hospital_name")
        if hospital_name:
            emissions.append((f"reservations_by_hospital|{hospital_name}", 1))
        bed_type = record.get("bed_type")
        if bed_type:
            emissions.append((f"reservations_by_bed_type|{bed_type}", 1))
        emissions.append(("total_reservations", 1))
    else:
        raise ValueError(f"Unsupported analytics record type: {record_type!r}.")
    return emissions


def aggregate_records(records):
    """Aggregate the same mapper output locally for the dashboard report."""
    counts = Counter()
    for record in records:
        counts.update(dict(map_record(record)))
    summary = {
        "requests_by_bed_type": {},
        "requests_by_hospital": {},
        "requests_by_hour": {},
        "reservations_by_hospital": {},
        "reservations_by_bed_type": {},
        "total_reservations": counts.pop("total_reservations", 0),
    }
    for key, value in sorted(counts.items()):
        category, dimension = key.split("|", 1)
        summary[category][dimension] = value
    summary["total_requests"] = sum(summary["requests_by_bed_type"].values())
    return summary
