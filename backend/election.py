"""Bully-election protocol orchestration for the configured hospital nodes."""


def order_process_ids(hospital_ids):
    """Sort IDs by their numeric process suffix when present."""
    def process_order(hospital_id):
        suffix_start = len(hospital_id.rstrip("0123456789"))
        prefix = hospital_id[:suffix_start]
        suffix = hospital_id[suffix_start:]
        return (prefix, int(suffix)) if suffix else (hospital_id, 0)

    return sorted(set(hospital_ids), key=process_order)


def run_bully_election(
    hospital_ids,
    send_election,
    announce_coordinator,
    initiator_id=None,
    epoch=1,
):
    """Run a Bully election over live hospital RPCs and return its message trace."""
    ordered_ids = order_process_ids(hospital_ids)
    if not ordered_ids:
        raise ValueError("An election requires at least one live hospital.")
    if initiator_id is None:
        initiator_id = ordered_ids[0]
    if initiator_id not in ordered_ids:
        raise ValueError("The election initiator must be a live hospital.")

    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
        raise ValueError("Election epochs must be positive integers.")
    initiator_index = ordered_ids.index(initiator_id)
    candidates = ordered_ids[initiator_index:]
    events = [f"{initiator_id} starts a Bully election (epoch {epoch})."]
    leader_id = None

    for candidate in candidates:
        candidate_index = ordered_ids.index(candidate)
        higher_ids = ordered_ids[candidate_index + 1:]
        replies = []
        for higher_id in higher_ids:
            response = send_election(candidate, higher_id, epoch)
            if response.get("answer"):
                replies.append(higher_id)
                events.append(f"{candidate} -> {higher_id}: ELECTION; {higher_id} replies OK.")

        if replies:
            events.append(f"{candidate} waits; higher-ID process(es) {', '.join(replies)} continue the election.")
            continue

        leader_id = candidate
        events.append(f"{candidate} received no higher-ID replies and becomes coordinator.")
        break

    if leader_id is None:
        raise RuntimeError("The Bully election ended without a coordinator.")

    acknowledged_nodes = []
    for node_id in ordered_ids:
        response = announce_coordinator(node_id, leader_id, epoch)
        if response.get("accepted"):
            acknowledged_nodes.append(node_id)
            events.append(f"{leader_id} -> {node_id}: COORDINATOR announcement accepted.")

    return {
        "algorithm": "BULLY",
        "epoch": epoch,
        "initiator_id": initiator_id,
        "leader_id": leader_id,
        "participants": ordered_ids,
        "acknowledged_nodes": acknowledged_nodes,
        "events": events,
    }
