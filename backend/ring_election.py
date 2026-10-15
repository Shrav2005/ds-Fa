"""Ring-election protocol for the configured hospital nodes."""

from election import order_process_ids


def run_ring_election(
    hospital_ids,
    send_election,
    announce_coordinator,
    initiator_id,
    epoch,
):
    """Circulate candidate IDs around the logical ring and announce its winner."""
    ordered_ids = order_process_ids(hospital_ids)
    if not ordered_ids:
        raise ValueError("An election requires at least one configured hospital.")
    if initiator_id not in ordered_ids:
        raise ValueError("The election initiator must be in the configured ring.")
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
        raise ValueError("Election epochs must be positive integers.")

    initiator_index = ordered_ids.index(initiator_id)
    ring_order = ordered_ids[initiator_index + 1:] + ordered_ids[:initiator_index]
    candidates = [initiator_id]
    events = [f"{initiator_id} starts a Ring election (epoch {epoch})."]
    failures = []

    for target_id in ring_order:
        result = send_election(target_id, list(candidates), epoch, initiator_id)

        if not isinstance(result, dict) or result.get("accepted") is not True:
            failures.append({
                "hospital_id": target_id,
                "message": (
                    result.get("message", "The node did not accept the election message.")
                    if isinstance(result, dict)
                    else "The node did not accept the election message."
                ),
            })
            events.append(f"Ring token could not pass {target_id}; it is skipped.")
            continue

        returned_candidates = result.get("candidate_ids")
        if (
            not isinstance(returned_candidates, list)
            or returned_candidates != [*candidates, target_id]
        ):
            failures.append({
                "hospital_id": target_id,
                "message": "The node returned an invalid ring election message.",
            })
            events.append(f"{target_id} returned an invalid election message.")
            continue

        candidates = list(dict.fromkeys(returned_candidates))
        events.append(
            f"Ring token -> {target_id}: candidates {', '.join(candidates)}."
        )

    if initiator_id not in candidates:
        candidates.insert(0, initiator_id)
    leader_id = order_process_ids(candidates)[-1]
    events.append(f"The election message returns; highest candidate {leader_id} is coordinator.")

    acknowledged_nodes = []
    for target_id in candidates:
        response = announce_coordinator(target_id, leader_id, epoch)

        if isinstance(response, dict) and response.get("accepted"):
            acknowledged_nodes.append(target_id)
            events.append(f"{leader_id} -> {target_id}: COORDINATOR announcement accepted.")
        else:
            failures.append({
                "hospital_id": target_id,
                "message": (
                    response.get("message", "The node did not accept the coordinator announcement.")
                    if isinstance(response, dict)
                    else "The node did not accept the coordinator announcement."
                ),
            })

    return {
        "algorithm": "RING",
        "epoch": epoch,
        "initiator_id": initiator_id,
        "leader_id": leader_id,
        "participants": candidates,
        "acknowledged_nodes": acknowledged_nodes,
        "events": events,
        "failures": failures,
    }
