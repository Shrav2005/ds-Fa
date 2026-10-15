"""Wait-for graph construction and edge-chasing deadlock detection."""


def _validate_transactions(transactions):
    if not isinstance(transactions, list):
        raise ValueError("transactions must be a list.")

    validated = {}
    for transaction in transactions:
        if not isinstance(transaction, dict):
            raise ValueError("Each transaction must be an object.")
        transaction_id = transaction.get("transaction_id")
        timestamp = transaction.get("lamport_timestamp")
        holds = transaction.get("holds", [])
        waits_for = transaction.get("waits_for", [])
        if not isinstance(transaction_id, str) or not transaction_id.strip():
            raise ValueError("Each transaction requires a non-empty transaction_id.")
        transaction_id = transaction_id.strip()
        if transaction_id in validated:
            raise ValueError(f"Duplicate transaction_id: {transaction_id}.")
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, int)
            or timestamp < 0
        ):
            raise ValueError(f"{transaction_id} requires a non-negative integer Lamport timestamp.")
        if (
            not isinstance(holds, list)
            or not all(isinstance(resource, str) and resource.strip() for resource in holds)
            or not isinstance(waits_for, list)
            or not all(isinstance(resource, str) and resource.strip() for resource in waits_for)
        ):
            raise ValueError(f"{transaction_id} holds and waits_for must be lists of resource names.")
        validated[transaction_id] = {
            "transaction_id": transaction_id,
            "lamport_timestamp": timestamp,
            "holds": sorted(set(resource.strip() for resource in holds)),
            "waits_for": sorted(set(resource.strip() for resource in waits_for)),
        }
    return validated


def build_wait_for_graph(transactions):
    """Convert resource ownership and waits into transaction-to-transaction edges."""
    owners = {}
    for transaction_id, transaction in transactions.items():
        for resource in transaction["holds"]:
            owners.setdefault(resource, []).append(transaction_id)

    edges = set()
    for transaction_id, transaction in transactions.items():
        for resource in transaction["waits_for"]:
            for owner in owners.get(resource, []):
                if owner != transaction_id:
                    edges.add((transaction_id, owner))
                elif resource in transaction["holds"]:
                    edges.add((transaction_id, owner))

    return [
        {"from": source, "to": target}
        for source, target in sorted(edges)
    ]


def _edge_chasing_cycles(graph):
    """Send CMH-style probes along wait edges and record probes returning home."""
    adjacency = {}
    nodes = set()
    for edge in graph:
        source, target = edge["from"], edge["to"]
        adjacency.setdefault(source, []).append(target)
        nodes.update((source, target))

    probes = []
    cycles = set()
    for initiator in sorted(nodes):
        stack = [(initiator, [initiator])]
        visited = set()
        while stack:
            sender, path = stack.pop()
            for receiver in sorted(adjacency.get(sender, [])):
                probes.append({
                    "initiator": initiator,
                    "sender": sender,
                    "receiver": receiver,
                })
                if receiver == initiator:
                    cycle = path
                    rotations = [
                        tuple(cycle[index:] + cycle[:index])
                        for index in range(len(cycle))
                    ]
                    cycles.add(min(rotations))
                    continue
                if receiver in path or (sender, receiver) in visited:
                    continue
                visited.add((sender, receiver))
                stack.append((receiver, path + [receiver]))
    return probes, [list(cycle) for cycle in sorted(cycles)]


def detect_deadlocks(raw_transactions):
    """Detect wait cycles, choose the youngest victim, and show the resolved graph."""
    transactions = _validate_transactions(raw_transactions)
    graph = build_wait_for_graph(transactions)
    probes, cycles = _edge_chasing_cycles(graph)
    participants = {node for cycle in cycles for node in cycle}
    victim_id = None
    if participants:
        victim_id = max(
            participants,
            key=lambda transaction_id: (
                transactions[transaction_id]["lamport_timestamp"],
                transaction_id,
            ),
        )

    resolved = dict(transactions)
    victim = None
    if victim_id is not None:
        victim = dict(transactions[victim_id])
        victim["status"] = "ABORTED"
        del resolved[victim_id]

    return {
        "algorithm": "CHANDY_MISRA_HAAS_EDGE_CHASING",
        "classification": "DISTRIBUTED_EDGE_CHASING",
        "transactions": [
            {
                **transaction,
                "status": "ABORTED" if transaction_id == victim_id else "WAITING",
            }
            for transaction_id, transaction in sorted(transactions.items())
        ],
        "wait_for_graph": graph,
        "probes": probes,
        "cycles": cycles,
        "deadlock_detected": bool(cycles),
        "victim": victim,
        "resolved_wait_for_graph": build_wait_for_graph(resolved),
    }
