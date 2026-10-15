# Distributed Hospital Emergency & Bed Allocation System

This project demonstrates a simple distributed system using gRPC, RabbitMQ, and MongoDB between independent hospital nodes. It is built for learning and viva demonstration, not for production use.

## Main idea
The coordinator is not allowed to directly modify hospital bed state. It asks each hospital for local data using gRPC and reserves a bed only after a request-specific consultation and a fresh hospital availability check.

## Persistence
MongoDB stores hospital documents, current bed counts, reservation and release history, RabbitMQ events, coordinator communication logs, and emergency request records. Each hospital is initialized from its CLI defaults only when its MongoDB document does not exist. After a restart, the node reloads the existing document instead of resetting its beds.

The Docker Compose file uses the named `mongodb_data` volume, so data survives container restarts. Remove that volume only when you intentionally want a fresh demo database.

## Distributed Systems concepts shown
- RPC communication between coordinator and hospitals using gRPC deadlines
- Lamport logical clocks carried across coordinator/hospital reservation RPCs
- Causally ordered reservation and release records with persistent per-node clocks
- Message-oriented communication through durable RabbitMQ events
- MongoDB persistence for hospital and coordinator data
- Fault tolerance when a hospital goes offline
- Central coordination with independent hospital nodes
- Local locking inside a hospital process for safe reservation
- Docker virtualization for running the services in containers

Each process starts its Lamport clock at zero, advances it before local events and sends, and updates it to `max(local, received) + 1` when receiving a timestamped message. The coordinator sends its logical timestamp with reserve/release requests. Each hospital merges it with its own persisted Lamport clock, stamps the resulting bed event, and returns that timestamp. The coordinator merges the reply before its next event. The fair reservation mutex orders requests by Lamport timestamp; Communication Logs show each event's `L<timestamp>` and node. These clocks preserve happened-before order but cannot identify every concurrent event and do not synchronize physical clocks.

## Vector clocks
Hospital status, availability, reservation, and release RPCs exchange vector timestamps in addition to Lamport timestamps. Each node merges the received components and increments its own; MongoDB atomically persists per-node vector state, and reservation/release records retain the event vector. Communication Logs show `V{node:counter,...}`. Vector-clock comparison can identify concurrent events where Lamport ordering alone cannot. Vector clocks are only exchanged on these coordinator/hospital RPCs; they do not claim to cover independent browser or RabbitMQ events.

## Clock synchronization estimates and Berkeley synchronization
Hospital status replies include receive/send wall-clock timestamps. The coordinator applies the four-timestamp NTP offset/delay equations and reports estimated hospital-minus-coordinator skew and round-trip time in the dashboard. Select **Run Berkeley Synchronization** to average the coordinator clock with online hospitals that have usable offset estimates, then tell each participant how far to adjust toward that mean. The dashboard reports per-hospital adjustments and partial failures. Adjustments affect simulated clock readings in process memory only: they never modify the host operating-system clocks and are lost on restart. Estimates assume roughly symmetric network delay and cannot prevent later clock drift.

## Global state snapshots
Use **Capture Global State** on the coordinator dashboard to store the configured hospitals' observed status, bed counts, and local Lamport clocks. Each hospital status query carries the coordinator clock and returns the hospital clock, so the collection observes causal message ordering. This prototype queries nodes sequentially and does not record in-flight RPC/channel messages; snapshots are best-effort views, not atomic Chandy–Lamport consistent cuts.

## Hospital and coordinator heartbeat beacons
Each hospital sends an HTTP liveness beacon to `COORDINATOR_URL` every two seconds; a hospital beacon older than eight seconds is stale and the coordinator excludes that node from allocation. The coordinator also sends gRPC heartbeats to the hospital nodes every two seconds. A hospital that has not received a coordinator heartbeat for eight seconds starts a Ring election by circulating candidate IDs around the configured logical ring. The highest-ID reachable participant is announced to the ring and begins sending coordinator heartbeats. Other hospitals monitor that leader heartbeat and can start a new election if it becomes stale. Local processes default to `http://localhost:5000`; Compose and Kubernetes set `DOCKER_ENV=true` for service-name discovery.

A missed heartbeat is suspicion, not proof of a crash; network delay or a partition can cause false positives and competing elections. The elected hospital role is recorded on the nodes, but this demo does not move the Flask API or shared request state off the configured coordinator process. Browser WebRTC signaling is provided by the separate Node service in `frontend/signaling`; it is not the hospital coordinator. True service failover needs replicated coordinator state and service discovery/routing to the elected process.

## Course topic coverage and design choices

| Topic | Project coverage | Boundary / reason |
| --- | --- | --- |
| Clock synchronization | Four-timestamp offset and round-trip estimates are shown on hospital cards. **Run Berkeley Synchronization** averages the estimated coordinator/hospital clocks and applies per-node corrections. | Corrections are simulated, process-local offsets; machine clocks are unchanged, offsets are not persistent, and estimates assume roughly symmetric network delay. |
| Logical clocks and Lamport's algorithm | Coordinator and hospitals exchange and persist Lamport timestamps on RPCs. | Orders causally related events, not physical time. |
| Vector algorithm | RPCs exchange persisted vector clocks; comparisons can identify concurrent events. | Covers coordinator/hospital RPC flows, not every browser or RabbitMQ event. |
| Global state | Dashboard snapshots collect status, beds, and hospital Lamport clocks. | Best-effort sequential observation, not a Chandy–Lamport consistent cut; channel messages are not recorded. |
| Beacon protocol | Hospitals send periodic heartbeats; stale beacons exclude nodes from new allocations. | A failure detector can mistake a slow network for a failed hospital. |
| Mutual exclusion and priority | The coordinator serializes reservation critical sections with a centralized lock. **Fair · Lamport FIFO** is the default; **Urgency first** lets Critical/High confirmation requests overtake Normal ones. Critical/high requests also route to the largest matching capacity, while normal requests use best-fit. | Urgency-first queueing can starve normal requests. The coordinator lock is not shared across coordinator replicas; MongoDB's conditional bed decrement remains the cross-request safety boundary. |
| Ring election and coordinator beacon | **Run Ring Election** circulates hospital candidate IDs and announces the highest reachable ID. Hospital nodes monitor coordinator heartbeats and start the same election automatically after an eight-second timeout; the elected hospital sends heartbeats to peers. **Run Bully Election** remains available for comparison. | Heartbeat timeouts can mistake network delay/partitions for failure. Election updates the hospital leader role only: the Flask API and shared request state remain hosted by the configured coordinator. |
| Lodha–Kshemkalyani fair mutual exclusion | The dashboard's Fair · Lamport FIFO policy demonstrates starvation-resistant centralized ordering; it is not the Lodha–Kshemkalyani protocol itself. | The named fair distributed algorithm has its own protocol and assumptions; FIFO lock ordering is a comparison point, not a claim of implementing that algorithm. |
| Knapp deadlock-detection classification | **Simulate transfer deadlock** creates two opposite-order hospital resource waits, sends Chandy–Misra–Haas-style edge-chasing probes through the wait-for graph, and aborts the youngest transaction by Lamport timestamp. | This is a contained transfer simulation; it does not acquire or modify live hospital beds. It demonstrates distributed edge-chasing, one of Knapp's distributed detection classes. |
| Distributed object-based systems | The coordinator creates a `RemoteHospitalObject` proxy bound to a hospital ID and invokes allowlisted remote operations through gRPC. | This is a bounded object-proxy teaching example, not a general distributed object runtime; object state and lifecycle remain inside each hospital process. |
| Distributed web-based systems | React dashboard, Flask API, gRPC services, a separate Node WebSocket SDP/ICE service, and browser-to-browser WebRTC peers. | Browser role selection is a demo, not authentication; see the root README for HTTPS/TURN setup and the multi-browser demo. |
| Distributed file systems | The optional Compose analytics profile starts a NameNode and two DataNodes. The Activity report archives de-identified NDJSON through WebHDFS at `/api/analytics/hdfs/export`. | This is a learning cluster with one NameNode, not HA storage; it stores report snapshots, not clinical files or MongoDB records. |
| Apache Hadoop | The analytics profile also starts YARN with two NodeManagers; `analytics/run_hadoop_job.sh` runs the existing Hadoop Streaming mapper/reducer against archived HDFS input. | Hadoop is a batch report path, not part of live bed allocation. |
| Serverless architectures | The optional `deploy/knative-analytics.yaml` exposes the analytics reducer as a Knative Service configured to scale from zero to three pods. The coordinator invokes it only when `ANALYTICS_FUNCTION_URL` is set. | Knative needs a Kubernetes cluster and its Serving/networking components. The coordinator, hospital gRPC servers, heartbeat senders, and RabbitMQ consumers remain long-running services. |
| Kubernetes | Optional local-cluster manifest at `deploy/kubernetes.yaml`. | Provides service discovery and container health/restarts. The single MongoDB and RabbitMQ deployments are not replicated or highly available. |
| AWS, Cloudflare, Megaport | External architecture examples are documented in the root README. | These provider products are not integrated; a cloud deployment or inter-site network needs separate infrastructure, security, and operations decisions. |
| Blockchain / distributed ledger | Not implemented. | MongoDB is a trusted central persistence service here. A replicated consensus ledger adds latency and operational complexity without a current trust-boundary requirement. |

### Database trade-offs
MongoDB is used because the hospital bed document supports an atomic conditional decrement, and the coordinator and hospitals share a straightforward document model for requests, logs, and snapshots. This protects the bed count from going below zero when reservations race. The coordinator's earlier availability read is only a selection hint; the selected hospital checks current state and performs the authoritative reservation.

This deployment favors simple operations and per-document atomic updates over multi-node availability. The Compose and Kubernetes examples use a single MongoDB server, not a replica set; persisted volumes protect against ordinary container restarts, not disk loss or site failure. Reservation state and its history are also written as separate operations, so this prototype should not be presented as providing a cross-document transaction or production-grade audit guarantee. A production evolution should evaluate MongoDB replica sets and transactions (or another database with an explicit transactional/availability model), backups, authentication, TLS, and recovery tests.

### Demonstrating election and deadlock detection
Start the coordinator and hospital nodes as described below, then open the dashboard. Run **Ring Election** to see the candidate list circulate and the highest-ID reachable hospital elected, or run **Bully Election** for comparison. Stop the coordinator process to see hospitals detect the missed heartbeat and automatically start a Ring election; the elected hospital then sends heartbeats to the other nodes. The hospital leader role is elected, but the Flask coordinator and its signaling/request state do not migrate, so this is not transparent API failover. Use **Reservation Mutual Exclusion** to switch between Lamport-FIFO fairness and urgency-first critical-section ordering. Run **Simulate transfer deadlock** to inspect the wait-for graph and probe trace for two simulated transfers. The detector aborts the transaction with the larger Lamport timestamp; the demonstration does not change live bed counts.

### Running HDFS and Hadoop Streaming locally

Start the regular app with the HDFS/YARN cluster:

```bash
docker compose --profile analytics up --build -d
```

Archive an analytics snapshot with the Activity report's **Archive to HDFS** button, or call:

```bash
curl -X POST http://localhost:5000/api/analytics/hdfs/export
```

Use the returned `hdfs_path` as the input and choose a new output directory for each run:

```bash
docker compose --profile analytics exec namenode hdfs dfs -mkdir -p /hospital-analytics/results
docker compose --profile analytics exec namenode bash /opt/hospital-analytics/run_hadoop_job.sh \
  /hospital-analytics/exports/analytics-REPLACE-WITH-ARCHIVE-FILENAME.ndjson \
  /hospital-analytics/results/run-001
docker compose --profile analytics exec namenode hdfs dfs -cat /hospital-analytics/results/run-001/part-00000
```

This uses two HDFS DataNodes and two YARN NodeManagers. HDFS data persists in named Docker volumes; the single NameNode is still a failure point. Reports contain aggregate dimensions only; patient IDs and names are excluded.

### Running the Knative analytics function

Knative is an optional free, open-source serverless runtime on Kubernetes. Install Knative Serving and a networking layer using the [official installation guide](https://knative.dev/docs/install/), build the backend image into the cluster as described in the root README, and deploy:

```bash
kubectl apply -f deploy/knative-analytics.yaml
kubectl rollout status -n distributed-hospital ksvc/analytics-function
kubectl set env deployment/coordinator \
  ANALYTICS_FUNCTION_URL=http://analytics-function.distributed-hospital.svc.cluster.local \
  -n distributed-hospital
```

When enabled, `GET /api/analytics/summary` calls Knative for the report; a function outage returns HTTP 502 and is not hidden by a local fallback. Without the environment variable, the coordinator uses its local MapReduce-compatible implementation. The function is only for de-identified reporting and does not serve the emergency allocation path.

## Project structure
- `hospital_node.py` : one hospital server process
- `coordinator.py` : Flask coordinator that talks to all hospital nodes
- `ring_election.py` : candidate circulation and coordinator announcement for Ring elections
- `rpc_service.py` : local hospital operations used by gRPC
- `grpc_service.py` : small gRPC server and client transport
- `message_service.py` : RabbitMQ publisher and consumer
- `hospital_registry.py` : hospital configuration loader
- `hospitals.json` : hospital addresses and initial bed counts
- `Dockerfile` : Docker image for the backend services
- `requirements.txt` : Python dependencies
- `mongo_service.py` : MongoDB persistence layer
## Technologies used
- Python
- Flask
- gRPC
- RabbitMQ
- MongoDB
- Docker
- React
- Vite
- CSS

## How to run

### 1. Install Python packages
```bash
cd backend
pip install -r requirements.txt
```

### 2. Start RabbitMQ and MongoDB

The easiest option is Docker:

```bash
docker compose up rabbitmq mongodb
```

RabbitMQ management UI: `http://localhost:15672` (guest / guest).

### 3. Start Hospital A
```bash
python hospital_node.py --hospital-id H001 --name "City Hospital" --port 5001 --icu 2 --emergency 5 --general 10
```

### 4. Start Hospital B
```bash
python hospital_node.py --hospital-id H002 --name "Metro Hospital" --port 5002 --icu 0 --emergency 3 --general 8
```

### 5. Start Hospital C
```bash
python hospital_node.py --hospital-id H003 --name "Central Hospital" --port 5003 --icu 4 --emergency 2 --general 12
```

### 6. Start coordinator
```bash
python coordinator.py
```

### 7. Access the frontend
Open the frontend in a browser after running Vite.

## Docker setup
This project can also be run in containers for virtualization and easier environment management. Compose starts RabbitMQ, three gRPC hospital nodes, and the coordinator:

```bash
docker compose up --build
```

Hospital gRPC endpoints are exposed on ports `5001`, `5002`, and `5003`. RabbitMQ uses port `5672`; its management UI uses `15672`. MongoDB uses ports `27017` and `27018` and stores data in the `mongodb_data` Docker volume. Use `mongodb://localhost:27018` in Compass to connect specifically to the Docker database.

## Kubernetes local-cluster demo
The manifest at `deploy/kubernetes.yaml` runs MongoDB, RabbitMQ, the three hospital nodes, coordinator, WebSocket signaling, and the frontend. The Nginx frontend proxies `/api/` and `/ws` to their internal Kubernetes Services. The supplied MongoDB and RabbitMQ deployments each have one pod with a persistent volume claim; Kubernetes can restart pods, but these single-pod stores are not replicated or highly available.

From the repository root, with Docker, Minikube, and kubectl installed:

```bash
minikube start
docker build -t distributed-hospital-backend:local .\backend
docker build -t distributed-hospital-frontend:local .\frontend
docker build -t distributed-hospital-signaling:local -f .\frontend\signaling\Dockerfile .\frontend
minikube image load distributed-hospital-backend:local
minikube image load distributed-hospital-frontend:local
minikube image load distributed-hospital-signaling:local
kubectl apply -f .\deploy\kubernetes.yaml
kubectl rollout status -n distributed-hospital deployment/mongodb
kubectl rollout status -n distributed-hospital deployment/rabbitmq
kubectl rollout status -n distributed-hospital deployment/coordinator
kubectl rollout status -n distributed-hospital deployment/hospital-1
kubectl rollout status -n distributed-hospital deployment/hospital-2
kubectl rollout status -n distributed-hospital deployment/hospital-3
kubectl rollout status -n distributed-hospital deployment/signaling
kubectl rollout status -n distributed-hospital deployment/frontend
kubectl port-forward -n distributed-hospital service/frontend 8080:80
```

Open `http://localhost:8080`. This same-origin route reaches the coordinator API and signaling service through Nginx, and localhost allows browser camera/microphone permissions. For remote clients, use an HTTPS Ingress: install an Ingress controller, configure DNS and a TLS certificate, replace the example hostname in `deploy/kubernetes-ingress.yaml`, then apply that optional manifest. Direct HTTP NodePort access from another machine does not provide the secure context required for camera/microphone access; restrictive NAT/firewall paths may additionally require a TURN server.

The manifest requests persistent volumes from the cluster's default storage class. Keep the namespace and its claims if you want the demo data to remain available; this local setup is not a backup or production availability configuration.

The Kubernetes manifest has not been applied to a live cluster in this workspace because no kubectl context is configured. Knative resources require Knative Serving CRDs and an ingress/networking layer; applying the optional Knative manifest to a plain Kubernetes cluster will fail until those are installed.

## Demo request
Send a request with:
- Patient ID: P102
- Bed Type: ICU
- Priority: Critical

Expected result:
- City Hospital -> ICU 2
- Metro Hospital -> ICU 0
- Central Hospital -> ICU 4
- Coordinator selects Central Hospital for consultation; no bed is reserved yet
- Open the dashboard in another browser, set “This dashboard represents” to Central Hospital, and use the offered audio/video consultation
- After the connected call ends, choose “Confirm and Reserve Bed”
- Central Hospital rechecks availability and reserves the ICU bed

## Fault tolerance example
Stop Hospital C and request another ICU bed. The coordinator should detect failure and select a remaining online hospital for consultation. If the selected hospital goes offline or runs out of beds before confirmation, the reservation is rejected and a new request is required.

## Browser WebRTC demonstration
The previous Flask HTTP-polling call signaling has been removed. For the replacement browser-to-browser WebRTC demo, follow the install, TLS/TURN configuration, and two-laptop steps in the root [README](../README.md). The separate Node WebSocket service relays registration/presence and SDP/ICE only; application traffic and video use peer connections. The browser role selector is a demo identity, not authentication.

## Future production-oriented work
- Add coordinator replicas and a consensus-backed or otherwise well-defined leader-election mechanism before claiming coordinator high availability.
- Use MongoDB replica-set transactions if bed state and reservation history must commit atomically.
- Add authentication, authorization, TLS, backups, and recovery drills before using real patient data.
- Keep textbook Lodha–Kshemkalyani fairness and Knapp deadlock-detection demonstrations separate from the operational reservation path unless the resource model actually requires them.
