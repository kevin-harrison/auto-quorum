# AutoQuroum
AutoQuorum is a runtime optimizer for leader-based SMR protocols such as Paxos and Raft, providing the ability to automatically reconfigure according to the workload. AutoQuorum is embedded into the protocol logic at each node, where it collects metadata and exchanges it with the other nodes. This enables AutoQuorum to construct a global view of the system and workload that is used to compute the optimal configuration for the system. If the current configuration is not optimal, the leader triggers a reconfiguration to modify the leadership and quorum sizes accordingly. To seamlessly switch between different configurations, AutoQuorum introduces a reconfiguration mechanism that enables changing the configuration parameters without stopping the system. This is accompanied by a novel decentralized read operation that can be performed by any server even during reconfiguration, which allows AutoQuorum to have minimal effect on availability.

This repo can be used to build AutoQuorum server and client binaries which communicate over TCP. The repo also contains benchmarking code which delploys AutoQuorum servers and clients onto [GCP](https://cloud.google.com) instances and to run experiments collecting client response latencies (see `benchmarks/README.md`).

# How to run
The `build_scripts` directory contains various utilities for configuring and running AutoQuorum clients and servers. Also contains examples of TOML file configuration.
 - `run-local-client.sh` runs two clients in separate local processes. Configuration such as which server to connect to defined in TOML files.
 - `run-local-cluster.sh` runs a 3 server cluster in separate local processes.
 - `docker-compose.yml` docker compose for a 3 server cluster.
 - See `benchmarks/README.md` for benchmarking scripts 
