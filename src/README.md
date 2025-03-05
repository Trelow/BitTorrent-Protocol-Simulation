# BitTorrent Protocol Simulation

## Overview
This project simulates the **BitTorrent protocol** for **peer-to-peer file sharing** using **MPI (Message Passing Interface)** for distributed communication. The simulation includes a **tracker** and multiple **peers** that can download and upload files in a decentralized manner.

## Features
### 1. **MPI-Based Peer-to-Peer File Sharing**
- Uses **MPI** to simulate **peer-to-peer (P2P)** communication.
- Implements **multi-threading** using `pthread` for concurrent **upload** and **download** operations.
- Peers exchange file **segments** instead of full files, mimicking real **BitTorrent swarming** behavior.

### 2. **Tracker-Based Peer Discovery**
- A single **tracker process** keeps track of file availability.
- Peers register their **available files and segments** with the tracker.
- Peers query the tracker to find **other peers (seeds or leechers)** to download from.

### 3. **Efficient Parallel File Downloading**
- **Round-Robin Peer Selection**: Each file segment is downloaded from a different peer to balance the network load.
- **Randomized Segment Ordering**: To improve efficiency when multiple peers download the same file simultaneously.
- **Hash Verification**: Ensures that downloaded file segments are not corrupted.
- **Dynamic Role Updates**: Peers transition from **leecher → peer → seed** as they download more segments.

## Architecture
The system consists of two primary components:
### **1. Tracker** (Rank 0)
- Stores information about **which peers have which files**.
- Responds to requests from peers for file availability.
- Updates peer roles dynamically (**leecher → peer → seed**).
- Terminates once all peers finish downloading their required files.

### **2. Peers** (Rank 1 to `N-1`)
- Each peer has an **input file** defining:
  - Files it **owns** (available for upload).
  - Files it **wants to download**.
  - **Hash values** for verification.
- Communicates with the tracker and other peers.
- Runs **two threads**:
  - **Download Thread**: Requests file segments from other peers.
  - **Upload Thread**: Responds to segment requests from other peers.

## Implementation Details
### **Initialization**
1. **Tracker Process (Rank 0) Starts**:
   - Waits for all peers to send their **owned files** and **hashes**.
   - Stores the file-to-peer mapping in memory.
   - Responds with an "OK" message to all peers, signaling the start of downloads.

2. **Each Peer (Ranks 1 to `N-1`) Starts**:
   - Reads its input file to determine **owned** and **desired** files.
   - Registers available files with the tracker.
   - Starts **upload** and **download** threads.

### **Downloading Process**
1. A peer requests a **swarm list** from the tracker for a file it wants.
2. The tracker responds with a list of available **seeds/peers**.
3. The peer downloads file segments:
   - Uses **round-robin scheduling** to avoid overloading any single peer.
   - Segments are downloaded in **random order** to ensure diversity in the swarm.
4. After every **10 segments**, the peer updates its **swarm list** from the tracker.
5. Once a file is completely downloaded:
   - The peer verifies **hash integrity**.
   - Notifies the tracker and becomes a **seed** for that file.
   - Saves the file locally.

### **Uploading Process**
- Peers respond to `DOWNLOAD_SEGMENT_CODE` requests from other peers.
- If they **own the requested segment**, they send it.
- If they **do not have the segment**, they respond with `NACK`.
- The tracker notifies upload threads to terminate when the last peer finishes downloading.

### **Role Transition Mechanism**
| Role     | Condition |
|----------|-----------|
| **SEED** | Has all segments of a file |
| **PEER** | Has downloaded at least **10 segments** of a file |
| **LEECHER** | Still downloading a file |

## Message Protocol
| Message Code | Description |
|-------------|-------------|
| `SWARM_REQ_CODE` | Peer requests available peers for a file |
| `SWARM_UPDATE_CODE` | Peer updates tracker with newly downloaded segments |
| `DOWNLOAD_SEGMENT_CODE` | Peer requests a specific file segment |
| `FILE_DOWNLOAD_FINISHED_CODE` | Peer informs tracker that a file is fully downloaded |
| `ALL_FINISHED_CODE` | Peer notifies tracker that all downloads are complete |


## Running the Simulation
### **Prerequisites**
- **MPI Library** (`mpich` or `openmpi`)
- **C++ Compiler** (`g++`)

### **Compilation**
```sh
g++ -o bittorrent bittorrent_simulation.cpp -lpthread -lmpi
```

### **Execution**
Run the simulation with `N` processes:
```sh
mpirun -np <num_peers+1> ./bittorrent
```
Example for **5 peers**:
```sh
mpirun -np 6 ./bittorrent
```

### **Input File Format (`inX.txt`)**
Each input file specifies the files a peer **owns** and **wants**:
```
<num_files_owned>
<file_name> <num_chunks>
<chunk_hash1>
<chunk_hash2>
...
<num_files_needed>
<needed_file_name>
```
Example (`in1.txt`):
```
1
fileA.txt 3
abc123
xyz789
def456
1
fileB.txt
```

### **Output Files**
Once a peer downloads a file, it saves it in `output/` with the format:
```
client<id>_<filename>.txt
```

## Performance Optimizations
### **1. Load Balancing via Round-Robin Scheduling**
- Segments are downloaded from **different peers** to prevent overload.

### **2. Randomized Segment Selection**
- Segments are downloaded in a **random order** to increase diversity in the swarm.

### **3. Hash Verification for Data Integrity**
- Each segment is verified using a **hash comparison** before being marked as downloaded.

### **4. Parallel Uploading & Downloading**
- Uses **multi-threading** so that peers can upload and download simultaneously.

## Conclusion
This simulation successfully models the **BitTorrent protocol**, allowing efficient parallel file sharing. The use of **MPI for communication** and **multi-threading for concurrent operations** ensures high performance and scalability.

By implementing **swarm-based downloading, role transitions, and peer discovery**, this project provides an accurate representation of real-world **peer-to-peer networking**. 🚀

