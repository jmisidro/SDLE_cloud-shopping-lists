import sys
import zmq
import time
import threading
import hashlib
from ORSet import ShoppingListORSet
from database import Database

HEARTBEAT = 5

class Worker:
    def __init__(self, port, xsub_addr="tcp://localhost:5555", xpub_addr="tcp://localhost:5556"):
        self.port = port
        self.id = hashlib.sha256(str(port).encode('utf-8')).hexdigest()
        self.lists = {}
        self.db = Database(filename=f'database/worker{port}/shopping_lists.json')  # Initialize the database
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)  # REP socket to receive requests from other workers
        self.socket.bind(f"tcp://*:{port}")  # Binding to port for communication

        self.poller = zmq.Poller() # Poller for the socket
        self.poller.register(self.socket, zmq.POLLIN) # Register the socket with the poller

        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.connect(xsub_addr)
        
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.connect(xpub_addr)
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

        self.neighbors = []  # set of neighboring workers
        self.privious_neighbors = []  # set of previous neighboring workers
        self.index = 0  # index of the worker in the ring
        
        # Ring for the workers
        self.worker_ring = {}


    def add_list(self, list_id):
        """Add a new shopping list to the worker."""
        self.lists[list_id] = ShoppingListORSet(listID=list_id)
        self.db.add_list(list_id, self.lists[list_id].serialize())  # Store the shopping list in the database


    def get_list(self, list_id):
        """Get the current items in the shopping list with their quantities."""
        if list_id not in self.lists:
            serialized_list = self.db.get_list(list_id)
            if serialized_list:
                self.lists[list_id] = ShoppingListORSet(listID=list_id)
                self.lists[list_id].deserialize(serialized_list)
        list = self.lists.get(list_id)
        return list.serialize() if list else None

    def merge_lists(self, list_id, other_list):
        """Merge two shopping lists."""
        if list_id not in self.lists:
            self.add_list(list_id)
        self.print_merge_lists(other_list)
        self.lists[list_id].merge(other_list)
        self.db.add_list(list_id, self.lists[list_id].serialize())  # Update the shopping list in the database
        return f"List {list_id} merged successfully."


    def merge_replicas(self, list_id, replica):
        """Merge a replica of the data from a neighboring worker."""
        if list_id not in self.lists:
            self.add_list(list_id)
        self.print_merge_replica(replica)
        self.lists[list_id].merge(replica)
        self.db.add_list(list_id, self.lists[list_id].serialize())  # Update the shopping list in the database
        return f"Replica of list {list_id} merged successfully."

    def _replicate_data(self, list_id):
        self.print_replicating_data(list_id)
        """Replicate data to the next two neighboring workers."""
        
        message = {
            "id": self.id,
            "port": self.port,
            "action": "merge_replicas",
            "list_id": list_id,
            "list": self.get_list(list_id)
        }
        
        for neighbor in self.neighbors:
            # Establish connection with the targer worker
            connection = self.context.socket(zmq.REQ)
            connection.connect(f"tcp://127.0.0.1:{neighbor['port']}")
            connection.send_json(message)
            response = connection.recv_json()

            if response["status"] == "success":
                self.print_success_replicate(list_id, neighbor)
            else:
                self.print_unsuccessfully_replicate(list_id, neighbor)    
            # Close the connection
            connection.close()
        

    def receive_updates(self):
        while True:
            socks = dict(self.poller.poll())

            if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                request = self.socket.recv_json()
            
            action = request.get("action")
            list_id = request.get("list_id")
            list = request.get("list")

            target_worker = None
            sorted_workers = sorted(self.worker_ring.values(),key=lambda worker: hashlib.sha256(str(worker["port"]).encode('utf-8')).hexdigest())

            for i in range(len(sorted_workers)):
                if(sorted_workers[i]["id"] > list_id):
                    target_worker = sorted_workers[i]["port"]
                    break

            if target_worker is None:
                target_worker = sorted_workers[0]["port"]

            if action == "merge_replicas":
                message = self.merge_replicas(list_id, list)
                response = {"status": "success", "message": message, "list": self.get_list(list_id)}
                self.socket.send_json(response)
            else:
                if(target_worker == self.port):
                    if action == "get_list":
                        response = {"status": "success", "list": self.get_list(list_id)}
                        self.socket.send_json(response)
                    elif action == "merge_lists":
                        message = self.merge_lists(list_id, list)
                        response = {"status": "success", "message": message, "list": self.get_list(list_id)}
                        self.socket.send_json(response)
                        self._replicate_data(list_id)
                    else:
                        response = {"status": "error", "message": "Invalid action."}
                        self.socket.send_json(response)

                else:
                    self.print_target_worker(target_worker)

                    connection = self.context.socket(zmq.REQ)
                    connection.connect(f"tcp://127.0.0.1:{target_worker}")
                    connection.send_json(request)
                    forwarded_response = connection.recv_json()
                    connection.close()
                    self.socket.send_json(forwarded_response)



    def determine_neighbors(self):
        """Determine the neighboring workers in the ring."""
        workers = list(self.worker_ring.values())
        ring_size = len(workers)

        if ring_size <= 1:
            self.neighbors = []
            self.previous_neighbors = []
            return

        index = next(i for i, worker in enumerate(workers) if worker["id"] == self.id)
        self.index = index

        num_neighbors = 2 if ring_size > 2 else 1
        self.neighbors = [
            workers[(index + i) % ring_size]
            for i in range(1, num_neighbors + 1)
        ]

        self.previous_neighbors = [
            workers[(index - i) % ring_size]
            for i in range(1, num_neighbors + 1)
        ]
        

    def add_to_ring(self, worker):
        """Add/Update a worker to the ring."""
        len_before = len(self.worker_ring)
        self.worker_ring[worker["id"]] = worker
        len_after = len(self.worker_ring)
        self.worker_ring = dict(sorted(self.worker_ring.items(), key=lambda item: item[1]['id']))
        if len_after > len_before:
            self.print_add_ring(worker)
            self.adjust_data_add(worker)
        self.determine_neighbors()
        

    def remove_from_ring(self, worker):
        """Remove a worker from the ring."""
        if worker["id"] in self.worker_ring:
            del self.worker_ring[worker["id"]]
        self.determine_neighbors()

    def check_heartbeats(self):
        for worker in list(self.worker_ring.keys()):
            if time.time() - self.worker_ring[worker]["timestamp"] > HEARTBEAT:
                self.adjust_data_remove(self.worker_ring[worker])
                self.print_fail_heartbeat(worker)
                self.remove_from_ring(self.worker_ring[worker])
                
                
    def adjust_data_remove(self, worker_breakdown):
        
        workers = list(self.worker_ring.values())
        ring_size = len(workers)
        
        if ring_size < 4:
            return
        
        index = next(i for i, worker in enumerate(workers) if worker["id"] == worker_breakdown["id"])
        
        next_neighbor_index = (index + 1) % ring_size
        prev_neighbor_index = (index - 1) % ring_size
        
        if (next_neighbor_index != self.index and prev_neighbor_index != self.index):
            return
        
        # Next Neighbor
        if (next_neighbor_index == self.index):
            self.print_remove_worker()
            neighbor_0 = self.neighbors[0]
            neighbor_1 = self.neighbors[1]
            neighbor_0_id = self.neighbors[0]["id"]
            prev_neighbor_0_id = self.previous_neighbors[0]["id"]
            prev_neighbor_1_id = self.previous_neighbors[1]["id"]

            for list_id in self.lists.keys():
                # Add to neighbor[1]
                if list_id < prev_neighbor_1_id or (prev_neighbor_1_id < neighbor_0_id and neighbor_0_id < list_id):
                    self.adjust_data(neighbor_0, list_id)

                # Add to neighbor[2]
                elif prev_neighbor_1_id < list_id < prev_neighbor_0_id or (prev_neighbor_0_id < prev_neighbor_1_id and (list_id > prev_neighbor_1_id or list_id < prev_neighbor_0_id)):
                    self.adjust_data(neighbor_1, list_id)
        
        # Previous Neighbor
        elif (prev_neighbor_index == self.index):
            self.print_remove_worker()
            neighbor_1 = self.neighbors[1]
            prev_neighbor_0_id = self.previous_neighbors[0]["id"]
            prev_neighbor_1_id = self.previous_neighbors[1]["id"]
            
            for list_id in self.lists.keys():
                # Add to neighbor[0]
                if prev_neighbor_1_id < list_id < prev_neighbor_0_id or (prev_neighbor_0_id < prev_neighbor_1_id and (list_id > prev_neighbor_1_id or list_id < prev_neighbor_0_id)):
                    self.adjust_data(neighbor_1, list_id)
                    
            
    def adjust_data_add(self, worker_added):
        
        workers = list(self.worker_ring.values())
        ring_size = len(workers)
        
        if ring_size < 2:
            return
        
        index = next(i for i, worker in enumerate(workers) if worker["id"] == worker_added["id"])
        
        next_neighbor0_index = (index + 1) % ring_size
        next_neighbor1_index = (index + 2) % ring_size
        next_neighbor2_index = (index + 3) % ring_size
        
        if ring_size < 4:
            if (self.index == next_neighbor0_index):
                for list_id in self.lists.keys():
                    self.adjust_data(worker_added, list_id)
            return
        
        if (self.index == next_neighbor0_index):
            self.print_add_worker()
            for list_id in list(self.lists.keys()):
                if (list_id < worker_added["id"]) or (self.id < self.previous_neighbors[1]["id"] and list_id > self.previous_neighbors[1]["id"]):
                    self.adjust_data(worker_added, list_id)
                
                if (self.previous_neighbors[1]["id"] < self.id and list_id < self.previous_neighbors[1]["id"]) or (self.previous_neighbors[1]["id"] > self.id and self.id < list_id < self.previous_neighbors[1]["id"]):
                    self.db.delete_list(list_id)
                    del self.lists[list_id]
                    
        elif (self.index == next_neighbor1_index):
            self.print_add_worker()
            for list_id in list(self.lists.keys()):  
                if (self.previous_neighbors[1]["id"] < self.id and list_id < self.previous_neighbors[1]["id"]) or (self.previous_neighbors[1]["id"] > self.id and self.id < list_id < self.previous_neighbors[1]["id"]):
                    self.db.delete_list(list_id)
                    del self.lists[list_id]
                    
        elif (self.index == next_neighbor2_index):
            self.print_add_worker()
            for list_id in list(self.lists.keys()):
                if (worker_added["id"] < self.id and list_id < worker_added["id"]) or (worker_added["id"] > self.id and self.id < list_id < worker_added["id"]):
                    self.db.delete_list(list_id)
                    del self.lists[list_id]

        
        
    def adjust_data(self, neighbor, list_id):
        """Helper function to adjust data."""
        message = {
            "id": self.id,
            "port": self.port,
            "action": "merge_replicas",
            "list_id": list_id,
            "list": self.get_list(list_id)
        }
        connection = self.context.socket(zmq.REQ)
        connection.connect(f"tcp://127.0.0.1:{neighbor['port']}")
        connection.send_json(message)
        response = connection.recv_json()
        if response["status"] == "success":
            self.print_successfully_adjust_data(list_id, neighbor)
        else:
            self.print_unsuccessfully_adjust_data(list_id, neighbor)
        connection.close()
        

    def send_heartbeat(self):
        """Continuously send heartbeat messages."""
        while True:
            message = {
                "id": self.id,
                "port": self.port,
                "timestamp": time.time()
                ## Add more fields here
            }
            # Update itself in the ring
            self.add_to_ring(message)
            self.publisher.send_pyobj(message)
            self.check_heartbeats() # Check for failed workers
            time.sleep(1)

    def receive_heartbeat(self):
        """Continuously listen for heartbeats from other workers and update their last heartbeat time."""
        while True:
            message = self.subscriber.recv_pyobj()
            # Ignore heartbeats from self
            if message["id"] == self.id:
                continue
            id = message["id"]
            port = message["port"]
            worker = {
                "id": id,
                "port": port,
                "timestamp": message["timestamp"],
                ## Add more fields here
            }
            self.add_to_ring(worker) # Add worker to ring

    def print_fail_heartbeat(self,worker):
        print('*************************************')
        print('**                                 **')
        print(f"** Worker {self.worker_ring[worker]['port']} ({worker[:6]})... failed! **")
        print('**                                 **')
        print('*************************************\n')
    
    def print_add_ring(self,worker):
        print('***********************************************')
        print('**                                           **')
        print(f"** Worker {worker['port']} ({worker['id'][:6]}) has joined the ring. **")
        print('**                                           **')
        print('***********************************************\n')

    def print_target_worker(self, target_worker):
        print('*********************************')
        print('**                             **')
        print(f"** Transmiting to worker {target_worker}. **")
        print('**                             **')
        print('*********************************\n')

    def print_success_replicate(self,list_id, neighbor):
        print('***********************************')
        print(f'**   Worker {self.port} ({self.id[:6]})        **')
        print(f"**   Replicated list {list_id[:6]} to   **")
        print(f"**   Worker {neighbor['port']} ({neighbor['id'][:6]}).       **")
        print('***********************************\n')  

    def print_unsuccessfully_replicate(self,list_id, neighbor):
        print('****************************************')
        print(f'** Worker {self.port} ({self.id[:6]})               **')
        print(f"** Failed to replicated list {list_id[:6]} to **")
        print(f"** Worker {neighbor['port']} ({neighbor['id'][:6]}).               **")
        print('****************************************\n')      

    def print_replicating_data(self, list_id):
        print('***************************************')
        print('**                                   **')
        print(f"** Replicating data for list {list_id[:6]}. **")
        print('**                                   **')
        print('***************************************\n')

    def print_merge_replica(self, replica):
        print('******************************************************')
        print('**                                                  **')
        print(f"** Merging local replica with received list {replica['listID'][:6]}. **")
        print('**                                                  **')
        print('******************************************************\n')

    def print_merge_lists(self, other_list):
        print('***************************************************')
        print('**                                               **')
        print(f"** Merging local list with received list {other_list['listID'][:6]}. **")
        print('**                                               **')
        print('***************************************************\n')

    def print_successfully_adjust_data(self,list_id,neighbor):
        print('*******************************************')
        print('**                                       **')
        print(f'**   List {list_id[:6]} Successfully adjusted   **')
        print(f"**   To neighbor {neighbor['port']} ({neighbor['id'][:6]}).          **")
        print('**                                       **')
        print('*******************************************\n')

    def print_unsuccessfully_adjust_data(self,list_id,neighbor):
        print('*************************************************')
        print('**                                       **')
        print(f'**   Failed to adjust List {list_id[:6]} Successfully   **')
        print(f"**   To neighbor {neighbor['port']} ({neighbor['id'][:6]}).                **")
        print('**                                             **')
        print('*************************************************\n')

    def print_add_worker(self):
        print('***************************************')
        print('** Worker was added! Adjusting data. **')
        print('***************************************\n')

    def print_remove_worker(self):
        print('*****************************************')
        print('** Worker was removed! Adjusting data. **')
        print('*****************************************\n')

    def start(self):
        threading.Thread(target=self.receive_updates).start()
        threading.Thread(target=self.send_heartbeat).start()
        threading.Thread(target=self.receive_heartbeat).start()

    

if __name__ == "__main__":
    # Receive port through command line arguments
    port = int(sys.argv[1])
    worker = Worker(port)
    worker.start()