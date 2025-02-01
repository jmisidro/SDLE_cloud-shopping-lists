import zmq
from ORSet import ShoppingListORSet
from database import Database

class ShoppingListClient:
    def __init__(self, user):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)  # REQ socket to send requests to the server
        self.socket.connect("tcp://127.0.0.1:6000")  # Connect to server on port 6000
        self.socket.setsockopt(zmq.RCVTIMEO, 2000)  # Set timeout for receiving messages
        self.db = Database(filename=f'database/{user}/shopping_lists.json')  # Initialize the database

    def send_request(self, request):
        """Send a request to the server and get the response."""
        try:
            self.socket.send_json(request)
            response = self.socket.recv_json()
            return response
        except zmq.error.Again:
            self.socket.close()
            self.socket = self.context.socket(zmq.REQ)
            self.socket.connect("tcp://127.0.0.1:6000")
            self.socket.setsockopt(zmq.RCVTIMEO, 2000)
            return {"status": "error", "message": "Request timed out"}
    
    def create_list(self):
        new_list = ShoppingListORSet()  # Create new shopping list
        self.db.add_list(new_list.listID, new_list.serialize())  # Store the shopping list in the database
        return new_list.listID

    def add_item(self, list_id, item_name, target_quantity):
        shopping_list = self.get_list(list_id)
        
        if shopping_list is None:
          return (f"List {list_id} does not exist")
        if target_quantity <= 0:
          return (f"Quantity {target_quantity} is not valid")
        
        shopping_list.add_item(item_name, target_quantity)
        self.db.add_list(list_id, shopping_list.serialize())  # Update the shopping list in the database
        return (f"Item '{item_name}' added to list {list_id} with quantity {target_quantity}")

    def remove_item(self, list_id, item_name, quantity_acquired):
        shopping_list = self.get_list(list_id)

        if shopping_list is None:
            return (f"List {list_id} does not exist")
        if quantity_acquired <= 0:
            return (f"Quantity {quantity_acquired} is not valid")
          
        if (shopping_list.remove_item(item_name, quantity_acquired)) == -1:
          return (f"Item {item_name} does not exist in list {list_id}")
        
        self.db.add_list(list_id, shopping_list.serialize())  # Update the shopping list in the database
        return (f"Item '{item_name}' removed from list {list_id} with quantity {quantity_acquired}")

    def get_list(self, list_id):
        if list_id in list(self.db.get_lists()):
            shopping_list = ShoppingListORSet(listID=list_id)
            shopping_list.deserialize(self.db.get_list(list_id))
            return shopping_list
        request = {
            "action": "get_list",
            "list_id": list_id
        }
        response = self.send_request(request)
        if response.get('status') is "error":
            return None
        if response.get('list') is None:
            return None
        
        shopping_list = ShoppingListORSet(listID=list_id)
        shopping_list.deserialize(response["list"])
        self.db.add_list(list_id, shopping_list.serialize())  # Update the shopping list in the database
        return shopping_list
    
    def get_lists(self):
        if not self.db.get_lists():
            return {"status": "error", "message": "No lists found"}
        lists = self.db.get_lists()
        result = {}
        for list_id in lists:
            shopping_list = self.get_list(list_id)
            items = dict(shopping_list.get_list())
            result[list_id] = items
            print(f"List ID: {list_id}")
            for item_name, quantity in items.items():
                print(f" - {item_name}: {quantity}")
        return result

    def merge_lists(self, list_id):
        if list_id not in self.db.get_lists():
            return {"status": "error", "message": f"List {list_id} does not exist"}

        request = {
            "action": "merge_lists",
            "list_id": list_id,
            "list": self.db.get_list(list_id)
        }
        response = self.send_request(request)
        if response.get('status') is "error":
            return response
        self.db.add_list(list_id, response["list"])  # Update the shopping list in the database
        shopping_list = ShoppingListORSet(listID=list_id)
        shopping_list.deserialize(response["list"])
        return response
