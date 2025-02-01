from datetime import datetime
import uuid
import hashlib

class ShoppingListORSet:
    def __init__(self, listID=None):
        # Unique ID for each shopping list (auto-generated if not provided)
        self.listID = listID if listID else hashlib.sha256(str(uuid.uuid4()).encode('utf-8')).hexdigest()
        self.add_set = set()  # Stores tuples of (item_name, target_quantity, tag)
        self.remove_set = set()  # Stores tuples of (item_name, target_quantity, tag)
        self.processed_tags = set() 

    def _generate_tag(self):
        """Generate a unique tag for each item (UUID + timestamp)."""
        return f"{uuid.uuid4()}-{datetime.now().isoformat()}"

    def add_item(self, item_name, target_quantity):
        """Add an item with a target quantity to the shopping list."""
        tag = self._generate_tag()
        self.add_set.add((item_name, target_quantity, tag))
        self.processed_tags.add(tag)

    def remove_item(self, item_name, quantity_acquired):
        """Remove a specified quantity of an item from the shopping list."""
        tag = self._generate_tag()
        items_to_remove = [(i, qty, t) for (i, qty, t) in self.add_set if i == item_name]

        if not items_to_remove:
            return -1
        else:
            self.remove_set.add((item_name, quantity_acquired, tag))
            self.processed_tags.add(tag)
    
    
    def get_list(self):
        """Get the current items in the shopping list with their quantities."""
        items = {}
        for item_name, qty, _ in self.add_set:
            if item_name in items:
                items[item_name] += qty
            else:
                items[item_name] = qty

        for item_name, qty, _ in self.remove_set:
            if item_name in items:
                items[item_name] -= qty

        return items.items()

    def merge(self, other_list):
        """Merge this shopping list with another shopping list (other OR-Set)."""
        if isinstance(other_list, dict):
            other = ShoppingListORSet()
            other.deserialize(other_list)
        else:
            other = other_list

        new_adds = {item for item in other.add_set if item[2] not in self.processed_tags}
        new_removes = {item for item in other.remove_set if item[2] not in self.processed_tags}

        self.processed_tags.update(tag for _, _, tag in new_adds | new_removes)

        self.add_set |= new_adds
        self.remove_set |= new_removes


    def serialize(self):
        """Serialize the shopping list to a dictionary."""
        return {
            "listID": self.listID,
            "add_set": list(self.add_set),
            "remove_set": list(self.remove_set)
        }
    
    def deserialize(self, data):
        """Deserialize a dictionary to a shopping list."""
        self.listID = data["listID"]
        # Add the items to the add set
        self.add_set = set(tuple(item) for item in data["add_set"])
        
        # Add the items to the remove set
        self.remove_set = set(tuple(item) for item in data["remove_set"])

    def __repr__(self):
        """Represent the current shopping list."""
        return f"ShoppingListORSet({self.get_list()})"
