import json
import os

class Database:
    def __init__(self, filename='database/shopping_lists.json'):
        self.filename = filename
        self.data = {}
        self.load()

    def load(self):
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as file:
                self.data = json.load(file)
        else:
            self.data = {}
            os.makedirs(os.path.dirname(self.filename), exist_ok=True)
            with open(self.filename, 'w') as file:
                json.dump({}, file)
    def save(self):
        with open(self.filename, 'w') as file:
            json.dump(self.data, file, indent=4)

    def add_list(self, list_id, items):
        self.data[list_id] = items
        self.save()

    def get_lists(self):
        return self.data.keys()

    def get_list(self, list_id):
        return self.data.get(list_id, [])

    def delete_list(self, list_id):
        if list_id in self.data:
            del self.data[list_id]
            self.save()