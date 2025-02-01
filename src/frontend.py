import sys
from client import ShoppingListClient

class ShoppingListCLI:
    def __init__(self, user="alice"):
        self.user = user
        self.client = ShoppingListClient(user)

    def create_list(self):
        message = self.client.create_list()
        return f"List '{message}' created"

    def add_item(self, list_id, item_name, quantity):
        message = self.client.add_item(list_id, item_name, quantity)
        return message
        
    def remove_item(self, list_id, item_name, quantity):
        message = self.client.remove_item(list_id, item_name, quantity)
        return message

    def get_items(self, list_id):
        shopping_list = self.client.get_list(list_id)
        if shopping_list is None:
          return (f"List {list_id} does not exist") 
        if shopping_list and shopping_list.get_list():
            message = f"Items in list {list_id}:"
            for item_name, quantity in shopping_list.get_list():
                message += f"\n - {item_name}: {quantity}"
        else:
            message = f"No items found in list {list_id}"
        
        return message

    def get_lists(self):
        response = self.client.get_lists()
        if response.get("status") == "error":
            print(response.get("message"))
            return
        return response

    def sync_list(self, list_id):
        response = self.client.merge_lists(list_id)

        if response.get('status') == 'success':
            message = f"List '{list_id}' synced to the server"
        else:
            message = f"Error syncing list '{list_id}' to server: '{response.get('message')}'"

        return message

    def welcome(self):
        print("************************")
        print(f"** Hi, {self.user}! Welcome to the Shopping List CLI!")
        print("************************")

    def convert_to_int(self, value):
        try:
            return int(value)
        except ValueError:
            return "Invalid quantity"

    def run(self):
        while True:
            command = input("\nEnter command (create, add, get, get_lists, remove, sync, quit): ").strip().lower()
            if command == "create":
                message = self.create_list()
                print("\n*************************************************")
                print(f"** {message}")
                print("*************************************************")
            elif command == "add":
                print("\n**********")
                list_id = input("** Enter list ID: ").strip()
                if not list_id:
                    print("\n*****************************")
                    print("** List ID cannot be empty **")
                    print("*****************************")
                    continue
                item_name = input("Enter item name: ").strip()
                if not item_name:
                    print("\n*******************************")
                    print("** Item name cannot be empty **")
                    print("*******************************")
                    continue
                quantity = self.convert_to_int(input("Enter quantity: ").strip())
                if quantity == "Invalid quantity":
                    print("\n**********************")
                    print("** Invalid quantity **")
                    print("**********************")
                    continue
                message = self.add_item(list_id, item_name, quantity)
                print(message)
            elif command == "get":
                print("\n**********")
                list_id = input("** Enter list ID: ").strip()
                if not list_id:
                    print("\n*****************************")
                    print("** List ID cannot be empty **")
                    print("*****************************")
                    continue
                message = self.get_items(list_id)
                print(message)
            elif command == "get_lists":
                self.get_lists()
            elif command == "remove":
                print("\n**********")
                list_id = input("** Enter list ID: ").strip()
                if not list_id:
                    print("\n*****************************")
                    print("** List ID cannot be empty **")
                    print("*****************************")
                    continue
                item_name = input("Enter item name: ").strip()
                if not item_name:
                    print("\n*******************************")
                    print("** Item name cannot be empty **")
                    print("*******************************")
                    continue
                quantity = self.convert_to_int(input("Enter quantity: ").strip())
                if quantity == "Invalid quantity":
                    print("\n**********************")
                    print("** Invalid quantity **")
                    print("**********************")
                    continue
                message = self.remove_item(list_id, item_name, quantity)
                print(message)
            elif command == "sync":
                print("\n**********")
                list_id = input("** Enter list ID: ").strip()
                if not list_id:
                    print("\n*****************************")
                    print("** List ID cannot be empty **")
                    print("*****************************")
                    continue
                message = self.sync_list(list_id)
                print(message)
            elif command == "quit":
                break
            else:
                print("\n*********************")
                print("** Invalid command **")
                print("*********************")

if __name__ == "__main__":
    # Receive user through command line arguments
    if len(sys.argv) < 2:
        print("Please provide a username.")
        sys.exit(1)
    user = str(sys.argv[1])
    cli = ShoppingListCLI(user)
    cli.welcome()
    cli.run()
