.PHONY: frontend proxy worker clean

# To run the frontend, use the following command
# make frontend user=alice
user=alice # default user
frontend:
	cd src && python3 frontend.py $(user)

proxy:
	cd src && python3 proxy.py

# To run a worker, use the following command
# make worker port=6000
port=6000 # default port
worker:
	cd src && python3 worker.py $(port)

clean:
	rm -rf src/database/*



