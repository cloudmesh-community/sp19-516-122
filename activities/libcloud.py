from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

google = get_driver(Provider.GCE)
driver = google('some-random-looking-email@developer.gserviceaccount.com',
                'path-to-private-key.json',
                project='engr-e516',
                datacenter='us-west1-b')

#Lists my one f1-micro node.
print(driver.list_nodes())

nodes = driver.list_nodes()
node = nodes[0]

#Start the node...
driver.ex_start_node(node=node)
nodes = driver.list_nodes()
node = nodes[0]
print(node.state)

#And stop the node...
driver.ex_stop_node(node=node)
nodes = driver.list_nodes()
node = nodes[0]
print(node.state)
