from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

google = get_driver(Provider.GCE)
driver = google('743305692107-compute@developer.gserviceaccount.com',
                'C:\\Users\\aduer\\Desktop\\untitled\\engr-e516-f6b9d3eb8a2f.json',
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
