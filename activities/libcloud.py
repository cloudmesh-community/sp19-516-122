import pprint as pp

from cloudmesh.management.configuration.config import Config

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

config = Config("C:\\Users\\aduer\\Desktop\\untitled\\cloudmesh4.yaml")
print(config['cloudmesh.cluster.gce'])

google = get_driver(Provider.GCE)
driver = google(config['cloudmesh.cluster.gce.account'],
                config['cloudmesh.cluster.gce.publickey'],
                project=config['cloudmesh.cluster.gce.project'],
                datacenter=config['cloudmesh.cluster.gce.datacenter'])

# Loop through all available images.
# Images include flavors. For example:
# <NodeImage: id=6619680254813170149, name=ubuntu-1810-cosmic-v20190212, driver=Google Compute Engine  ...>
# <NodeImage: id=7943564659214147674, name=ubuntu-minimal-1810-cosmic-v20190212, driver=Google Compute Engine  ...>
images = driver.list_images()
for image in images:
    pp.pprint(image)

nodes = driver.list_nodes()
for node in nodes:
    pp.pprint(node)

# Lists my one f1-micro node.
print(driver.list_nodes())

nodes = driver.list_nodes()
node = nodes[0]

# Start the node...
driver.ex_start_node(node=node)
nodes = driver.list_nodes()
node = nodes[0]
print(node.state)

# And stop the node...
driver.ex_stop_node(node=node)
nodes = driver.list_nodes()
node = nodes[0]
print(node.state)

del driver
