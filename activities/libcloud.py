import pprint as pp

from cloudmesh.management.configuration.config import Config

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

#
# note this is not how we would initialize from config
#
config = Config("C:\\Users\\aduer\\Desktop\\untitled\\cloudmesh.yaml")

google = get_driver(Provider.GCE)
driver = google(config['cloudmesh.cloud.gce.credentials.clientid'],
                config['cloudmesh.cloud.gce.credentials.clientsecret'],
                config['cloudmesh.cloud.gce.credentials.project'],
                config['cloudmesh.cloud.gce.credentials.datacenter'])

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
