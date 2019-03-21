from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.EMR.api.manager import Manager
from cloudmesh.common.console import  Console
from cloudmesh.common.util import path_expand
from pprint import pprint

import boto3

from cloudmesh.management.configuration.config import Config
from prettytable import PrettyTable

'''  Todo - upload program to S3. upload data to S3. Copy data from S3 to local. Copy data from local to S3.
            Run/Submit Jobs.
'''


class EmrCommand(PluginCommand):
    # noinspection PyUnusedLocal
    @command
    def do_EMR(self, args, arguments):
        """
        ::

          Usage:
                EMR list clusters [--start --boot --run --wait --terminating --down --error]
                EMR list instances <clusterid> [--master --core --task]  [--start --provision --boot --run --down]
                EMR describe <clusterid>
                EMR start <name> [--count=N] [--mastertype=MTYPE] [--nodetype=STYPE]
                EMR stop <clusterid>

          This command is used to interface with Amazon Web Service's
          Elastic Map Reduce (EMR) service to run Apache Spark jobs.
          It can start, list, and stop clusters and submit jobs to them.

          Arguments:
            clusterid           The cluster ID to query

          Options:
            --start                 Search for clusters/nodes that are starting.
            --boot                  Search for clusters/nodes that are booting.
            --run                   Search for clusters/nodes that are running.
            --wait                  Search for clusters that are waiting.
            --terminate             Search for clusters that are shutting down.
            --down                  Search for clusters/nodes that are shutdown.
            --error                 Search for clusters that are in an error state.
            --provision             Search for nodes that are awaiting provisioning.
            --master                List only MASTER nodes.
            --core                  List only CORE nodes.
            --task                  List only TASK nodes.
            --count=<N>             The number of instances to launch [default: 3]
            --mastertype=MTYPE      The AWS instance type for the master node [default: m1.medium]
            --nodetype=STYPE        The AWS instance type for the worker node(s) [default: m1.medium]
        """
        #print(arguments)

        manager = Manager()

        if arguments['list'] and arguments['clusters']:
            clusters = manager.list_clusters(arguments)

            if len(clusters) == 0:
                print("No clusters were found.")
            else:
                t = PrettyTable(['ID', 'Name', 'State', 'Reason', 'Message', 'Hours'])
                for cluster in clusters:
                    status = cluster['Status']

                    t.add_row([cluster['Id'], cluster['Name'], cluster['Status']['State'],
                               status['StateChangeReason']['Code'], status['StateChangeReason']['Message'],
                               cluster['NormalizedInstanceHours']])
                print(t)
        elif arguments['list'] and arguments['instances']:
            instances = manager.list_instances(arguments)

            if len(instances) == 0:
                print("No instances were found.")
            else:
                t = PrettyTable(['ID', 'State', 'State Reason', 'State Message', 'Market', 'Type'])

                for instance in instances:
                    t.add_row([instance['Id'], instance['Status']['State'],
                               instance['Status']['StateChangeReason']['Code'],
                               instance['Status']['StateChangeReason']['Message'], instance['Market'],
                               instance['InstanceType']])

                print(t)
        elif arguments['describe']:
            cluster = manager.describe_cluster(arguments)

            t = PrettyTable(['Label', 'Value'])

            t.add_row(['ID', cluster['Id']])
            t.add_row(['Name', cluster['Name']])
            t.add_row(['State', cluster['Status']['State']])
            t.add_row(['Status', cluster['Status']['StateChangeReason']['Code']])
            t.add_row(['Status Code', cluster['Status']['StateChangeReason']['Message']])
            t.add_row(['Region', cluster['Ec2InstanceAttributes']['Ec2AvailabilityZone']])
            t.add_row(['Type', cluster['InstanceCollectionType']])
            t.add_row(['Instance Hours', cluster['NormalizedInstanceHours']])

            apps = ""
            for app in cluster['Applications']:
                apps += app['Name'] + " " + app['Version'] + " "
            apps = apps[:-1]

            t.add_row(['Applications', apps])
            print(t)
        elif arguments['stop']:
            result = manager.stop_cluster(arguments)
            print(result['result'])
        elif arguments['start']:
            result = manager.start_cluster(arguments)
            print(result['ClusterId'] + " is now starting. You can check on its status via --list.")
