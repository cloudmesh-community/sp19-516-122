from __future__ import print_function
from cloudmesh.shell.command import command, map_parameters
from cloudmesh.shell.command import PluginCommand
from cloudmesh.emr.api.manager import Manager

from cloudmesh.common.Printer import Printer


'''
Todo - upload program to S3. upload data to S3. Copy data from S3 to local. Copy data from local to S3.
       Run/Submit Jobs.
'''

class EmrCommand(PluginCommand):

    # noinspection PyUnusedLocal
    @command
    def do_emr(self, args, arguments):
        """
        ::

        Usage:
            emr list clusters [--status=STATUS...] [--format=FORMAT]
            emr list instances <CLUSTERID> [--status=STATUS...] [--type=TYPE...] [--format=FORMAT]
            emr describe <CLUSTERID>
            emr stop <CLUSTERID>
            emr start <NAME> [--master=MASTER] [--node=NODE] [--count=COUNT]


        This command is used to interface with Amazon Web Services
        Elastic Map Reduce (EMR) service to run Apache Spark jobs.
        It can start, list, and stop clusters and submit jobs to them.

        Arguments:
            CLUSTERID               The AWS Cluster ID.

        Options:
            --status=STATUS         The status to search for.  [default: all]
            --type=TYPE             The type of instance to search for.  [default: all]
            --format=FORMAT         How to format the output.  [default: table]
            --master=MASTER         The type of server to use for the master node. [default: m1.medium]
            --node=NODE             The type of server to use for the worker nodes. [default: m1.medium]
            --count=COUNT           The number of servers to use [default: 3]

        Description:
            emr list clusters [--status=STATUS] [--format=FORMAT]
                Lists all clusters viewable to the credentials with a given status [default: all]. Valid statuses are:
                start, boot, run, wait, terminating, shutdown, and error.
            emr list instances [--status=STATUS...] [--format=FORMAT]
                Lists all instances viewable to the credentials with a given status [default: all}. Valid statuses are:
                start, provision, boot, run, down. Valid types are: master, core, and task.
            emr describe <CLUSTERID>
                Describes a cluster. Lists its status, region, type, etc.
            emr stop <CLUSTERID>
                Stops a cluster. Once a shutdown is initiated, it cannot be undone.
            emr start <NAME> [--master=MASTER] [--node=NODE] [--count=COUNT]
                Starts a cluster with a given name, number of servers, and server type. Bootstraps with Hadoop and
                Spark.
        """

        map_parameters(arguments, 'status', 'format', 'type', 'master', 'node', 'count')
        #print(arguments)

        emr = Manager()

        if arguments['list'] and arguments['clusters']:
            clusters = emr.list_clusters(arguments)
            if len(clusters) == 0:
                print("No clusters were found.")
            else:
                print(Printer.flatwrite(clusters,
                                        sort_keys=["Id"],
                                        order=["Id", "Name", "Status.State", "Status.StateChangeReason.Code",
                                               "Status.StateChangeReason.Message", "NormalizedInstanceHours"],
                                        header=["ID", "Name", "State", "State Reason", "State Message", "Hours"],
                                        output=arguments['format']))
        elif arguments['list'] and arguments['instances']:
            instances = emr.list_instances(arguments)

            if len(instances) == 0:
                print("No instances were found.")
            else:
                print(Printer.flatwrite(instances,
                                        sort_keys=["Id"],
                                        order=["Id", "Status.State", "Status.StateChangeReason.Code",
                                               "Status.StateChangeReason.Message", "Market", "InstanceType"],
                                        header=["ID", "State", "State Reason", "State Message", "Market",
                                                "Instance Type"],
                                        output=arguments['format']))
        elif arguments['describe']:
            cluster = emr.describe_cluster(arguments)

            # Fixing formatting.
            apps = ""
            for application in cluster["Applications"]:
                apps += application["Name"] + " " + application["Version"] + ", "
            apps = apps[:-2]
            cluster["Applications"] = apps
            cluster = [cluster]

            print(Printer.flatwrite(cluster,
                                    sort_keys=["Id"],
                                    order=["Id", "Name", "Status.State", "Status.StateChangeReason.Code",
                                           "Status.StateChangeReason.Message",
                                           "Ec2InstanceAttributes.Ec2AvailabilityZone", "InstanceCollectionType",
                                           "NormalizedInstanceHours", "Applications"],
                                    header=["ID", "Name",  "State", "State Reason", "State Message", "Region",
                                            "Type", "Instance Hours", "Applications"],
                                    output=arguments['format']))
        elif arguments['stop']:
            cluster = emr.stop_cluster(arguments)
            print(cluster['name'] + ": " + cluster["status"])
        elif arguments['start']:
            cluster = emr.start_cluster(arguments)
            print(cluster['name'] + ": " + cluster['cluster'] + " " + cluster["status"])

        return ""

