from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.anthony.api.manager import Manager
from cloudmesh.common.console import Console
from cloudmesh.common.util import path_expand
from pprint import pprint

import boto3
from cloudmesh.management.configuration.config import Config
from prettytable import PrettyTable


class EmrCommand(PluginCommand):
    # noinspection PyUnusedLocal
    @command
    def do_emr(self, args, arguments):
        """
        ::

          Usage:
                emr list (--clusters | --instances=CLUSTERID)
                emr describe --cluster=CLUSTERID
                emr start --cluster=CLUSTERID
                emr stop --cluster=CLUSTERID
                emr upload [--program=PROGRAM | --data=DATA] --s3=BUCKET

          This command is used to interface with Amazon Web Service's
          Elastic Map Reduce (EMR) service to run Apache Spark jobs.
          It can start, list, and stop clusters and submit jobs to them.

          Arguments:
            CLUSTERID  The Amazon Cluster ID for the cluster being queried.
            CLUSTERNAME  The name to be given to a new cluster.
            PROGRAM  The Python program (*.py) to be uploaded to the cluster via S3 and run.
            DATA  The Data to be uploaded to the S3 bucket for processing.
            BUCKET  The S3 bucket ID to store programs or data.

          Description:
            emr
              Amazon Web Services's (AWS) Elastic Map Reduce service is managed via
              the emr command.

              The configuration file is read from ~/.cloudmesh/cloudmesh4.yaml and must contain the
              following keys with valid values:
                 cloudmesh.cloud.aws.credentials.EC2_ACCESS_ID
                 cloudmesh.cloud.aws.credentials.EC2_SECRET_KEY

              Once the Access ID and Secret Key are created, the following commands can be
              used to interface with EMR.

                emr list (--clusters | --instances=CLUSTERID)
                  List clusters its ID, name, and status. For instances, lists the ID, Type, Status, and Market.
                emr describe --cluster=CLUSTERID
                  Describe a cluster and lists the ID, Name, Status, Region, Type, and Instance Hours.
                emr start --cluster=CLUSTERNAME
                  Starts a cluster with a name set to CLUSTERNAME.
                emr stop --cluster=CLUSTERID
                  Stops a cluster with the given ID, if it exists.
                emr upload [--program=PROGRAM | --data=DATA] --s3=BUCKET
                  Uploads a program or data to an S3 bucket for use by the Spark cluster.

        """
        print(arguments)

        #Extract boto3 stuff out into provider class.
        configs = Config()
        client = boto3.client('emr', region_name='us-west-1',
                              aws_access_key_id=configs['cloudmesh.cloud.aws.credentials.EC2_ACCESS_ID'],
                              aws_secret_access_key=configs['cloudmesh.cloud.aws.credentials.EC2_SECRET_KEY'])

        if arguments['list'] and arguments['--clusters']:
            clusters = client.list_clusters()
            if len(clusters) == 0:
                print("No clusters appear to have been created.")
            else:
                t = PrettyTable(['ID', 'Name', 'Status'])
                for cluster in clusters['Clusters']:
                    t.add_row([cluster['Id'], cluster['Name'], cluster['Status']['State']])
                print(t)
        elif arguments['list'] and arguments['--instances'] is not None:
            instances = client.list_instances(ClusterId=arguments['--instances'])
            if len(instances) == 0:
                print("No instances appear to have been created.")
            else:
                t = PrettyTable(['ID', 'Type', 'Status', 'Market'])
                for instance in instances['Instances']:
                    t.add_row([instance['Id'], instance['InstanceType'], instance['Status']['State'],
                               instance['Market']])
                print(t)
        elif arguments['describe'] and arguments['--cluster'] is not None:
            description = client.describe_cluster(ClusterId=arguments['--cluster'])
            if len(description) == 0:
                print("No cluster not found. Please check the id.")
            else:
                t = PrettyTable(['Label', 'Value'])
                t.add_row(['ID', description['Cluster']['Id']])
                t.add_row(['Name', description['Cluster']['Name']])
                t.add_row(['Status', description['Cluster']['Status']['State']])
                t.add_row(['Region', description['Cluster']['Ec2InstanceAttributes']['Ec2AvailabilityZone']])
                t.add_row(['Type', description['Cluster']['InstanceCollectionType']])
                t.add_row(['Instance Hours', description['Cluster']['NormalizedInstanceHours']])

                apps = ""
                for app in description['Cluster']['Applications']:
                    apps += app['Name'] + " " + app['Version'] + " "
                apps = apps[:-1]

                t.add_row(['Applications', apps])
                print(t)
        elif arguments['stop'] and arguments['--cluster'] is not None:
            client.terminate_job_flows(JobFlowIds=[arguments['--cluster']])
            print("Shutdown request sent. You can check its status via --list.")
        elif arguments['start'] and arguments['--cluster'] is not None:

            setup = {'MasterInstanceType': 'm1.medium',
                     'SlaveInstanceType': 'm1.medium',
                     'InstanceCount': 3,
                     'KeepJobFlowAliveWhenNoSteps': True,
                     'TerminationProtected': False,
                     }

            response = client.run_job_flow(Name=arguments['--cluster'], ReleaseLabel="emr-5.21.0", Instances=setup,
                                           Applications=[{'Name': 'Spark'}, {'Name': 'Hadoop'}],
                                           VisibleToAllUsers=True, JobFlowRole='EMR_EC2_DefaultRole',
                                           ServiceRole='EMR_DefaultRole')
            print(response['JobFlowId'] + " is now starting. You can check on its status via --list.")
