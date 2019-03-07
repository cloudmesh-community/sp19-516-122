from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.anthony.api.manager import Manager
from cloudmesh.common.console import  Console
from cloudmesh.common.util import path_expand
from pprint import pprint

import boto3
from cloudmesh.management.configuration.config import Config
from prettytable import PrettyTable

class AnthonyCommand(PluginCommand):
    # noinspection PyUnusedLocal
    @command
    def do_anthony(self, args, arguments):
        """
        ::

          Usage:
                anthony list (--clusters | --instances=<ClusterID>)

          This command is used to interface with Amazon Web Service's
          Elastic Map Reduce (EMR) service to run Apache Spark jobs.
          It can start, list, and stop clusters and submit jobs to them.

          Arguments:


          Options:

        """
        print(arguments)

        configs = Config()
        #ToDo: Change region to use Config()
        client = boto3.client('emr', region_name='us-west-1',
                              aws_access_key_id=configs['cloudmesh.cloud.aws.credentials.EC2_ACCESS_ID'],
                              aws_secret_access_key=configs['cloudmesh.cloud.aws.credentials.EC2_SECRET_KEY'])

        if arguments['list'] and arguments['--clusters']:
            clusters = client.list_clusters()
            if len(clusters)==0:
                print("No clusters appear to have been created.")
            else:
                t = PrettyTable(['ID', 'Name', 'Status'])
                for cluster in clusters['Clusters']:
                    t.add_row([cluster['Id'], cluster['Name'], cluster['Status']['State']])
                print(t)
        elif arguments['list'] and arguments['--instances'] is not None:
            instances = client.list_instances(ClusterId=arguments['--instances'])
            if len(instances)==0:
                print("No instances appear to have been created.")
            else:
                t = PrettyTable(['ID', 'Type', 'Status', 'Market'])
                for instance in instances['Instances']:
                    t.add_row([instance['Id'], instance['InstanceType'], instance['Status']['State'],
                               instance['Market']])
                print(t)
