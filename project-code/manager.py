import boto3
import sys

from cloudmesh.management.configuration.config import Config


class Manager(object):

    def __init__(self):
        return

    def get_client(self):
        configs = Config()

        key_id = configs['cloudmesh.cloud.aws.credentials.EC2_ACCESS_ID']
        access_key = configs['cloudmesh.cloud.aws.credentials.EC2_SECRET_KEY']
        region = configs['cloudmesh.cloud.aws.default.region']

        client = boto3.client('emr', region_name=region,
                              aws_access_key_id=key_id,
                              aws_secret_access_key=access_key)

        return client

    def list_clusters(self, args):
        client = self.get_client()

        input_states = ['--start', '--boot', '--run', '--wait', '--terminating', '--down', '--error']
        states = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING', 'TERMINATED',
                  'TERMINATED_WITH_ERRORS']

        cluster_state = []
        for input_state, state in zip(input_states, states):
            if args[input_state]:
                cluster_state += [state]

        results = client.list_clusters(ClusterStates=cluster_state)

        return results['Clusters']

    def list_instances(self, args):
        client = self.get_client()

        input_states = ['--start', '--provision', '--boot', '--run', '--down']
        states = ['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING', 'RUNNING', 'TERMINATED',]

        instance_state = []
        for input_state, state in zip(input_states, states):
            if args[input_state]:
                instance_state += [state]

        input_types = ['--master', '--core', '--task']
        types = ['MASTER', 'CORE', 'TASK']

        instance_type = []
        for input_type, server_type in zip(input_types, types):
            if args[input_type]:
                instance_type += [server_type]

        results = client.list_instances(ClusterId=args['<clusterid>'], InstanceGroupTypes=instance_type,
                                        InstanceStates=instance_state)

        return results['Instances']

    def describe_cluster(self, args):
        client = self.get_client()
        results = client.describe_cluster(ClusterId=args['<clusterid>'])
        print(results)
        return results['Cluster']

    def stop_cluster(self, args):
        client = self.get_client()

        client.terminate_job_flows(JobFlowIds=[args['<clusterid>']])
        return {"result" : "Shutdown request sent. You can check on its status using list or describe."}

    def start_cluster(self, args):
        client = self.get_client()

        setup = {'MasterInstanceType': args['--mastertype'], 'SlaveInstanceType': args['--nodetype'],
                 'InstanceCount': args['--count'], 'KeepJobFlowAliveWhenNoSteps': True, 'TerminationProtected': False}

        bootstrap = [{'Name': 'Maximize Spark Default Config', 'ScriptBootstrapAction':
                     {'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config'}}]

        result = client.run_job_flow(Name=args['<name>'], ReleaseLabel="emr-5.21.0", Instances=setup,
                                     Applications=[{'Name': 'Spark'}, {'Name': 'Hadoop'}], VisibleToAllUsers=True,
                                     JobFlowRole='EMR_EC2_DefaultRole', BootstrapActions=bootstrap)
        return {"ClusterId" : result['JobFlowId']}
