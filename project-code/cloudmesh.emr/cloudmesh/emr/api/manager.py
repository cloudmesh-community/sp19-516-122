import boto3
from cloudmesh.management.configuration.config import Config

class Manager(object):

    def __init__(self):
        return

    def list(self, parameter):
        print("list", parameter)

    def get_client(self):
        configs = Config()

        key_id = configs['cloudmesh.cloud.aws.credentials.EC2_ACCESS_ID']
        access_key = configs['cloudmesh.cloud.aws.credentials.EC2_SECRET_KEY']
        region = configs['cloudmesh.cloud.aws.credentials.region']

        client = boto3.client('emr', region_name=region,
                              aws_access_key_id=key_id,
                              aws_secret_access_key=access_key)
        return client

    def parse_options(self, options, states):
        result = []

        if 'all' not in options:
            for option in options:
                if option in states:
                    result += [states[option]]
        return result


    def list_clusters(self, args):
        client = self.get_client()

        options = args['status']
        opt_states = {'start': 'STARTING', 'boot': 'BOOTSTRAPPING', 'run': 'RUNNING', 'wait': 'WAITING',
                      'terminating': 'TERMINATING', 'shutdown': 'TERMINATED', 'error': 'TERMINATED_WITH_ERRORS'}

        cluster_state = self.parse_options(options, opt_states)
        results = client.list_clusters(ClusterStates=cluster_state)

        return results['Clusters']

    def list_instances(self, args):
        client = self.get_client()

        options = args['status']
        opt_states = {'start': 'AWAITING_FULFILLMENT', 'provision': 'PROVISIONING', 'boot': 'BOOTSTRAPPING',
                      'run': 'RUNNING', 'down': 'TERMINATED'}
        instance_state = self.parse_options(options, opt_states)

        options = args['type']
        opt_types = {'master': 'MASTER', 'core': 'CORE', 'task': 'TASK'}
        instance_types = self.parse_options(options, opt_types)

        results = client.list_instances(ClusterId=args['<CLUSTERID>'], InstanceGroupTypes=instance_types,
                                        InstanceStates=instance_state)
        return results['Instances']

    def describe_cluster(self, args):
        client = self.get_client()
        results = client.describe_cluster(ClusterId=args['<CLUSTERID>'])

        return results['Cluster']

    def stop_cluster(self, args):
        client = self.get_client()
        client.terminate_job_flows(JobFlowIds=[args['<CLUSTERID>']])

        results = {"cloud": "aws", "kind": "emr", "name": args['<CLUSTERID>'], "status": "Shutting down"}
        return results

    def start_cluster(self, args):
        client = self.get_client()

        setup = {'MasterInstanceType': args['master'], 'SlaveInstanceType': args['node'],
                 'InstanceCount': int(args['count']), 'KeepJobFlowAliveWhenNoSteps': True,
                 'TerminationProtected': False}

        bootstrap = [{'Name': 'Maximize Spark Default Config', 'ScriptBootstrapAction':
                     {'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config'}}]

        results = client.run_job_flow(Name=args['<NAME>'], ReleaseLabel="emr-5.21.0", Instances=setup,
                                      Applications=[{'Name': 'Spark'}, {'Name': 'Hadoop'}], VisibleToAllUsers=True,
                                      JobFlowRole='EMR_EC2_DefaultRole', ServiceRole='EMR_DefaultRole',
                                      BootstrapActions=bootstrap)

        return {"cloud": "aws", "kind": "emr", "cluster": results['JobFlowId'], "name": args['<NAME>'],
                "status": "Starting"}

