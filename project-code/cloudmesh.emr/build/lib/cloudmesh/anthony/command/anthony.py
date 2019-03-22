from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.anthony.api.manager import Manager
from cloudmesh.common.console import  Console
from cloudmesh.common.util import path_expand
from pprint import pprint

class AnthonyCommand(PluginCommand):
    # noinspection PyUnusedLocal
    @command
    def do_anthony(self, args, arguments):
        """
        ::

          Usage:
                anthony list (clusters | instances)

          This command is used to interface with Amazon Web Service's
          Elastic Map Reduce (EMR) service to run Apache Spark jobs.
          It can start, list, and stop clusters and submit jobs to them.          

          Arguments:
              FILE   a file name

          Options:
              list      list currently running clusters, instances, fleets, or groups.
              upload    upload programs or data to an S3 bucket accessible to the cluster.

        """
        print(arguments)
#        arguments.FILE = arguments['--file'] or None
#
#        print(arguments)
#
#        m = Manager()
#
#        if arguments.FILE:
#            print("option a")
#            m.list(path_expand(arguments.FILE))
#        elif arguments.list:
#            print("option b")
#            m.list("just calling list without parameter")
#        elif arguments.put:
#            print("option c")
#
#        Console.error("This is just a sample5")
#        return ""
