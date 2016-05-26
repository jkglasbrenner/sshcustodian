# File: sshcustodian/sshcustodian.py
# -*- coding: utf-8 -*-

# Python 2/3 Compatibility
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
from six.moves import filterfalse
"""
This module creates a subclass of the main Custodian class in the Custodian
project (github.com/materialsproject/custodian), which is a wrapper that
manages jobs running on computing clusters. The Custodian module is part of The
Materials Project (materialsproject.org/).

This subclass adds the functionality to copy the temporary directory created
via monty to the scratch partitions on slave compute nodes, provided that the
cluster's filesystem is configured in this way. The implementation invokes a
subprocess to utilize the ssh executable installed on the cluster, so it is not
particularly elegant or platform independent, nor is this solution likely to be
general to all clusters. This is why this modification has not been submitted
as a pull request to the main Custodian project.
"""

# Import modules
import logging
import subprocess
import sys
import datetime
import os
from itertools import islice, groupby
from socket import gethostname

from monty.tempfile import ScratchDir
from monty.shutil import gzip_dir
from monty.json import MontyEncoder
from monty.serialization import dumpfn

from custodian.custodian import Custodian
from custodian.custodian import CustodianError

# Module-level logger
logger = logging.getLogger(__name__)


class SSHCustodian(Custodian):
    """
    The SSHCustodian class modifies the Custodian class from the custodian
    module to be able to handle clusters that have separate scratch partitions
    for each node. When scratch_dir_node_only is enabled, the temp_dir that
    monty creates will be copied to all other compute nodes used in the
    calculation and subsequently removed when the job is finished.
    """
    __doc__ += Custodian.__doc__

    def __init__(self, handlers, jobs, validators=None, max_errors=1,
                 polling_time_step=10, monitor_freq=30,
                 skip_over_errors=False, scratch_dir=None,
                 gzipped_output=False, checkpoint=False,
                 scratch_dir_node_only=False, pbs_nodefile=None):
        """    scratch_dir_node_only (bool): If set to True, custodian will grab
                the list of nodes in the file path provided to pbs_nodefile and
                use copy the temp_dir to the scratch_dir on each node over
                ssh. This is necessary on cluster setups where each node has
                its own independent scratch partition.
            pbs_nodefile (str): The filepath to the list of nodes to be used in
                a calculation. If this path does not point to a valid file,
                then scratch_dir_node_only will be automatically set to False.
        """
        super(SSHCustodian, self).__init__(handlers, jobs, validators,
                                           max_errors, polling_time_step,
                                           monitor_freq, skip_over_errors,
                                           scratch_dir, gzipped_output,
                                           checkpoint)
        self.hostname = gethostname()
        if pbs_nodefile is None:
            self.scratch_dir_node_only = False
            self.slave_compute_node_list = None
        elif os.path.exists(pbs_nodefile):
            self.scratch_dir_node_only = scratch_dir_node_only
            self.pbs_nodefile = pbs_nodefile
            self.slave_compute_node_list = (
                self._process_pbs_nodefile(self.pbs_nodefile, self.hostname))
        else:
            self.scratch_dir_node_only = False
            self.pbs_nodefile = None
            self.slave_compute_node_list = None

    @staticmethod
    def _process_pbs_nodefile(pbs_nodefile, hostname):
        with open(pbs_nodefile) as in_file:
            nodelist = in_file.read().splitlines()
        slave_compute_node_list = [
            node for node, _ in groupby(filterfalse(lambda x: x == hostname,
                                                    nodelist))
        ]
        return slave_compute_node_list

    def _copy_to_slave_node_dirs(self, temp_dir_path):
        """
        Copy temporary directory from head node to other nodes.

        Args:
            temp_dir_path (str): The path to the temporary scratch directory.
                It is assumed here that the root path of the scratch directory
                is the same on all nodes.
        """
        process_list = []
        for node in self.slave_compute_node_list:
            command = ['rsync', '-azhq', temp_dir_path,
                       '{0}:{1}'.format(node,
                                        os.path.abspath(self.scratch_dir))]
            p = subprocess.Popen(command, shell=False)
            process_list.append(p)
        # Wait for syncing to finish before moving on
        for process in process_list:
            process.wait()

    def _delete_slave_node_dirs(self, temp_dir_path):
        """
        Delete the temporary directory on the non-head nodes.

        Args:
            temp_dir_path (str): The path to the temporary scratch directory.
                It is assumed here that the root path of the scratch directory
                is the same on all nodes.
        """
        process_list = []
        for node in self.slave_compute_node_list:
            command = 'ssh {0} "rm -rf {1}"'.format(node, temp_dir_path)
            p = subprocess.Popen(command, shell=True)
            process_list.append(p)
        # Wait for deletion to finish before moving on
        for process in process_list:
            process.wait()

    def _manage_node_scratch(self, temp_dir_path, job_start):
        """
        Checks whether the user wants to make use of scratch partitions on each
        compute node, and if True, either copies the temporary directory to or
        deletes the temporary directory from each slave compute node. If the
        user does not specify to use node-specific scratch partitions, then the
        function does nothing.

        Args:
            temp_dir_path (str): The path to the temporary scratch directory.
            job_start (bool): If True, then the job has started and the
                temporary directory will be copied to the slave compute
                nodes. If False, then the temporary directories will be deleted
                from the slave compute nodes.
        """
        if self.scratch_dir_node_only:
            if job_start:
                self._copy_to_slave_node_dirs(temp_dir_path)
            else:
                self._delete_slave_node_dirs(temp_dir_path)
        else:
            pass

    def run(self):
        """
        Override of Custodian.run() to include instructions to copy the
        temp_dir to the scratch partition on slave compute nodes if requested.
        """
        cwd = os.getcwd()

        with ScratchDir(self.scratch_dir, create_symbolic_link=True,
                        copy_to_current_on_exit=True,
                        copy_from_current_on_enter=True) as temp_dir:
            self._manage_node_scratch(temp_dir_path=temp_dir,
                                      job_start=True)
            self.total_errors = 0
            start = datetime.datetime.now()
            logger.info("Run started at {} in {}.".format(
                start, temp_dir))
            v = sys.version.replace("\n", " ")
            logger.info("Custodian running on Python version {}".format(v))

            try:
                # skip jobs until the restart
                for job_n, job in islice(enumerate(self.jobs, 1),
                                         self.restart, None):
                    self._run_job(job_n, job)
                    # Checkpoint after each job so that we can recover from
                    # last point and remove old checkpoints
                    if self.checkpoint:
                        super(SSHCustodian, self)._save_checkpoint(cwd, job_n)
            except CustodianError as ex:
                logger.error(ex.message)
                if ex.raises:
                    raise RuntimeError("{} errors reached: {}. Exited..."
                                       .format(self.total_errors, ex))
            finally:
                # Log the corrections to a json file.
                logger.info("Logging to {}...".format(super(SSHCustodian,
                                                            self).LOG_FILE))
                dumpfn(self.run_log, super(SSHCustodian, self).LOG_FILE,
                       cls=MontyEncoder, indent=4)
                end = datetime.datetime.now()
                logger.info("Run ended at {}.".format(end))
                run_time = end - start
                logger.info("Run completed. Total time taken = {}."
                            .format(run_time))
                self._manage_node_scratch(temp_dir_path=temp_dir,
                                          job_start=False)
                if self.gzipped_output:
                    gzip_dir(".")

            # Cleanup checkpoint files (if any) if run is successful.
            super(SSHCustodian, self)._delete_checkpoints(cwd)

        return self.run_log

    # Inherit Custodian docstrings
    __init__.__doc__ = Custodian.__init__.__doc__ + __init__.__doc__
    run.__doc__ = Custodian.run.__doc__
