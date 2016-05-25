# File: sshcustodian.py
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division

import logging
import sys
import datetime
import os
from itertools import islice

from monty.tempfile import ScratchDir
from monty.shutil import gzip_dir
from monty.json import MontyEncoder
from monty.serialization import dumpfn

from custodian import Custodian


logger = logging.getLogger(__name__)


class SSHCustodian(Custodian):
    """
    """
    __doc__ += Custodian.__doc__

    def __init__(self, handlers, jobs, validators=None, max_errors=1,
                 polling_time_step=10, monitor_freq=30,
                 skip_over_errors=False, scratch_dir=None,
                 gzipped_output=False, checkpoint=False):
        """
        """
        super(SSHCustodian, self).__init__(self, handlers, jobs, validators,
                                           max_errors, polling_time_step,
                                           monitor_freq, skip_over_errors,
                                           scratch_dir, gzipped_output,
                                           checkpoint)

    def run(self):
        """
        """
        cwd = os.getcwd()

        with ScratchDir(self.scratch_dir, create_symbolic_link=True,
                        copy_to_current_on_exit=True,
                        copy_from_current_on_enter=True) as temp_dir:
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
            except super(SSHCustodian, self).CustodianError as ex:
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
                if self.gzipped_output:
                    gzip_dir(".")

            # Cleanup checkpoint files (if any) if run is successful.
            super(SSHCustodian, self)._delete_checkpoints(cwd)

        return self.run_log

    # Inherit Custodian docstrings
    __init__.__doc__ = Custodian.__init__.__doc__ + __init__.__doc__
    run.__doc__ = Custodian.run.__doc__
