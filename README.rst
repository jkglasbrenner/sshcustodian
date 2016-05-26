=============
SSH Custodian
=============

This module depends on the Custodian class in the `custodian project
<https://github.com/materialsproject/custodian>`_, which is a wrapper that
manages jobs running on computing clusters. The custodian module is part of
`The Materials Project <http://materialsproject.org/>`_.

This module extends the Custodian class by creating the subclass SSHCustodian,
which adds the functionality to copy the temporary directory created via monty
to the scratch partitions on slave compute nodes, provided that the cluster's
file-system is configured in this way. The implementation invokes a sub-process
to utilize the ssh executable installed on the cluster, so it is not
particularly elegant or platform independent, nor is this solution likely to be
general to all clusters. This is why this modification has not been submitted
as a pull request to the main Custodian project.

You use SSHCustodian in the same way as the Custodian class, and it should
integrate in with your existing scripts. The SSHCustodian class takes two
additional arguments when creating a new instance, ``scratch_dir_node_only``
and ``pbs_nodefile``::
  
  scratch_dir_node_only (bool): If set to True, custodian will grab the list
      of nodes in the file path provided to pbs_nodefile and use copy the
      temp_dir to the scratch_dir on each node over ssh. This is necessary on
      cluster setups where each node has its own independent scratch
      partition.
      
  pbs_nodefile (str): The filepath to the list of nodes to be used in a
      calculation. If this path does not point to a valid file, then
      scratch_dir_node_only will be automatically set to False.

The subclass SSHVaspJob was also created, which overrides the setup method to
also check the environment variable ``PBS_NUM_PPN`` when ``auto_npar =
True``. This is necessary to implement for using compute node scratch
partitions on PBS-based queueing systems, as the generic method of using
``multiprocessing.cpu_count()`` to count the number of cores will include
hyperthreads, which will overestimate the number of physical cores and lead to
NPAR being set too large. One consequence of setting NPAR too high is that a
VASP job will hang, consuming resources but not doing anything useful. If you
are using this kind of scratch directory, be careful about setting NPAR.

On many clusters, the filepath for the list of compute nodes is in the
environment variable ``PBS_NODEFILE``, which can be accessed in bash as
``$PBS_NODEFILE`` and in python using the ``os`` module. An example of
how SSHCustodian can be used in a script is the following::

  import logging
  import os
  from sshcustodian.sshcustodian import SSHCustodian
  from custodian.vasp.handlers import (VaspErrorHandler,
                                       UnconvergedErrorHandler,
                                       MeshSymmetryErrorHandler,
                                       NonConvergingErrorHandler,
                                       PotimErrorHandler)
  from custodian.vasp.validators import VasprunXMLValidator
  from sshcustodian.vasp.sshjobs import SSHVaspJob
  from pymatgen.io.vasp import VaspInput

  FORMAT = '%(asctime)s %(message)s'
  logging.basicConfig(format=FORMAT, level=logging.INFO, filename="run.log")

  class VaspInputArgs:
      def __init__(self):
          """
          Set the default values for running a VASP job.
          """
          self.static_kpoint = 1
      
      def import_dict(self, in_dict):
          """
          Create and update self variables using dictionary.
          """
          for (key, value) in iteritems(in_dict):
              if key == "command":
                  self.command = value
              if key == "static_kpoint":
                  self.static_kpoint = value
              if key == "jobs":
                  self.jobs = value
           

  def get_runs(args):
      vasp_command = args.command.split()
      njobs = len(args.jobs)
      for i, job in enumerate(args.jobs):
          final = False if i != njobs - 1 else True
          if any(c.isdigit() for c in job):
              suffix = "." + job
          else:
              suffix = ".{}{}".format(job, i + 1)
          settings = []
          backup = True if i == 0 else False
          copy_magmom = False
          vinput = VaspInput.from_directory(".")
          if i > 0:
              settings.append(
                  {"file": "CONTCAR",
                   "action": {"_file_copy": {"dest": "POSCAR"}}})
          job_type = job.lower()
          auto_npar = True
          if job_type.startswith("static"):
              m = [i * args.static_kpoint for i in vinput["KPOINTS"].kpts[0]]
              settings.extend([
                  {"dict": "INCAR",
                   "action": {"_set": {"NSW": 0}}},
                  {'dict': 'KPOINTS',
                   'action': {'_set': {'kpoints': [m]}}}])
      
          yield SSHVaspJob(vasp_command, final=final, suffix=suffix,
                           backup=backup, settings_override=settings,
                           copy_magmom=copy_magmom, auto_npar=auto_npar)


  logging.info("Handlers used are %s" % args.handlers)
  scratch_root = os.path.abspath("/scratch")
  pbs_nodefile = os.environ.get("PBS_NODEFILE")
  job_args = VaspInputArgs()
  job_dict = {"command": "pvasp",
              "jobs": ["static"]}
  job_args.import_dict(job_dict)
  handlers = [VaspErrorHandler(), MeshSymmetryErrorHandler(),
              UnconvergedErrorHandler(), NonConvergingErrorHandler(),
              PotimErrorHandler()]
  validators = [VasprunXMLValidator()]
  c = SSHCustodian(handlers, get_runs(job_args), validators,
                   checkpoint=True,
                   scratch_dir=scratch_root,
                   scratch_dir_node_only=True,
                   pbs_nodefile=pbs_nodefile)
  c.run()

Note that depending on how your cluster is configured, the ``"command":
"pvasp"`` will need to be changed to however you invoke a parallel job.

For further information on how to use custodian, consult the `custodian project
documentation <https://pythonhosted.org/custodian/>`_.
