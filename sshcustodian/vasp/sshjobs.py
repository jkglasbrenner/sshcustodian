# File: sshcustodian/vasp/sshjobs.py
# -*- coding: utf-8 -*-

# Python 2/3 Compatibility
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

"""
Modifies custodian.vasp.jobs() to also check the PBS_NUM_PPN environment
variable.  This is necessary if you plan to use scratch partitions on compute
nodes in combination with auto_npar = True, as multiprocessing.cpu_count() also
counts hyperthreads and will set NPAR to be too large.
"""

# Import modules
import os
import shutil
import math

from pymatgen.io.vasp import Incar
from pymatgen.io.smart import read_structure

from custodian.vasp.interpreter import VaspModder
from custodian.vasp.jobs import VaspJob


VASP_INPUT_FILES = {"INCAR", "POSCAR", "POTCAR", "KPOINTS"}

VASP_OUTPUT_FILES = ['DOSCAR', 'INCAR', 'KPOINTS', 'POSCAR', 'PROCAR',
                     'vasprun.xml', 'CHGCAR', 'CHG', 'EIGENVAL', 'OSZICAR',
                     'WAVECAR', 'CONTCAR', 'IBZKPT', 'OUTCAR']


class SSHVaspJob(VaspJob):
    """
    Overrides the setup() method in VaspJob to also check the environment
    variable PBS_NUM_PPN when auto_npar = True. Note that counting using
    multiprocessing.cpu_count() includes hyperthreads, which will set NPAR to
    be too large. This can cause jobs to hang when using the scratch partitions
    on compute nodes.
    """
    __doc__ += VaspJob.__doc__

    def setup(self):
        """
        Method is identical to custodian.vasp.jobs.setup(), except that the
        environment variable PBS_NUM_PPN is checked first when auto_npar =
        True.
        """
        files = os.listdir(".")
        num_structures = 0
        if not set(files).issuperset(VASP_INPUT_FILES):
            for f in files:
                try:
                    struct = read_structure(f)
                    num_structures += 1
                except:
                    pass
            if num_structures != 1:
                raise RuntimeError("{} structures found. Unable to continue."
                                   .format(num_structures))
            else:
                self.default_vis.write_input(struct, ".")

        if self.backup:
            for f in VASP_INPUT_FILES:
                shutil.copy(f, "{}.orig".format(f))

        if self.auto_npar:
            try:
                incar = Incar.from_file("INCAR")
                # Only optimized NPAR for non-HF and non-RPA calculations.
                if not (incar.get("LHFCALC") or incar.get("LRPA") or
                        incar.get("LEPSILON")):
                    if incar.get("IBRION") in [5, 6, 7, 8]:
                        # NPAR should not be set for Hessian matrix
                        # calculations, whether in DFPT or otherwise.
                        del incar["NPAR"]
                    else:
                        import multiprocessing
                        # try pbs environment variable first
                        # try sge environment variable second
                        # Note!
                        # multiprocessing.cpu_count() will include hyperthreads
                        # in the CPU count, which will set NPAR to be too large
                        # and can cause the job to hang if you use compute
                        # nodes with scratch partitions.
                        ncores = (os.environ.get("PBS_NUM_PPN") or
                                  os.environ.get('NSLOTS') or
                                  multiprocessing.cpu_count())
                        ncores = int(ncores)
                        for npar in range(int(math.sqrt(ncores)),
                                          ncores):
                            if ncores % npar == 0:
                                incar["NPAR"] = npar
                                break
                    incar.write_file("INCAR")
            except:
                pass

        if self.settings_override is not None:
            VaspModder().apply_actions(self.settings_override)

    @staticmethod
    def get_jobtype():
        return "vasp"

    # Inherit docstrings
    setup.__doc__ += VaspJob.setup.__doc__
