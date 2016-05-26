# File: sshcustodian/vasp/sshjobs.py
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division

"""
"""

import os
import shutil
import math

from pymatgen.io.vasp import Incar
from pymatgen.io.smart import read_structure
from pymatgen.io.vasp.sets import MPVaspInputSet

from custodian.vasp.interpreter import VaspModder
from custodian.custodian.vasp import VaspJob


VASP_INPUT_FILES = {"INCAR", "POSCAR", "POTCAR", "KPOINTS"}

VASP_OUTPUT_FILES = ['DOSCAR', 'INCAR', 'KPOINTS', 'POSCAR', 'PROCAR',
                     'vasprun.xml', 'CHGCAR', 'CHG', 'EIGENVAL', 'OSZICAR',
                     'WAVECAR', 'CONTCAR', 'IBZKPT', 'OUTCAR']


class SSHVaspJob(VaspJob):
    """
    """

    def __init__(self, vasp_cmd, output_file="vasp.out", suffix="",
                 final=True, backup=True,
                 default_vasp_input_set=MPVaspInputSet(), auto_npar=True,
                 auto_gamma=True, settings_override=None,
                 gamma_vasp_cmd=None, copy_magmom=False):
        """
        """
        super(SSHVaspJob, self).__init__(vasp_cmd, output_file, suffix, final,
                                         backup, default_vasp_input_set,
                                         auto_npar, auto_gamma,
                                         settings_override, gamma_vasp_cmd,
                                         copy_magmom)

    def setup(self):
        """
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
                #Only optimized NPAR for non-HF and non-RPA calculations.
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
                        ncores = (os.environ.get("PBS_NUM_NODES") or
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
