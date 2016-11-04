#!/usr/bin/env python
from execo import Process, TaktukPut
from vm5k.engine import *
import string
import random
import os
from os.path import expanduser
from vm5k.utils import get_hosts_jobs, reboot_hosts
from execo import logger as exlog


class EcohamDeploy(vm5k_engine):
    def __init__(self):
        super(EcohamDeploy, self).__init__()
        self.options_parser.add_option("--s5k", dest="storage5k_path",
                                       type="string", default='storage5k.rennes.grid5000.fr:/data/yalforov_772041',
                                       help="maximum number of VMs")

    def _install_packages(self, setup, other_packages=None):
        """Installation of packages"""
        other_packages = other_packages.replace(',', ' ')
        logger.info('Installing extra packages \n%s',
                    style.emph(other_packages))

        cmd = 'export DEBIAN_MASTER=noninteractive ; ' + \
            'apt-get update && apt-get install -y --force-yes ' + \
            other_packages
        install_extra = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(install_extra)

    def _setup_storage5k(self, setup):
        logger.info('Mounting storage5k')
        cmd = '''mount %s /mnt''' % self.options.storage5k_path
        mount_storage5k = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(mount_storage5k)
        logger.info('Mounting storage5k ... Done')

    def _copy_packages(self, setup, filenames, dir_path=expanduser("~")):
        logger.info('Copying packages sourcecode to hosts')
        copy_files = setup.fact.get_fileput(setup.hosts, ['%s/%s' % (dir_path, filename) for filename in filenames]).run()
        setup._actions_hosts(copy_files)
        logger.info('Copying packages sourcecode to hosts ... Done')

    def _compile_ecoham(self, setup):
        logger.info('Compiling ECOHAM')
        cmd = '''
            rsync -avzP /mnt/ECOHAM5_git ~/ &&
            cd ECOHAM5_git &&
            ./CompileJob-cluster.sh TEST 0 &&
            ./CompileJob-cluster.sh TEST 2
        '''
        compile_ecoham = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(compile_ecoham)
        logger.info('Compiling ECOHAM ... Done')

    def _compile_library(self, setup, params):
        logger.info('Compiling %s' % params['name'])
        cmd = '''
            (tar xf {zipfile}) &&
            cd {dirname} &&
            (./configure --prefix=/usr/local/) &&
            make &&
            make install
        '''.format(**params)
        if params['name'] == 'netCDF-Fortran':
            cmd = '''export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH} && ''' + cmd
        compile_library = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(compile_library)
        logger.info('Compiling %s ... Done' % params['name'])

    def setup_hosts(self):
        """ """
        disks = ['/home/lnguyen/benchs_vms_with_postgres.qcow2']
        logger.info('Initialize Host deployment')

        setup = vm5k_deployment(vms=[], resources=self.resources,
                                env_name=self.options.env_name,
                                env_file=self.options.env_file)
        setup.fact = ActionFactory(remote_tool=TAKTUK,
                                   fileput_tool=CHAINPUT,
                                   fileget_tool=SCP)
        logger.info('Deploy hosts')
        setup.hosts_deployment()
        logger.info('Install packages')
        setup.packages_management(other_packages='sysstat,gfortran,git', launch_disk_copy=False)
        logger.info('Finished installing packages')
        self._setup_storage5k(setup)
        self._copy_packages(setup, ['zlib-1.2.8.tar.gz',
            'hdf5-1.8.17.tar',
            'netcdf-4.4.0.tar.gz',
            'netcdf-fortran-4.4.4.tar.gz',
            'mpich-3.2.tar.gz'])
        self._compile_library(setup,
            {
                'name': 'zlib',
                'url': 'http://zlib.net/zlib-1.2.8.tar.gz',
                'zipfile': 'zlib-1.2.8.tar.gz',
                'dirname': 'zlib-1.2.8'
            })

        self._compile_library(setup,
            {
                'name': 'HDF5',
                'url': 'http://www.hdfgroup.org/ftp/HDF5/current/src/hdf5-1.8.17.tar',
                'zipfile': 'hdf5-1.8.17.tar',
                'dirname': 'hdf5-1.8.17'
            })

        self._compile_library(setup,
            {
                'name': 'netCDF',
                'url': 'ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4.4.0.tar.gz',
                'zipfile': 'netcdf-4.4.0.tar.gz',
                'dirname': 'netcdf-4.4.0'
            })

        self._compile_library(setup,
            {
                'name': 'netCDF-Fortran',
                'url': 'https://github.com/Unidata/netcdf-fortran/archive/v4.4.4.tar.gz',
                'zipfile': 'netcdf-fortran-4.4.4.tar.gz',
                'dirname': 'netcdf-fortran-4.4.4'
            })

        self._compile_library(setup,
            {
                'name': 'High-Performance Portable MPI',
                'url': 'http://www.mpich.org/static/downloads/3.2/mpich-3.2.tar.gz',
                'zipfile': 'mpich-3.2.tar.gz',
                'dirname': 'mpich-3.2'
            })

    def run(self):
        self.force_options()
        # The argument is a cluster
        self.cluster = self.args[0]
        self.frontend = get_cluster_site(self.cluster)
        # Analyzing options
        if self.options.oar_job_id:
            self.oar_job_id = self.options.oar_job_id
        else:
            self.oar_job_id = None

        try:
            job_is_dead = False
            if self.oar_job_id is None:
                self.make_reservation()
            # Retrieving the hosts and subnets parameters
            self.get_resources()
            # Hosts deployment and configuration
            if not self.options.no_hosts_setup:
                self.setup_hosts()
            if len(self.hosts) == 0:
                logger.error('No host!!!')
                return

            # Initializing the resources and threads
            available_hosts = list(self.hosts)
            available_ip_mac = list(self.ip_mac)
            # threads = {}

            # Checking that the job is running and not in Error
            while get_oar_job_info(self.oar_job_id, self.frontend)['state'] != 'Error' \
                    or len(threads.keys()) > 0:
                job_is_dead = False
                # while self.options.n_nodes > len(available_hosts):
                #     tmp_threads = dict(threads)
                #     for t in tmp_threads:
                #         if not t.is_alive():
                #             available_hosts.extend(tmp_threads[t]['hosts'])
                #             available_ip_mac.extend(tmp_threads[t]['ip_mac'])
                #             del threads[t]
                #     sleep(5)
                if get_oar_job_info(self.oar_job_id, self.frontend)['state'] == 'Error':
                    job_is_dead = True
                    break
                # if job_is_dead:
                #     break

                # # Getting the next combination
                # comb = self.sweeper.get_next()
                # if not comb:
                #     while len(threads.keys()) > 0:
                #         tmp_threads = dict(threads)
                #         for t in tmp_threads:
                #             if not t.is_alive():
                #                 del threads[t]
                #         logger.info('Waiting for threads to complete')
                #         sleep(20)
                #     break

                used_hosts = available_hosts[0:self.options.n_nodes]
                available_hosts = available_hosts[self.options.n_nodes:]

                # n_vm = self.comb_nvm(comb)
                # used_ip_mac = available_ip_mac[0:n_vm]
                # available_ip_mac = available_ip_mac[n_vm:]

                # t = Thread(target=self.workflow,
                #            args=(comb, used_hosts, used_ip_mac))
                # threads[t] = {'hosts': used_hosts, 'ip_mac': used_ip_mac}
                # logger.debug('Threads: %s', len(threads))
                # t.daemon = True
                # t.start()
            print 'info', get_oar_job_info(self.oar_job_id, self.frontend)
            if get_oar_job_info(self.oar_job_id, self.frontend)['state'] == 'Error':
                job_is_dead = True

            if job_is_dead:
                self.oar_job_id = None

        finally:
            if self.oar_job_id is not None:
                if not self.options.keep_alive:
                    logger.info('Deleting job')
                    oardel([(self.oar_job_id, self.frontend)])
                else:
                    logger.info('Keeping job alive for debugging')


if __name__ == "__main__":
    engine = EcohamDeploy()
    engine.start()
