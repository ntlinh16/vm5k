#!/usr/bin/env python
from vm5k.engine import *
import os
from datetime import datetime
from vm5k.utils import reboot_hosts
from execo import logger as exlog
from execo import TaktukPut, TaktukGet, Process


def parse_injectors(option, opt, value, parser):
    print option, opt, value, parser
    if isinstance(value, basestring):
        if ',' in value:
            setattr(parser.values, option.dest, value.split(','))
        else:
            setattr(parser.values, option.dest, [value])
    else:
        setattr(parser.values, option.dest, ['io'])
    print parser.values.load_injector
    if not set(parser.values.load_injector).issubset(set(['io', 'cpu', 'mem', 'pgbench', 'network', 'no_stress'])):
        parser.error('Injectors have to be the following: io, cpu, mem, pgbench, network or no_stress')


class ContainerBootMeasurement(vm5k_engine_para):
    def __init__(self):
        super(ContainerBootMeasurement, self).__init__()
        # self.n_nodes = 1
        self.options_parser.add_option("--vm", dest="n_vm",
                                       type="int", default=1,
                                       help="maximum number of VMs")
        self.options_parser.add_option("--cpu", dest="n_cpu",
                                       type="int", default=1,
                                       help="maximum number of CPUs")
        self.options_parser.add_option("--mem", dest="n_mem",
                                       type="int", default=1,
                                       help="maximum number of memory")
        self.options_parser.add_option("--host",
                                       dest="host",
                                       help="force host choice")
        self.options_parser.add_option("--injector",
                                       dest="load_injector",
                                       default=['cpu'],
                                       type="string",
                                       action="callback",
                                       callback=parse_injectors,
                                       help="""type of load stress methods to use:
                                            io, cpu, mem, pgbench, network or no_stress\ndefault to cpu""")
        self.options_parser.add_option("--io_scheduler",
                                       dest="io_scheduler",
                                       default='cfq',
                                       choices=['noop', 'deadline', 'cfq'],
                                       help="type of IO schedulers to use: noop, deadline, or cfq\ndefault to cfq")
        self.options_parser.add_option("--retry",
                                       dest="number_of_retries",
                                       default=7,
                                       help="set the number of retries for getting boot_time")
        self.options_parser.add_option("--renice",
                                       dest="renice",
                                       default=None,
                                       help="set the nice of coVMs")
        self.options_parser.add_option("--diskspeed",
                                       dest="dd",
                                       action="store_true",
                                       help="run dd disk speed check on eVMs")
        self.options_parser.add_option("--disk",
                                       dest="disk",
                                       default='hdd',
                                       choices=['hdd', 'ssd', 'ceph'],
                                       help="""type of disks to use: hdd, ssd, or ceph\n
                                            default to hdd""")
        self.options_parser.add_option("--ip",
                                       dest="ip",
                                       type="string",
                                       default=None,
                                       help="ip of iperf server to run network stress")
        self.options_parser.add_option("--band",
                                       dest="bandwidth",
                                       type="int",
                                       default=None,
                                       help="the maximum bandwidth to run network stress")
        self.options_parser.add_option("--multiboot",
                                       dest="multiboot",
                                       action="store_true",
                                       help="run multiboot on eCons")
        self.options_parser.add_option("--cephmaster",
                                       dest="ceph_master",
                                       type="string",
                                       default=None,
                                       help="the ceph master node hostname")

        self.disks = ['/home/lnguyen/image/benchs_vms.qcow2']
        self.vms = []

    def define_parameters(self):
        """Define the parameters you want to explore"""
        parameters = {
            'n_vm': range(1, self.options.n_vm + 1),
            'n_co_vms': range(0, 16),
            'n_mem': range(1, self.options.n_mem + 1),
            'n_cpu': range(1, self.options.n_cpu + 1),
            'cpu_sharing': [False],
            'cpu_policy': ['one_core'],
            'image_policy': ['one_per_vm'],
            # 'boot_policy': ['all_at_once', 'one_by_one', 'one_then_others'],
            'boot_policy': ['one_then_others'],
            # 'load_injector': ['cpu', 'mem', 'pgbench', 'network', 'no_stress'],
            'load_injector': self.options.load_injector,
            'iteration': range(1, 11)}
        if self.options.bandwidth:
            logger.info('Run network stress on HOST up to %sG bandwidths' % self.options.bandwidth)
            parameters['bandwidths'] = range(1, self.options.bandwidth + 1, 1)
            parameters['n_co_vms'] = range(0, 1)
        logger.info('Exploring the following parameters \n%s', pformat(parameters))
        excludes = ['n_nodes', 'n_cpu', 'n_vm', 'n_mem', 'load_injector', 'oar_job_id',
                    'env_file', 'log_level', 'no_hosts_setup', 'outofchart',
                    'output_mode', 'number_of_retries', 'merge_outputs', 'host',
                    'n_measure', 'env_name', 'backing_files']

        options = {key: val for key, val in self.options.__dict__.iteritems() if key not in excludes}
        logger.info('With the following options \n%s', pformat(options))
        return parameters

    def comb_nvm(self, comb):
        """Calculate the number of virtual machines in the combination,
        required to attribute a number of IP/MAC for a parameter combination
        """
        return comb['n_vm'] * (1 + comb['n_co_vms'])
        # return comb['n_vm'] * (2 + comb['n_co_vms'])

    def workflow(self, comb, hosts, ip_mac):
        """Perform a boot measurements on the VM """
        host = hosts[0]
        logger.info('hosts %s', host)
        logger.info('ip_mac %s', ip_mac)
        host_name = host.split('.')[0]
        n_cores_host = 16
        try:
            n_cores_host = get_host_attributes(host_name)['architecture']['smt_size']
            logger.info('Number of cores in host [%s] = %s' % (host_name, n_cores_host))
        except:
            logger.error('Cannot get number of cores from host [%s]' % host)
        thread_name = style.Thread(host.split('.')[0]) + ': '

        logger.info(thread_name + ' Number of VM / coVM / ipMAC %s %s %s',
                    str(comb['n_vm']), str(comb['n_co_vms']), str(len(ip_mac)))

        comb_ok = False
        too_many_VMS = False

        if comb['cpu_policy'] == 'one_by_core':
            if not comb['cpu_sharing']:
                if comb['n_vm'] + comb['n_co_vms'] * comb['n_vm'] > n_cores_host:
                    too_many_VMS = True

        try:
            if not too_many_VMS:
                logger.info(thread_name)
                logger.info(style.step(' Performing combination') + '\n' +
                            slugify(comb))

                logger.info(thread_name + 'Destroying all containers on hosts')
                # destroy_vms(hosts, undefine=True)
                self.destroy_containers(host)

                logger.info(thread_name + ' Create all containers on hosts')
                # vms = self.create_vms(comb, host, ip_mac)
                containers = [{'id': 'e-c-%s' % _} for _ in range(comb['n_vm'])]
                containers += [{'id': 'co-c-%s' % _} for _ in range(comb['n_co_vms'])]
                self.create_containers(comb, host, ip_mac)
                logger.info(thread_name + 'containers are ready to be started')

                e_containers = filter(lambda v: 'co-c' not in v['id'], containers)

                # get vm index for outputing ordered result
                e_containers_index = {vm['id']: str(index) for index, vm in enumerate(e_containers)}
                co_containers = filter(lambda v: 'co-c' in v['id'], containers)
                logger.info('Start coCon')
                for container in co_containers:
                    logger.info('Start container %s' % container)
                    self._start_container(container['id'], host)

                cmd = '''sync && echo 3 > /proc/sys/vm/drop_caches'''
                logger.info('clear cache of memory')
                run = TaktukRemote(cmd, host).run()
                sleep(1)

                logger.info('Sleep 20 seconds to wait for no I/O operation')
                sleep(20)

                logger.info(thread_name + '\nxp-containers: %s\nco-containers: %s' % (e_containers, co_containers))
                if len(co_containers) > 0:
                    if comb['load_injector'] == 'cpu':
                        self.stress_containers_cpu(co_containers, host)
                    elif comb['load_injector'] == 'io':
                        self.stress_containers_io(co_containers, host)
                    elif comb['load_injector'] == 'mem':
                        self.stress_containers_mem(co_containers, host)
                    elif comb['load_injector'] == 'network':
                        self.stress_containers_network(co_containers, host)
                    elif comb['load_injector'] == 'pgbench':
                        self.setup_containers_pgbench(co_containers, host)
                        self.stress_containers_pgbench(co_containers, host)

                if comb.get('bandwidths', None):
                    logger.info('[%s] Trigger network injector on HOST with bandwidth %s' %
                                (slugify(comb), comb['bandwidths']))
                    stress = self.network_stress_host(host, comb['bandwidths'])
                    stress.start()

                    logger.info('Sleep 20 seconds to wait for stress to reach its maximum')
                    sleep(20)

                # Boot eCons
                if self.options.multiboot:
                    logger.info('Trigger boot multiple eCons experiment: %s eCons' % comb['n_vm'])
                    boot_duration = self.get_multiple_container_boottime(e_containers, host)
                else:
                    logger.info('Getting containers boot time')
                    boot_duration = self.get_container_boottime(e_containers, host)

                speed = None

                if not self.save_results(host, comb, e_containers, e_containers_index, boot_duration, speed, self.hosts_speed.get(host)):
                    exit()
            else:
                logger.info(thread_name + slugify(comb) +
                            ' too many VMs impossible comb, comb_ok=%s' % comb_ok)
            comb_ok = True

        finally:
            if comb_ok:
                self.sweeper.done(comb)
                logger.info(thread_name + slugify(comb) + ' has been done')
            else:
                self.sweeper.cancel(comb)
                logger.warning(thread_name + slugify(comb) + ' has been canceled')
            logger.info(style.step('%s Remaining'), len(self.sweeper.get_remaining()))

    def _start_container(self, container_name, host):
        cmd = 'lxc-start -n %s' % container_name
        TaktukRemote(cmd, host).run()

    def create_containers(self, comb, host, ip_mac):
        if self.options.disk == 'ssd':
            rootfs_path = '/tmp/'
        elif self.options.disk == 'ceph':
            rootfs_path = '/tmp/data_%s/'
        else:
            rootfs_path = '/var/lib/lxc/'

        if comb['load_injector'] in ['cpu', 'io']:
            packages = 'lxc,stress'
        elif comb['load_injector'] in ['network']:
            packages = 'lxc,iperf3'
        else:
            packages = 'lxc,postgresql-9.4,postgresql-contrib-9.4'

        containers = ['e-c-%s' % _ for _ in range(comb['n_vm'])]
        containers += ['co-c-%s' % _ for _ in range(comb['n_co_vms'])]
        index = 0
        container = containers.pop()
        logger.info('Create the first container %s' % container)
        if self.options.disk == 'ceph':
            p = TaktukRemote('''lxc-create -n %s -t debian -B dir --dir %s -- -r jessie --packages=%s''' %
                             (container, rootfs_path % index, packages), host).run()
        else:
            p = TaktukRemote('''lxc-create -n %s -t debian -- -r jessie --packages=%s''' %
                             (container, packages), host).run()
        logger.info('Sleep 15 seconds to install and create the first container:%s' % container)
        sleep(15)
        path = '%s%s/rootfs/root/' % (rootfs_path, container)
        if self.options.disk == 'ceph':
            path = '%s/root/' % (rootfs_path % index)
        if comb['load_injector'] == 'mem':
            logger.info('Copy CacheBench to the first container %s' % container)
            p = TaktukRemote('''cp -r llcbench %s''' % (path), host).run()
        elif comb['load_injector'] == 'cpu':
            logger.info('Copy kflops to the first container %s' % container)
            p = TaktukRemote('''cp -r /root/kflops %s''' % (path), host).run()
        index += 1

        if self.options.disk == 'ceph':
            logger.info('Creating other containers %s in CEPH case' % containers)
            for i, c in enumerate(containers):
                p = TaktukRemote('''lxc-create -n %s -t debian -B dir --dir %s -- -r jessie --packages=%s''' %
                                 (c, rootfs_path % (i + 1), packages), host).run()
        else:
            logger.info('Cloning other containers %s' % containers)
            for c in containers:
                logger.info('Clone container: %s' % c)
                p = TaktukRemote('''lxc-copy -n %s -N %s''' % (container, c), host).run()
                sleep(4)

        if self.options.disk == 'ssd':
            rootfs_path = '/tmp/'
        else:
            rootfs_path = '/var/lib/lxc/'

        containers.append(container)
        logger.info('Pin CPU of containers to physical cores')
        if self.options.multiboot:
            for container in containers:
                if 'co-c-' not in container:
                    core = int(container.split('-')[-1]) + 1
                    logger.info('Pin CPU of e-container %s to core %s' % (container, core))
                    p = TaktukRemote("""echo 'lxc.cgroup.cpuset.cpus = %s' >> %s%s/config""" %
                                     (core, rootfs_path, container), host).run()
                    index += 1
                    sleep(1)
        elif comb['load_injector'] == 'cpu':
            for container in containers:
                logger.info('Pin CPU of container %s to core 0' % container)
                p = TaktukRemote("""echo 'lxc.cgroup.cpuset.cpus = 0' >> %s%s/config""" %
                                 (rootfs_path, container), host).run()
                index += 1
                sleep(1)
        elif comb['load_injector'] in ['io', 'mem', 'network']:
            for container in containers:
                if 'co-c-' not in container:
                    logger.info('Pin CPU of e-container %s to core 0' % container)
                    p = TaktukRemote("""echo 'lxc.cgroup.cpuset.cpus = 0' >> %s%s/config""" %
                                     (rootfs_path, container), host).run()
                    sleep(1)
                else:
                    core = int(container.split('-')[-1]) + 1
                    logger.info('Pin CPU of co-container %s to core %s' % (container, core))
                    p = TaktukRemote("""echo 'lxc.cgroup.cpuset.cpus = %s' >> %s%s/config""" %
                                     (core, rootfs_path, container), host).run()
                    sleep(1)
                index += 1

    def setup_containers_pgbench(self, containers, host):
        logger.info('Sleeping 30 s for starting postgres')
        sleep(30)
        for container in containers:
            logger.info('Init pgbench for container: %s' % container['id'])
            cmd = """lxc-attach -n %s -- su - postgres -c 'createdb benchdb'""" % container['id']
            TaktukRemote(cmd, host).run()
            sleep(7)
            cmd = """lxc-attach -n %s -- su - postgres -c 'pgbench -i -s 30 benchdb'""" % container['id']
            TaktukRemote(cmd, host).run()
            sleep(15)

    def stress_containers_pgbench(self, co_containers, host):
        logger.info('Stress pgbench containers on %s' % co_containers)
        pgbench_cmd = """su - postgres -c 'pgbench benchdb -c 90 -j 2 -T 3600'"""
        cmd = '''truncate -s 0 stress.sh && ''' + \
            '''echo "%s" > stress.sh && nohup bash stress.sh &>/dev/null &''' % \
            ';'.join(['''(lxc-attach -n %s -- %s &)''' % (container['id'], pgbench_cmd)
                      for container in co_containers])
        logger.info('Run command:\n%s' % cmd)
        p = TaktukRemote(cmd, host).start()
        logger.info('Sleep 35 seconds to wait for stress to reach its maximum')
        sleep(35)

    def stress_containers_cpu(self, co_containers, host):
        """Prepare a benchmark command with stress tool"""
        logger.info('Stress CPU containers on %s' % co_containers)
        kflops_cmd = '''/root/kflops/kflops'''
        cmd = '''truncate -s 0 stress.sh && ''' + \
            '''echo '%s' > stress.sh && nohup bash stress.sh &>/dev/null &''' % \
            ';'.join(['''(lxc-attach -n %s -- %s &)''' % (container['id'], kflops_cmd)
                      for container in co_containers])
        logger.info('Run command:\n%s' % cmd)
        p = TaktukRemote(cmd, host).start()
        logger.info('Sleep 15 seconds to wait for stress to reach its maximum')
        sleep(15)

    def stress_containers_io(self, co_containers, host):
        """Prepare a benchmark command with stress tool"""
        logger.info('Stress IO containers on %s' % co_containers)
        cmd = '''truncate -s 0 stress.sh && ''' + \
            '''echo '%s' > stress.sh && nohup bash stress.sh &>/dev/null &''' % ';'.join(
                ['''(lxc-attach -n %s -- stress --hdd 6 &)''' % container['id'] for container in co_containers])
        logger.info('Run command:\n%s' % cmd)
        p = TaktukRemote(cmd, host).start()
        logger.info('Sleep 20 seconds to wait for stress to reach its maximum')
        sleep(20)

    def stress_containers_mem(self, co_containers, host):
        """Prepare a benchmark command with stress tool"""
        logger.info('Stress mem containers on %s' % co_containers)
        cachebench_cmd = '''/root/llcbench/cachebench/cachebench -m 1024 -e 10 -x 20 -d 600 -b'''
        cmd = '''truncate -s 0 stress.sh && ''' + \
            '''echo '%s' > stress.sh && nohup bash stress.sh &>/dev/null &''' % \
            ';'.join(['''(lxc-attach -n %s -- %s &)''' % (container['id'], cachebench_cmd)
                      for container in co_containers])

        logger.info('Run command:\n%s' % cmd)
        p = TaktukRemote(cmd, host).start()
        logger.info('Sleep 15 seconds to wait for stress to reach its maximum')
        sleep(15)

    def stress_containers_network(self, co_containers, host):
        """Prepare a benchmark command with stress tool"""
        self.network_ports = ['%s' % i for i in range(5000, 5000 + len(co_containers))]
        # for c in co_containers:
        #     logger.info('Start container: %s' % c)
        #     p = TaktukRemote('''lxc-start -n %s''' % c['id'], host).run()
        #     sleep(2)
        logger.info('Stress Network containers on %s' % co_containers)
        attach_cmd = ';'.join(['''(lxc-attach -n %s -- iperf3 -c %s -t 3600 -P 1 -b 1G -R -p %s &)''' %
                               (container['id'], self.options.ip, self.network_ports[index])
                               for index, container in enumerate(co_containers)])
        cmd = '''truncate -s 0 stress.sh && ''' + \
            '''echo '%s' > stress.sh && nohup bash stress.sh &>/dev/null &''' % attach_cmd

        logger.info('Run command:\n%s' % cmd)
        p = TaktukRemote(cmd, host).start()
        logger.info('Sleep 15 seconds to wait for stress to reach its maximum')
        sleep(15)

    def destroy_containers(self, host):
        """Prepare a benchmark command with stress tool"""
        logger.info('kill stress')
        TaktukRemote('''pkill -9 -f stress''', host).run()
        logger.info('--> kill stress done')
        logger.info('kill cachebench')
        TaktukRemote('''pkill -9 -f cachebench''', host).run()
        logger.info('--> kill cachebench done')
        sleep(2)
        logger.info('Stop containers')
        p = TaktukRemote('''for c in $(lxc-ls); do lxc-stop -k -n $c; done;''', host).run()
        logger.info('Stop containers --> done')

        logger.info('Destroy containers')
        p = TaktukRemote('''for c in $(lxc-ls); do lxc-destroy -n $c; done;''', host).run()
        logger.info('Destroy containers --> done')

        logger.info('Remove folders')
        if self.options.disk == 'ceph':
            p = TaktukRemote(';'.join(['rm -rf /tmp/data_%s/*' % _ for _ in range(0, 18)]), host).run()
            p = TaktukRemote('rm -rf /var/lib/lxc/*', host).run()
        elif self.options.disk == 'sdd':
            p = TaktukRemote('rm -rf /tmp/*', host).run()
        else:
            p = TaktukRemote('rm -rf /var/lib/lxc/*', host).run()
        logger.info('Remove folders --> done')

    def _bootlog2boottime(self, bootlog):
        for line in bootlog.split('\n'):
            if 'lxc_start_ui' in line:
                begin = datetime.strptime(line.split()[1], '%Y%m%d%H%M%S.%f')
            if 'RUNNING' in line:
                end = datetime.strptime(line.split()[1], '%Y%m%d%H%M%S.%f')
                break
        return (end - begin).total_seconds()

    def _wait_for_boot(self, container, host):
        count = 6
        while count > 0:
            logger.info('Check container %s is started' % container)
            cmd = 'lxc-ls --fancy'
            run = TaktukRemote(cmd, host).run()
            for p in run.processes:
                for line in p.stdout.strip().split('\n'):
                    if container in line and 'RUNNING' in line:
                        return True
            count -= 1
            sleep(5)
        return False

    def get_container_boottime(self, e_containers, host):
        """ Get container boot time """
        boot_duration = {container['id']: {'link_up': '', 'ssh': ''} for container in e_containers}
        for _id, val in boot_duration.iteritems():
            logger.info('Get container %s boottime' % _id)
            logger.info('Remove boot.log of container %s' % _id)
            cmd = 'rm -rf /root/boot.log'
            run = TaktukRemote(cmd, host).run()
            logger.info('Start container %s to get boottime' % _id)
            cmd = 'lxc-start -n %s -l TRACE -o /root/boot.log' % _id
            run = TaktukRemote(cmd, host).run()
            self._wait_for_boot(_id, host)
            cmd = 'cat /root/boot.log'
            run = TaktukRemote(cmd, host).run()

            for p in run.processes:
                boottime = self._bootlog2boottime(p.stdout.strip())
            val['link_up'] = boottime
        logger.info('Boot result:\n%s' % boot_duration)
        return boot_duration

    def get_multiple_container_boottime(self, e_containers, host):
        """ Get container boot time of simultaneously boot """
        cmd = '''free && sync && echo 3 > /proc/sys/vm/drop_caches && free'''
        logger.info('clear cache of memory')
        run = TaktukRemote(cmd, host).start()
        sleep(2)
        boot_duration = {container['id']: {'link_up': '', 'ssh': ''} for container in e_containers}

        if self.options.disk == 'ssd':
            rootfs_path = '/tmp/'
        else:
            rootfs_path = '/var/lib/lxc/'

        for index, c in enumerate(e_containers):
            logger.info('Remove boot.log of container %s' % c['id'])
            cmd = 'rm -rf %s%s/%s.log' % (rootfs_path, c['id'], c['id'])
            run = TaktukRemote(cmd, host).run()

        cmd = 'parallel -k \'lxc-start -n {} -l TRACE\' ::: %s' % \
              ' '.join([c['id'] for c in e_containers])
        logger.info('run parallel: %s' % cmd)
        run = TaktukRemote(cmd, host).run()

        for index, c in enumerate(e_containers):
            self._wait_for_boot(c['id'], host)
            cmd = 'cat %s%s/%s.log' % (rootfs_path, c['id'], c['id'])
            run = TaktukRemote(cmd, host).run()

            for p in run.processes:
                boottime = self._bootlog2boottime(p.stdout.strip())
            boot_duration[c['id']]['link_up'] = boottime

        logger.info('Boot result:\n%s' % boot_duration)
        return boot_duration

    def get_cpu_utilization(self, host):
        results = []
        for index in range(5):
            logger.info('Run CPU utilization test #%s' % index)
            # retry Taktuk for k-times
            retries = self.options.number_of_retries
            while retries > 0:
                is_error = False
                cpu_process = SshProcess("python -c 'import psutil;print psutil.cpu_percent(percpu=True);'",
                                         host, shell=True).run()
                try:
                    cpu_utils = eval(cpu_process.stdout.strip())
                    if not isinstance(cpu_utils, list):
                        is_error = True
                    logger.info('Result CPU utilization test #%s: %s' % (index, cpu_utils))
                    results.append(cpu_utils)
                except:
                    logger.info('Error, with stdout: %s' % p.stdout)
                    is_error = True
                if is_error:
                    logger.info('Retrying [#%s] because there is error in getting CPU utilization\n' %
                                (self.options.number_of_retries - retries + 1))
                    retries -= 1
                    # sleep 5 second then retry
                    sleep(5)
                else:
                    break
        return results

    def get_hosts_disk_speed(self, hosts):
        sleep(30)
        result = {}
        logger.info('Measuring disk speed of hosts: %s' % pformat(hosts))
        if self.options.disk == 'ceph':
            out_file_path = '/tmp/data/'
        else:
            out_file_path = '/tmp/'
        hosts_speeds = self.get_disk_speed([{'ip': host} for host in hosts], out_file_path=out_file_path)
        if not hosts_speeds:
            return {}
        for host, host_disk_speeds in hosts_speeds.iteritems():
            # host_disk_speeds = self.get_disk_speed([{'ip': host}]).get(host, list())
            host_speed = None
            host_unit = None
            host_speeds = []
            host_units = []
            for index, each in enumerate(host_disk_speeds):
                logger.info('[%s] Speed #%s: %s (%s)' % (host, index, each['speed'], each['unit']))
                host_speeds.append(float(each['speed']))
                host_units.append(each['unit'].strip().lower())
            if len(host_speeds) > 0:
                host_speed = max(host_speeds)
                host_unit = set(host_units).pop()
                logger.info('[%s] Average speed: %s (%s); max-min speed: %s -> %s (%s)' %
                            (host, sum(host_speeds) * 1.0 / len(host_speeds), host_unit,
                                max(host_speeds), min(host_speeds), host_unit))
                result[host] = host_speed
            # sleep(15)
        self.hosts_speed = result
        print self.hosts_speed
        return result

    def get_disk_speed(self, vms, out_file_path='/tmp/data/'):
        """Boot duration is defined as the time f """
        disk_speeds = {vm['ip']: [] for vm in vms}
        for index in range(5):
            logger.info('Run disk speed test #%s' % index)
            #cmd = 'dd if=/dev/urandom of=%smyfile bs=1M count=2048' % out_file_path
            cmd = 'dd if=/dev/zero of=%szero conv=fdatasync bs=120k count=30k' % out_file_path
            # retry Taktuk for k-times
            retries = self.options.number_of_retries
            while retries > 0:
                is_error = False
                get_boottime = TaktukRemote(cmd, [vm['ip'] for vm in vms]).run()
                for p in get_boottime.processes:
                    if p.error or 'copied' not in p.stderr:
                        is_error = True
                        continue
                    speed, unit = p.stderr.strip().split(' s, ')[1].split(' ')
                    unit = unit.replace('\n', '')
                    if speed:
                        disk_speeds[p.host.address].append({'speed': speed, 'unit': unit})
                if is_error:
                    logger.info('Retrying [#%s] because there is error in Taktuk on disk speed test\n' %
                                (self.options.number_of_retries - retries + 1))
                    retries -= 1
                    # sleep 10 seconds then retry
                    sleep(10)
                else:
                    break
        return disk_speeds

    def save_results(self, host, comb, vms, xpvms_index, boot_duration, dd=None, host_speed=None):
        # Gathering results
        comb_dir = self.result_dir + '/' + slugify(comb) + '/'
        if not os.path.exists(comb_dir):
            os.mkdir(comb_dir)
        else:
            logger.warning('%s already exists, removing '
                           'existing files', comb_dir)
            for f in listdir(comb_dir):
                try:
                    remove(comb_dir + f)
                except Exception as e:
                    logger.info(e)
                    continue

        logger.info('Writing boot time in result files')
        logger.info('Boot duration:\n%s' % boot_duration)
        text_file = open(comb_dir + "boot_time.txt", "w")
        for vm in vms:
            text_file.write('%s,%s,%s,%s\n' % (xpvms_index[vm['id']],
                                               boot_duration[vm['id']]['link_up'],
                                               boot_duration[vm['id']]['ssh'],
                                               '0'))
        text_file.close()

        if self.options.dd:
            logger.info('Writing host disk speed in result files')
            with open(comb_dir + "host_disk_speed.txt", "w") as text_file:
                text_file.write('%s' % host_speed)
        return True

    def umount_mount_tmp(self, host, retry=10):
        if self.options.disk == 'ssd':
            umount = SshProcess('sync; echo 3 > /proc/sys/vm/drop_caches; '
                                'umount /tmp; sleep 5; mount -t ext4 /dev/sdf1 /tmp',
                                host, shell=True).run()
        elif self.options.disk == 'ceph':
            umount = SshProcess('sync; echo 3 > /proc/sys/vm/drop_caches; '
                                'umount /tmp/backing_file; sleep 5; mount -t ext4 /dev/rbd0 /tmp/backing_file',
                                host, shell=True).run()
        else:
            umount = SshProcess('sync; echo 3 > /proc/sys/vm/drop_caches; '
                                'umount /tmp; sleep 5; mount /tmp',
                                host, shell=True).run()

        while (not umount.finished_ok) and (retry > 0):
            lsof = SshProcess('lsof /tmp; virsh list', host,
                              ignore_exit_code=True,
                              nolog_exit_code=True).run()
            logger.info(host + ' : lsof /tmp : %s', lsof.stdout.strip())
            sleep(5)
            umount.reset()
            umount.run()
            retry -= 1

        if not umount.finished_ok:
            logger.error('Failed to unmount /tmp for %s', host)
            logger.error('mount/umount error : %s %s',
                         umount.stdout.strip(),
                         umount.stderr.strip())
            exit()

    def _setup_iperf3_host(self, setup):
        logger.info('Setup iperf3 for the HOST')
        cmd = '''echo "deb http://ftp.debian.org/debian sid main" >> /etc/apt/sources.list && ''' + \
            '''apt-get update -y --force-yes && apt-get install -y --force-yes -t sid libc6 libc6-dev libc6-dbg && ''' + \
            '''apt-get install -y --force-yes iperf3 && ''' + \
            '''sed -i "s/deb http:\/\/ftp.debian.org\/debian sid main/#deb http:\/\/ftp.debian.org\/debian sid main/g" /etc/apt/sources.list'''
        logger.info('Installing iperf3')
        temp = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(temp)

    def _setup_ssd(self, setup):
        """Installation of packages"""
        logger.info('Setup SSD disk for the cluster')

        cmd = 'umount /tmp; mount -t ext4 /dev/sdf1 /tmp;'
        mount = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(mount)

        logger.info('Setup SSD path for LXC')
        cmd = "echo 'lxc.lxcpath = /tmp' > /etc/lxc/lxc.conf"
        mount = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(mount)

    def _setup_ceph(self, setup):
        """Installation of packages"""
        # logger.info('Wait 2 minute for setting from CEPH master')
        # sleep(120)

        logger.info('Setup ceph')

        cmd = 'modprobe rbd'
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        for index in range(0, 18):
            logger.info('Mounting data %s volume' % index)
            cmd = 'rbd --pool data map data_%s' % index
            p = setup.fact.get_remote(cmd, setup.hosts).run()
            setup._actions_hosts(p)

            cmd = 'mkfs.ext4 -F /dev/rbd%s' % index
            p = setup.fact.get_remote(cmd, setup.hosts).run()
            setup._actions_hosts(p)

            cmd = 'mkdir /tmp/data_%s' % index
            p = setup.fact.get_remote(cmd, setup.hosts).run()
            setup._actions_hosts(p)

            cmd = 'mount /dev/rbd%s /tmp/data_%s' % (index, index)
            p = setup.fact.get_remote(cmd, setup.hosts).run()
            setup._actions_hosts(p)

    def _setup_iperf3(self, vms):
        logger.info('Setting up iperf3 for the VMs')
        vms_ip = [vm['ip'] for vm in vms]

        cmd = '''echo "deb http://ftp.debian.org/debian sid main" >> /etc/apt/sources.list && ''' + \
            '''apt-get update -y --force-yes && apt-get install -y --force-yes -t sid libc6 libc6-dev libc6-dbg && ''' + \
            '''apt-get install -y --force-yes iperf3 && ''' + \
            '''sed -i "s/deb http:\/\/ftp.debian.org\/debian sid main/#deb http:\/\/ftp.debian.org\/debian sid main/g" /etc/apt/sources.list'''

        temp = TaktukRemote(cmd, vms_ip).run()

    def _setup_lxc(self, setup):
        """Installation of packages"""
        logger.info('Install LXC on host')

        cmd = '''export DEBIAN_FRONTEND=noninteractive && ''' + \
              '''apt-get install -y --force-yes -t jessie-backports  lxc libvirt0 linux-image-amd64 && ''' + \
              '''apt-get install -y --force-yes libpam-cgroup libpam-cgfs bridge-utils && ''' + \
              '''sh -c 'echo "kernel.unprivileged_userns_clone=1" > /etc/sysctl.d/80-lxc-userns.conf' && ''' + \
              '''sysctl --system && ''' + \
              '''usermod --add-subuids 1258512-1324048 $USER && ''' + \
              '''usermod --add-subgids 1258512-1324048 $USER && ''' + \
              '''mkdir -p .config/lxc && ''' + \
              '''(echo "lxc.include = /etc/lxc/default.conf")  >> .config/lxc/default.conf && ''' + \
              '''(echo "lxc.id_map = u 0 1258512 65537")  >> .config/lxc/default.conf && ''' + \
              '''(echo "lxc.id_map = g 0 1258512 65537")  >> .config/lxc/default.conf && ''' + \
              '''(echo "lxc.mount.auto = proc:mixed sys:ro cgroup:mixed")  >> .config/lxc/default.conf && ''' + \
              '''(echo "lxc.network.link = lxcbr0")  >> .config/lxc/default.conf && ''' + \
              '''(echo "lxc.network.flags = up")  >> .config/lxc/default.conf && ''' + \
              '''(echo "lxc.network.hwaddr = 00:FF:xx:xx:xx:xx") >> .config/lxc/default.conf'''

        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        cmd = """echo 'USE_LXC_BRIDGE="true"' >> /etc/default/lxc-net"""
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        logger.info('Config network of default container')
        cmd = """(echo 'lxc.network.type = veth' > /etc/lxc/default.conf) && """
        cmd += """(echo 'lxc.network.flags = up' >> /etc/lxc/default.conf) && """
        cmd += """(echo 'lxc.network.link = lxcbr0' >> /etc/lxc/default.conf) && """
        cmd += """(echo 'lxc.network.hwaddr = 00:16:3e:xx:xx:xx' >> /etc/lxc/default.conf) && """
        logger.info('Config memory')
        cmd += """(echo 'lxc.cgroup.memory.limit_in_bytes = 1024M' >> /etc/lxc/default.conf)"""
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        cmd = """service lxc-net restart"""
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        logger.info('Download and un-tar LLCBench (with CacheBench)')
        cmd = '''wget http://icl.cs.utk.edu/llcbench/llcbench.tar.gz && tar -xzvf llcbench.tar.gz llcbench'''
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        logger.info('Make and install LLCBench (with CacheBench)')
        cmd = '''make -C llcbench/ linux-lam && make -C llcbench/ cache-bench'''
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        logger.info('Upload kflops in LINPACK from host')
        TaktukPut(setup.hosts, ['/home/lnguyen/kflops'], remote_location='/root/').run()

    def _remove_sshkey(self, setup):
        cmd = '''ssh-keygen -f "/home/lnguyen/.ssh/known_hosts" -R %s''' % setup.hosts[0]
        logger.info('Remove keygen test: %s' % cmd)
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

    def setup_hosts(self):
        """ """
        disks = ['/home/lnguyen/image/benchs_vms.qcow2']
        logger.info('Initialize vm5k_deployment')

        setup = vm5k_deployment(vms=[], resources=self.resources,
                                env_name=self.options.env_name,
                                env_file=self.options.env_file)
        setup.fact = ActionFactory(remote_tool=TAKTUK,
                                   fileput_tool=CHAINPUT,
                                   fileget_tool=SCP)
        logger.info('Deploy hosts')
        setup.hosts_deployment()

        logger.info('Change GRUB to add memory cgroups')
        cmd = """sed -i '/GRUB_CMDLINE_LINUX=/c\GRUB_CMDLINE_LINUX="debian-installer=en_US cgroup_enable=memory swapaccount=1"' /etc/default/grub"""
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        cmd = """update-grub2"""
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        logger.info('Rebooting hosts')
        reboot_hosts(self.hosts)
        logger.info('Rebooting hosts ... done')

        setup.hosts_deployment()

        setup.packages_management(
            other_packages='iftop sysstat time build-essential parallel gawk nload git', launch_disk_copy=False)

        self._setup_lxc(setup)

        logger.info('Install ceph + packages in a different way')
        cmd = '''export DEBIAN_FRONTEND=noninteractive && ''' + \
              '''apt-get update -y --force-yes && ''' + \
              '''apt-get install -y --force-yes ceph'''
        p = setup.fact.get_remote(cmd, self.hosts).run()
        setup._actions_hosts(p)
        logger.info('Install ceph + packages in a different way ... DONE')

        if self.options.disk == 'ceph':
            logger.info('Get setting from CEPH master, sleep 60s')
            # sleep(60)
            host = self.hosts[0]
            cmd = '''ceph-deploy install %s''' % (host)
            p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()
            print p
            cmd = '''ceph-deploy admin %s''' % (host)
            p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()
            print p
            logger.info('Get setting from CEPH master ... DONE')

            for index in range(0, 18):
                logger.info('Create volume for data %s in CEPH master' % index)
                cmd = 'rbd create data_%s --size 30720 --pool data' % index
                p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()

            logger.info('Create volume in CEPH master ... DONE')

            self._setup_ceph(setup)
            backing_file_dir = '/tmp/backing_file'
            data_file_dir = '/tmp/data'
            # backing_file_dir = '/tmp'
        else:
            backing_file_dir = '/tmp'

        if self.options.disk == 'ssd':
            self._setup_ssd(setup)

        if self.options.bandwidth:
            self._setup_iperf3_host(setup)

    def run(self):
        """The main experimental workflow, as described in
        ``Using the Execo toolkit to perform ...``
        """
        p = Process('grep -m 1 cache_mode /home/lnguyen/vm5k/vm5k/src/vm5k/actions.py').run()
        cache_mode = p.stdout.split('cache_mode=')[-1].split('\'')[1]
        logger.info('Cache mode: %s' % cache_mode)

        self.force_options()
        if 'network' in self.options.load_injector and not self.options.ip:
            logger.info('Network injector needs server IP')
            var = raw_input("Please enter IP address of the server: ")
            logger.info("You have entered IP: %s" % var)
            self.options.ip = var.strip()

        # The argument is a cluster
        self.cluster = self.args[0]
        self.frontend = get_cluster_site(self.cluster)

        if self.options.disk == 'ceph' and self.cluster not in ['econome', 'paravance', 'parasilo']:
            logger.info('Ceph only runs on Nantes and Rennes clusters')
            exit()

        if self.cluster == 'econome':
            self.ceph_key = 'AQA56Y1XxwqHAhAAPksOQTL7o5h920oqclqiDw=='
        else:
            self.ceph_key = 'AQA/LJdXaL8WJhAAswINQl7lVQ2NH7XwXV44sQ=='

        # Analyzing options
        if self.options.oar_job_id:
            self.oar_job_id = self.options.oar_job_id
        else:
            self.oar_job_id = None

        try:
            # Creation of the main iterator which is used for the first control loop.
            # You need have a method called define_parameters, that returns a list of parameter dicts
            self.create_paramsweeper()

            job_is_dead = False
            # While they are combinations to treat
            while len(self.sweeper.get_remaining()) > 0:
                # If no job, we make a reservation and prepare the hosts for the experiments
                if self.oar_job_id is None:
                    self.make_reservation()
                # Retrieving the hosts and subnets parameters
                self.get_resources()
                # Hosts deployment and configuration
                if not self.options.no_hosts_setup:
                    self.setup_hosts()
                if len(self.hosts) == 0:
                    break
                print "Test dd", self.options.dd
                if self.options.dd:
                    self.get_hosts_disk_speed(self.hosts)
                else:
                    self.hosts_speed = {host: 0.0 for host in self.hosts}

                # Initializing the resources and threads
                available_hosts = list(self.hosts)
                available_ip_mac = list(self.ip_mac)
                threads = {}

                # Checking that the job is running and not in Error
                while self.is_job_alive()['state'] != 'Error' \
                        or len(threads.keys()) > 0:
                    job_is_dead = False
                    while self.options.n_nodes > len(available_hosts):
                        tmp_threads = dict(threads)
                        for t in tmp_threads:
                            if not t.is_alive():
                                available_hosts.extend(tmp_threads[t]['hosts'])
                                available_ip_mac.extend(tmp_threads[t]['ip_mac'])
                                del threads[t]
                        sleep(5)
                        if self.is_job_alive()['state'] == 'Error':
                            job_is_dead = True
                            break
                    if job_is_dead:
                        break

                    # Getting the next combination
                    comb = self.sweeper.get_next()
                    if not comb:
                        while len(threads.keys()) > 0:
                            tmp_threads = dict(threads)
                            for t in tmp_threads:
                                if not t.is_alive():
                                    del threads[t]
                            logger.info('Waiting for threads to complete')
                            sleep(20)
                        break

                    used_hosts = available_hosts[0:self.options.n_nodes]
                    available_hosts = available_hosts[self.options.n_nodes:]

                    n_vm = self.comb_nvm(comb)
                    used_ip_mac = available_ip_mac[0:n_vm]
                    available_ip_mac = available_ip_mac[n_vm:]

                    t = Thread(target=self.workflow,
                               args=(comb, used_hosts, used_ip_mac))
                    threads[t] = {'hosts': used_hosts, 'ip_mac': used_ip_mac}
                    logger.debug('Threads: %s', len(threads))
                    t.daemon = True
                    t.start()

                if self.is_job_alive()['state'] == 'Error':
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

    def is_job_alive(self):
        rez = get_oar_job_info(self.oar_job_id, self.frontend)
        while 'state' not in rez:
            rez = get_oar_job_info(self.oar_job_id, self.frontend)
        return rez


if __name__ == "__main__":
    engine = ContainerBootMeasurement()
    engine.start()
