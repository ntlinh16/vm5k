#!/usr/bin/env python
from vm5k.engine import *
import string
import random
import os
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
        setattr(parser.values, option.dest, ['mem'])
    print parser.values.load_injector
    if not set(parser.values.load_injector).issubset(set(['io', 'cpu', 'mem', 'pgbench', 'network', 'no_stress'])):
        parser.error('Injectors have to be the following: io, cpu, mem, pgbench, network or no_stress')


class VMBootMeasurement(vm5k_engine_para):
    def __init__(self):
        super(VMBootMeasurement, self).__init__()
        # self.n_nodes = 1
        self.options_parser.add_option("--vm", dest="n_vm",
                                       type="int", default=1,
                                       help="maximum number of VMs")
        self.options_parser.add_option("--covm", dest="n_co_vms",
                                       type="int", default=1,
                                       help="maximum number of co_VMs")
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
                                       default=['no_stress'],
                                       type="string",
                                       action="callback",
                                       callback=parse_injectors,
                                       help="""type of load stress methods to use:
                                            io, cpu, mem, pgbench, network or no_stress\ndefault to no_stress""")
        self.options_parser.add_option("--io_scheduler",
                                       dest="io_scheduler",
                                       default='cfq',
                                       choices=['noop', 'deadline', 'cfq'],
                                       help="type of IO schedulers to use: noop, deadline, or cfq\ndefault to cfq")
        self.options_parser.add_option("--retry",
                                       dest="number_of_retries",
                                       default=15,
                                       help="set the number of retries for getting boot_time")
        self.options_parser.add_option("--renice",
                                       dest="renice",
                                       default=None,
                                       help="set the nice of coVMs")
        self.options_parser.add_option("--diskspeed",
                                       dest="dd",
                                       action="store_true",
                                       help="run dd disk speed check on eVMs")
        self.options_parser.add_option("--image",
                                       dest="image",
                                       type="string",
                                       default='/home/lnguyen/image/benchs_vms.qcow2',
                                       help="the path to the image to deploy VMs")
        self.options_parser.add_option("--disk",
                                       dest="disk",
                                       default='hdd',
                                       choices=['hdd', 'ssd', 'ceph', 'opendedup'],
                                       help="""type of disks to use: opendedup, hdd, ssd, or ceph\n
                                            default to hdd""")
        self.options_parser.add_option("--monitor",
                                       dest="monitor",
                                       default=None,
                                       help="run resource monitor with vmstat and iostat")
        self.options_parser.add_option("--ceph_backing_local",
                                       dest="ceph_backing_local",
                                       action="store_true",
                                       help="run CEPH with backing file store in local disk")
        self.options_parser.add_option("--ksm",
                                       dest="ksm",
                                       action="store_true",
                                       help="enable KSM in host kernel")
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
        self.options_parser.add_option("--limit",
                                       dest="limit",
                                       type="int",
                                       default=None,
                                       help="the maximum limit on the host, the unit is in Mbit")
        self.options_parser.add_option("--latency",
                                       dest="latency",
                                       type="int",
                                       default=None,
                                       help="the maximum latency added on the host, the unit is in ms")
        self.options_parser.add_option("--cephmaster",
                                       dest="ceph_master",
                                       type="string",
                                       default=None,
                                       help="give the ceph master node hostname")
        self.options_parser.add_option("--cache_mode",
                                       dest="cache_mode",
                                       type="string",
                                       default=None,
                                       help="the cache mode to configure for VMs")

        self.vms = []

    def define_parameters(self):
        """Define the parameters you want to explore"""
        parameters = {
            'n_vm': range(1, self.options.n_vm + 1),
            'n_co_vms': range(0, self.options.n_co_vms + 1),
            'n_mem': range(1, self.options.n_mem + 1),
            'n_cpu': range(1, self.options.n_cpu + 1),
            'cpu_policy': ['one_by_core'],
            #'image_policy': ['one', 'one_per_vm'],
            'image_policy': ['one'],
            # 'boot_policy': ['all_at_once', 'one_by_one', 'one_then_others', 'cache_all_at_once','boot_and_cache','boot_one_cache_then_all'],
            'boot_policy': ['all_at_once'],
            'load_injector': self.options.load_injector,
            'iteration': range(1, 11)}
        if self.options.bandwidth:
            logger.info('Run network stress on HOST up to %sG bandwidths' % self.options.bandwidth)
            parameters['bandwidths'] = range(1, self.options.bandwidth + 1, 5)
        if self.options.limit:
            logger.info('Limit network bandwidth on HOST up to %sMbps' % self.options.limit)
            print self.options.limit
            parameters['limits'] = range(1, self.options.limit + 1, 1)
            logger.info('Limit network bandwidth on HOST with the following bins: %s' % parameters['limits'])
        if self.options.latency:
            logger.info('Add latency to network on HOST')
            print self.options.latency
            parameters['latency'] = range(0, self.options.latency + 1, 20)
            logger.info('Add latency to network on HOST with the following bins: %s' % parameters['latency'])
        if self.options.cache_mode:
            logger.info('Cache mode to use: %s' % self.options.cache_mode)
            parameters['cache_mode'] = [self.options.cache_mode]

        self.disks = [self.options.image]

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

    def workflow(self, comb, hosts, ip_mac):
        """Perform a boot measurements on the VM """
        host = hosts[0]
        #logger.info('hosts %s', host)
        #logger.info('ip_mac %s', ip_mac)
        host_name = host.split('.')[0]
        thread_name = style.Thread(host.split('.')[0]) + ': '

        n_cores_host = 16
        try:
            n_cores_host = get_host_attributes(host_name)['architecture']['smt_size']
            logger.info('Number of cores on [%s] = %s' % (thread_name, n_cores_host))
        except:
            logger.error('Cannot get number of cores from [%s]' % thread_name)
        
    
        logger.info(thread_name + ' Number of VM, coVM, ipMAC are %s %s %s',
                    str(comb['n_vm']), str(comb['n_co_vms']), str(len(ip_mac)))

        comb_ok = False
        too_many_VMS = False
        #Checking if there is enough core to asign to VMs or not
        if comb['cpu_policy'] == 'one_by_core':
            if comb['n_vm'] + comb['n_co_vms'] * comb['n_vm'] > n_cores_host:
              too_many_VMS = True

        try:
            if not too_many_VMS:
                logger.info(style.step(' Performing combination:') + '\n' + slugify(comb))

                if comb.get('limits', -1) >= 0:
                    logger.info('[%s] Remove limit network on HOST' % slugify(comb))
                    self.network_limit_restart(host)

                if comb.get('latency', -1) >= 0:
                    logger.info('[%s] Remove latency on network on HOST' % slugify(comb))
                    self.network_latency_limit_restart(host)

                logger.info(thread_name + 'Destroying all vms on hosts')
                destroy_vms(hosts, undefine=True)

                if self.options.disk != 'ceph':
                    self.umount_mount_tmp(host)

                if self.options.disk == 'ceph':
                    self._clean_data_disks(host)
                    
                logger.info('Create all vms on ' + thread_name)
                vms = self.create_vms(comb, host, ip_mac)
                logger.info('Create all vms on ' + thread_name + ': DONE')

                xpvms = filter(lambda v: 'covm' not in v['id'], vms)
                # get vm index for outputing ordered result
                xpvms_index = {vm['id']: str(index) for index, vm in enumerate(xpvms)}
                covms = filter(lambda v: 'covm' in v['id'], vms)
                logger.info(thread_name + ' # xpvms = %s| # covms = %s' % (len(xpvms), len(covms)))

                injector = None
                # Starting co_VMs and running workloads
                if len(covms) > 0:
                    logger.info('Starting covms \n%s',
                                "\n".join([vm['id'] + ': ' + vm['ip']
                                           for vm in covms]))
                    start_vms(covms).run()
                    booted = wait_vms_have_started(covms)
                    if not booted:
                        logger.error('Unable to boot all the coVMS for %s', slugify(comb))
                        exit()

                    logger.info('All coVMs are booted !!!')
                    sleep(5)
                    if self.options.renice:
                        renice = self.renice_covms(host)
                        logger.info('Sleep 10 seconds to wait for renice-ing')
                        sleep(5)

                    if 'network' in self.options.load_injector:
                        self.network_ports = ['%s' % i for i in range(5000, 5000 + len(covms))]

                    # Inject load in covms
                    if comb['load_injector'] == 'cpu':
                        logger.info('[%s] Trigger cpu injector' % slugify(comb))
                        injector = self.kflops(covms).start()
                    elif comb['load_injector'] == 'mem':
                        logger.info('[%s] Trigger mem injector' % slugify(comb))
                        injector = self.cache_bench(covms).start()
                    elif comb['load_injector'] == 'io':
                        logger.info('[%s] Trigger io injector' % slugify(comb))
                        injector = self.io_stress(covms).start()
                    elif comb['load_injector'] == 'pgbench':
                        logger.info('[%s] Trigger pgbench injector' % slugify(comb))
                        injector = self.pgbench(covms).start()
                    elif comb['load_injector'] == 'network':
                        logger.info('[%s] Trigger network injector' % slugify(comb))
                        injector = self.network_stress(covms).start()
                    elif comb['load_injector'] == 'no_stress':
                        logger.info('[%s] Trigger NO STRESS injector' % slugify(comb))
                    else:
                        mem = []
                        cpu = []
                        io = []
                        for v in covms:
                            random.choice((mem, cpu, io)).append(v)
                        injector = []
                        if len(cpu):
                            logger.info('[%s] Mixed -> cpu injector [%s]' % (slugify(comb), cpu))
                            injector.append(self.kflops(cpu).start())
                        if len(mem):
                            logger.info('[%s] Mixed -> mem injector [%s]' % (slugify(comb), mem))
                            injector.append(self.cache_bench(mem).start())
                        if len(io):
                            logger.info('[%s] Mixed -> io injector [%s]' % (slugify(comb), io))
                            injector.append(self.io_stress(io).start())

                    logger.info('Sleep 30 seconds to wait for stress to reach its maximum')
                    sleep(30)
                    logger.info('covms are up and run stress %s',
                                comb['load_injector'])
                # logger.info('Get CPU utilization of all coVMs on this host')
                # cpu_utils = self.get_cpu_utilization(host)


                #setup limit network on HOST
                stress = None
                if comb.get('bandwidths', None):
                    logger.info('[%s] Trigger network injector on HOST with bandwidth %s' %
                                (slugify(comb), comb['bandwidths']))
                    stress = self.network_stress_host(host, comb['bandwidths'])
                    stress.start()
                    logger.info('Sleep 30 seconds to wait for stress to reach its maximum')
                    sleep(30)

                if comb.get('limits', 0) > 0:
                    logger.info('[%s] Add network limitation %s on HOST' % (slugify(comb), comb['limits']))
                    self.network_limit(host, comb['limits'])
                    sleep(2)
                if comb.get('latency', 0) > 0:
                    logger.info('[%s] Add latency %s to network on HOST' % (slugify(comb), comb['latency']))
                    self.network_latency_limit(host, comb['latency'])
                    sleep(2)


                # Start Resource Monitor
                if comb.get('monitor', None):
                    logger.info('Run Resource Monitor')
                    stat_cmd = '''vmstat 1 | perl -e \'$| = 1; while (<>) { print localtime() . ": $_"; }\' > vmstat.txt &'''
                    #logger.info('Run vmstat monitor: %s' % stat_cmd)
                    run = TaktukRemote(stat_cmd, host).start()

                    if self.options.disk == 'hdd':
                        stat_cmd = '''iostat -xdt sda5 1 > iostat.txt &'''
                    elif self.options.disk == 'ssd':
                        stat_cmd = '''iostat -xdt sdf1 1 > iostat.txt &'''
                    else:
                        stat_cmd = '''iostat 1 > iostat.txt &'''
                    #logger.info('Run iostat monitor: %s' % stat_cmd)
                    run = TaktukRemote(stat_cmd, host).start()
                    sleep(1)



                logger.info('Booting eVms %s\n%s', comb['boot_policy'],
                            "\n".join([vm['id'] + ': ' + vm['ip'] for vm in xpvms]))
                # clear cache before boot eVms
                cmd = '''sync && echo 3 > /proc/sys/vm/drop_caches'''
                logger.info('Clear cache before booting eVMs')
                run = TaktukRemote(cmd, host).run()
                sleep(1)                
                # Boot XP vms
                self.boot_vms(host, comb, xpvms)

                # Stop Resource Monitor
                if comb.get('monitor', None):
                    logger.info('Stop resource monitor')
                    stat_cmd = '''pkill -f vmstat'''
                    #logger.info('stop vmstat monitor: %s' % stat_cmd)
                    run = TaktukRemote(stat_cmd, host).start()

                    stat_cmd = '''pkill -f iostat'''
                    #logger.info('stop iostat monitor: %s' % stat_cmd)
                    run = TaktukRemote(stat_cmd, host).start()

                # Retrieves measurements on eVms
                boot_duration = self.get_boot_duration(xpvms)
                speed = None
                if stress:
                    logger.info('Kill the iperf3 process')
                    stress.kill()

                if self.options.dd:
                    logger.info('Run dd to check disk speed')
                    speed = self.get_disk_speed(xpvms, out_file_path='/tmp/')

                if injector:
                    logger.info('Kill the iperf3 process')
                    injector.kill()

                if not self.save_results(host, comb, xpvms, xpvms_index, boot_duration, speed, self.hosts_speed[host]):
                    exit()

                if (comb['boot_policy'] in ['cache_all_at_once', 'boot_and_cache', 'boot_one_cache_then_all']):
                    comb_dir = self.result_dir + '/' + slugify(comb) + '/'
                    logger.info('Copying the cache time file')
                    stress = TaktukGet(host, ['/root/time_cache.txt'], comb_dir).run()

            else:
                logger.info(thread_name + slugify(comb) +
                            '\n The core on the node is not enough to allocate to VMs, comb_ok=%s' % comb_ok)
            comb_ok = True

        finally:
            if comb_ok:
                self.sweeper.done(comb)
                logger.info(thread_name + slugify(comb) + ' has been done')
            else:
                self.sweeper.cancel(comb)
                logger.warning(thread_name + slugify(comb) + ' has been canceled')
            logger.info(style.step('%s Remaining'), len(self.sweeper.get_remaining()))

    def cache_bench(self, vms):
        """Prepare a benchmark command with cachebench"""
        logger.info("Running mem bench with %s" % vms)
        memsize = [str(27 + int(vm['n_cpu'])) for vm in vms]
        vms_ip = [vm['ip'] for vm in vms]
        vms_out = [vm['ip'] + '_' + vm['cpuset'] for vm in vms]
        stress = TaktukRemote('while true ; do ./benchs/llcbench/cachebench/cachebench ' +
                              '-m {{memsize}} -e 1 -x 2 -d 1 -b > /root/cachebench_{{vms_out}}_rmw.out ; done',
                              vms_ip)

        return stress

    def pgbench(self, vms):
        """Prepare a benchmark command with pgbench"""
        logger.info('Sleep 30s for postgres to be ready')
        sleep(30)
        logger.info('Running pgbench with the following vms:\n%s' % vms)
        vms_ip = [vm['ip'] for vm in vms]
        vms_out = [vm['ip'] + '_' + vm['cpuset'] for vm in vms]
        stress = TaktukRemote(
            '''(su - postgres -c 'pgbench benchdb -c 90 -j 2 -T 3600' > /root/pgbench_{{vms_out}}.out)''', vms_ip)
        return stress

    def io_stress(self, vms):
        """Prepare a benchmark command with stress tool"""
        logger.info('Running Stress with the following vms:\n%s' % vms)
        vms_ip = [vm['ip'] for vm in vms]
        # vms_out = [vm['ip'] + '_' + vm['cpuset'] for vm in vms]
        stress = TaktukRemote('''stress --hdd 6''', vms_ip)
        return stress

    def network_stress(self, vms):
        """Prepare a benchmark command with stress tool"""
        logger.info('Running Network Stress with the following vms:\n%s' % vms)
        vms_ip = [vm['ip'] for vm in vms]
        stress = TaktukRemote('''iperf3 -c %s -t 3600 -P 1 -b %sG -R -p {{[port for port in self.network_ports]}}''' %
                              (self.options.ip, 1), vms_ip)
        return stress

    def network_stress_host(self, host, bandwidth):
        """Prepare a benchmark command with stress tool"""
        logger.info('Running Network Stress with bandwidth %s on the following host: %s' % (bandwidth, host))
        stress = TaktukRemote('''iperf3 -c %s -t 3600 -P 1 -b %sG -R''' % (self.options.ip, bandwidth), host)
        return stress

    def network_limit(self, host, limit):
        """Limit the network in host"""
        logger.info('Sleep 20s then limit network to %sMbps on the following host: %s' % (limit, host))
        sleep(20)

        cmd = '''
        tc qdisc add dev ifb0 root handle 1: htb default 30 &&
        tc class add dev ifb0 parent 1: classid 1:1 htb rate "%smbit" &&
        tc class add dev ifb0 parent 1: classid 1:2 htb rate "%smbit" &&
        tc filter add dev ifb0 protocol ip parent 1:0 prio 1 u32 match ip dst $(hostname -i)/32 flowid 1:1 &&
        tc filter add dev ifb0 protocol ip parent 1:0 prio 1 u32 match ip src $(hostname -i)/32 flowid 1:2 &&
        tc qdisc add dev br0 root handle 1: htb default 30 &&
        tc class add dev br0 parent 1: classid 1:1 htb rate "%smbit" &&
        tc class add dev br0 parent 1: classid 1:2 htb rate "%smbit" &&
        tc filter add dev br0 protocol ip parent 1:0 prio 1 u32 match ip dst $(hostname -i)/32 flowid 1:1 &&
        tc filter add dev br0 protocol ip parent 1:0 prio 1 u32 match ip src $(hostname -i)/32 flowid 1:2
        ''' % (limit, limit, limit, limit)
        TaktukRemote(cmd, host).start()

    def network_limit_restart(self, host):
        """Limit the network in host"""
        logger.info('Remove limit network on the following host: %s' % host)
        # TaktukRemote('''pkill -9 -f netimpair''', host).start()
        TaktukRemote('''tc qdisc del dev ifb0 root && tc qdisc del dev br0 root''', host).run()

    def network_latency_limit(self, host, limit):
        """Limit the network in host"""
        logger.info('Sleep 5s then add network latency of %s ms on the following host: %s' % (limit, host))
        sleep(5)
        limit_new = int(limit / 2)
        TaktukRemote('''tc qdisc add dev br0 root netem delay %sms''' % limit_new, host).start()
        TaktukRemote('''tc qdisc add dev ifb0 root netem delay %sms''' % limit_new, host).start()

    def network_latency_limit_restart(self, host):
        """Limit the network in host"""
        logger.info('Remove network latency on the following host: %s' % host)
        TaktukRemote('''tc qdisc del dev br0 root netem''', host).start()
        TaktukRemote('''tc qdisc del dev ifb0 root netem''', host).run()

    def _clean_data_disks(self, host):
        """Clean the content of data directories"""
        for index in range(1, 16):
            #logger.info('Clean data directory of covm-%s on the following host: %s' % (index, host))
            logger.info('Clean data directory')
            TaktukRemote('rm -rf /tmp/data_%s/*' % index, host).run()

    def no_stress(self, vms):
        """Prepare a benchmark command with stress tool"""
        logger.info('Running NO Stress with the following vms:\n%s' % vms)
        return True

    def renice_covms(self, host):
        """Renice covms to set new priority"""
        logger.info('Renice coVMs on the following host: %s' % host)
        all_processes = TaktukRemote("ps -AL | grep qemu | awk \'{print $2}\'", host).run()
        for p in all_processes.processes:
            pids = p.stdout.strip().split()
        logger.info('All pid of processes to be reniced: %s' % set(pids))
        cmds = ['renice -n %s -p %s' % (self.options.renice, pid) for pid in set(pids)]
        TaktukRemote(';'.join(cmds), host).run()

    def get_boot_duration(self, vms):
        """Boot duration is defined as the time f """

        boot_duration = {vm['ip']: {'link_up': '', 'ssh': ''} for vm in vms}
        cmd = 'grep "link up" /var/log/messages |grep eth0| tail -n 1 ' + \
            '| awk \'{print $7}\''
        get_boottime = TaktukRemote(cmd, [vm['ip'] for vm in vms]).run()
        for p in get_boottime.processes:
            boot_duration[p.host.address]['link_up'] = p.stdout.strip()[0:-1]

        cmd = """p=`grep "Server listening on .* port 22" /var/log/auth.log | tail -n 1 | awk '{print $5}'`; pid=${p:5:${#p}-7};clk_tck=$(getconf CLK_TCK);start=`cat /proc/$pid/stat |  awk '{print $22}'`; echo $start $clk_tck | awk '{ print $1/$2 }'"""
        #print 'Boottime Command: %s' % cmd
        # retry Taktuk for k-times
        retries = self.options.number_of_retries
        while retries > 0:
            is_error = False
            get_boottime = TaktukRemote(cmd, [vm['ip'] for vm in vms]).run()
            for p in get_boottime.processes:
                if p.error:
                    is_error = True
                    continue
                cur_boot_duration = p.stdout.strip()
                if cur_boot_duration:
                    boot_duration[p.host.address]['ssh'] = cur_boot_duration
            if is_error:
                logger.info('Retrying [#%s] because there is error in Taktuk\n' %
                            (self.options.number_of_retries - retries + 1))
                retries -= 1
                # sleep 10 seconds then retry to get boot time
                sleep(7)
            else:
                break
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
                    # sleep 20 seconds then retry
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
                                               boot_duration[vm['ip']]['link_up'],
                                               boot_duration[vm['ip']]['ssh'],
                                               vm['cpuset']))
            # text_file.write(xpvms_index[vm['id']] + ',' + boot_duration[vm['ip']] + ',' + vm['cpuset'] + '\n')
        text_file.close()

        logger.info('Copying the vmstat monitor file')
        stress = TaktukGet(host, ['/root/vmstat.txt'], comb_dir).run()
        logger.info('Copying the iostat monitor file')
        stress = TaktukGet(host, ['/root/iostat.txt'], comb_dir).run()

        # print '-->DEBUG', 'dd', dd, 'host_speed', host_speed, 'vms', vms
        if self.options.dd:
            logger.info('Writing disk speed in result files')
            with open(comb_dir + "disk_speed.txt", "w") as text_file:
                for vm in vms:
                    for cur_measurement in dd[vm['ip']]:
                        text_file.write('%s,%s,%s,%s\n' % (xpvms_index[vm['id']],
                                                           cur_measurement['speed'], cur_measurement['unit'],
                                                           host_speed))
        return True

    def create_vms(self, comb, host, ip_mac):
        """ """
        # set the ID of the virtual machine
        vms_ids = ['vm-' + str(i) for i in range(comb['n_vm'])] + \
            ['covm-' + str(i) for i in range(comb['n_co_vms'] * comb['n_vm'])]
        # set the disk
        backing_file = self.options.image

        real_file = comb['image_policy'] == 'one_per_vm'
        # set the CPU
        n_cpu = comb['n_cpu']
        if comb['cpu_policy'] == 'one_by_core':
            cpusets = [','.join(str(i) for j in range(comb['n_cpu']))
                       for i in range(comb['n_vm'])]
            cpusets += [str(comb['n_vm'] + k)
                        for k in range(comb['n_co_vms'] * comb['n_vm'])]
        else:
            cpusets = [str(0)] * comb['n_vm']
            cpusets += [str(1)] * comb['n_co_vms'] * comb['n_vm']
        # set the memory
        mem = comb['n_mem'] * 1024

        #logger.info('Create VMS %s %s', str(len(vms_ids)), str(len(ip_mac)))
        #logger.info('Create VMs with parameters:\nCpusets: %s\nComb:%s' % (cpusets, slugify(comb)))
        # define all the virtual machines
        vms = define_vms(vms_ids,
                         host=host,
                         ip_mac=ip_mac,
                         n_cpu=n_cpu,
                         cpusets=cpusets,
                         mem=mem,
                         backing_file=backing_file,
                         real_file=real_file)

        if self.options.disk == 'ceph':
            logger.info('Creating VM disks on ceph')
            if self.options.ceph_backing_local:
                backing_file_dir = '/tmp/'
            else:
                backing_file_dir = '/tmp/backing_file/'
            for index, vm in enumerate(vms):
                create = create_disks([vm],
                                      data_file_dir='/tmp/data_%s/' % (index + 1),
                                      backing_file_dir=backing_file_dir).run()
            logger.info('Sleep %s seconds after created disks and installed %s vms' %
                        (15 + (4 * len(vms_ids)), len(vms_ids)))
            sleep(15 + (4 * len(vms_ids)))
            
        elif self.options.disk == 'opendedup':
            logger.info('Creating disks with opendedup')
            create = create_disks(vms,
                                  data_file_dir='/tmp/dedup/',
                                  backing_file_dir='/tmp/dedup/').run()

            if comb['image_policy'] == 'one_per_vm':
                logger.info('Sleep %s seconds after created disks and installed %s vms' %
                            (3 * len(vms_ids), len(vms_ids)))
                sleep(3 * len(vms_ids))
        else:
            logger.info('Creating VM disks on HDD/SSD')
            create = create_disks(vms).run()

            if comb['image_policy'] == 'one_per_vm':
                logger.info('Sleep %s seconds after created disks and installed %s vms' %
                            (10 + 10 * len(vms_ids), len(vms_ids)))
                sleep(10 + 10 * len(vms_ids))
            else:
                logger.info('Sleep %s seconds after created disks and installed %s vms' %
                            (5 + 2 * len(vms_ids), len(vms_ids)))
                sleep(5 + 2 * len(vms_ids))
        if not create.ok:
            logger.error('Unable to create the VMS disks for %s ',
                         slugify(comb))
            exit()

        #set cache mode for VMs
        cache_mode = comb.get('cache_mode', 'writethrough')

        logger.info('Installing VMS')
        if self.options.disk == 'ceph':
            for index, vm in enumerate(vms):
                install = install_vms([vm],
                                      data_file_dir='/tmp/data_%s/' % (index + 1), cache_mode=cache_mode).run()
        else:
            install = install_vms(vms, cache_mode=cache_mode).run()
        if not install.ok:
            logger.error('Unable to install the VMS for %s ', slugify(comb))
            exit()
        return vms

    def boot_vms(self, host, comb, xpvms):
        if comb['boot_policy'] == 'all_at_once':
            start_vms(xpvms).run()
            booted = wait_vms_have_started(xpvms)
        elif comb['boot_policy'] == 'one_by_one':
            for evm in xpvms:
                logger.info('Boot eVM: %s' % evm['id'])
                start_vms([evm]).run()
                booted = wait_vms_have_started([evm])
                sleep(2)
        elif comb['boot_policy'] == 'cache_all_at_once':
            logger.info('Loading image into cache')
            if self.options.disk == 'ceph':
                if self.options.ceph_backing_local:
                    cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command.sh'''
                else:
                    cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command_ceph.sh'''
            else:
                cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command.sh'''

            run = TaktukRemote(cmd, host).run()

            logger.info('Loading image into cache ... DONE')
            sleep(1)

            start_vms(xpvms).run()
            booted = wait_vms_have_started(xpvms)
        elif comb['boot_policy'] == 'boot_and_cache':
            logger.info('Loading image into cache')
            if self.options.disk == 'ceph':
                if self.options.ceph_backing_local:
                    cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command.sh'''
                else:
                    cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command_ceph.sh'''
            else:
                cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command.sh'''

            run = TaktukRemote(cmd, host).start()
            start_vms(xpvms).run()
            booted = wait_vms_have_started(xpvms)

        elif comb['boot_policy'] == 'boot_one_cache_then_all':
            logger.info('Loading image into cache')
            if self.options.disk == 'ceph':
                if self.options.ceph_backing_local:
                    cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command.sh'''
                else:
                    cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command_ceph.sh'''
            else:
                cmd = '''/usr/bin/time -o /root/time_cache.txt -p bash /tmp/vmtouch_command.sh'''

            run = TaktukRemote(cmd, host).start()

            first_vm = [xpvms[0]]
            # start_vms(first_vm, cmd).run()
            start_vms(first_vm).run()
            booted = wait_vms_have_started(first_vm)

            if len(xpvms) > 1:
                other_vms = xpvms[1:]
                start_vms(other_vms).run()
                booted = wait_vms_have_started(other_vms)

        else:
            first_vm = [xpvms[0]]
            start_vms(first_vm).run()
            booted = wait_vms_have_started(first_vm)
            if len(xpvms) > 1:
                other_vms = xpvms[1:]
                start_vms(other_vms).run()
                booted = wait_vms_have_started(other_vms)

    def umount_mount_tmp(self, host, retry=10):
        if self.options.disk == 'ssd':
            umount = SshProcess('sync; echo 3 > /proc/sys/vm/drop_caches; '
                                'umount /tmp; sleep 5; mount -t ext4 /dev/sdf1 /tmp',
                                host, shell=True).run()
        elif self.options.disk == 'ceph':
            if self.options.ceph_backing_local:
                backing_file_dir = '/tmp'
            else:
                backing_file_dir = '/tmp/backing_file'
            umount = SshProcess('sync; echo 3 > /proc/sys/vm/drop_caches; '
                                'umount %s; sleep 5; mount -t ext4 /dev/rbd0 %s' % backing_file_dir,
                                host, shell=True).run()
        elif self.options.disk == 'opendedup':
            umount = SshProcess('sync; echo 3 > /proc/sys/vm/drop_caches; '
                                "umount /tmp/dedup; sleep 5; mount.sdfs pool_dedup /tmp/dedup; sleep 10; echo -ne '\n'",
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

    def kflops(self, vms):
        """Prepare a benchmark command with kflops"""
        logger.info("Running cpu kflop with %s" % vms)
        vms_ip = [vm['ip'] for vm in vms]
        vms_out = [vm['ip'] + '_' + vm['cpuset'] for vm in vms]

        stress = TaktukRemote('./benchs/kflops/kflops > /root/kflops_{{vms_out}}.out',
                              vms_ip)

        return stress

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
        logger.info('Setup SSD disk for nodes')

        cmd = 'umount /tmp; mount -t ext4 /dev/sdf1 /tmp;'
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

        logger.info('Mounting backing_file volume')
        cmd = 'rbd --pool rbd map backing_file'
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        cmd = 'mkfs.ext4 -F /dev/rbd0'
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        cmd = 'mkdir /tmp/backing_file'
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        cmd = 'mount /dev/rbd0 /tmp/backing_file'
        p = setup.fact.get_remote(cmd, setup.hosts).run()
        setup._actions_hosts(p)

        for index in range(1, 17):
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

    def setup_hosts(self):
        """ """
        disks = [self.options.image]
        logger.info('Initialize vm5k_deployment')

        setup = vm5k_deployment(vms=[], resources=self.resources,
                                env_name=self.options.env_name,
                                env_file=self.options.env_file)
        setup.fact = ActionFactory(remote_tool=TAKTUK,
                                   fileput_tool=CHAINPUT,
                                   fileget_tool=SCP)
        logger.info('Deploy hosts')
        setup.hosts_deployment()
        
        if self.options.disk == 'ssd':
            self._setup_ssd(setup)

        setup.packages_management(
            other_packages='iftop sysstat time build-essential parallel gawk nload git', launch_disk_copy=False)

        if self.options.disk == 'ceph':
            #Install ceph
            logger.info('Install ceph + packages in a different way')
            cmd = '''export DEBIAN_FRONTEND=noninteractive && ''' + \
                  '''apt-get update -y --force-yes && ''' + \
                  '''apt-get install -y --force-yes ceph'''
            p = setup.fact.get_remote(cmd, self.hosts).run()
            setup._actions_hosts(p)
            logger.info('Install ceph + packages in a different way ... DONE')

            #Setup CEPH client        
            logger.info('Get setting from CEPH master')
            host = self.hosts[0]
            cmd = '''ceph-deploy install %s''' % (host)
            p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()
            print p
            cmd = '''ceph-deploy admin %s''' % (host)
            p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()
            print p
            logger.info('Get setting from CEPH master ... DONE')

            logger.info('Create volume for backing file in CEPH master')
            cmd = 'rbd create backing_file --size 30720 --pool rbd'
            p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()

            for index in range(1, 17):
                logger.info('Create volume for data %s in CEPH master' % index)
                cmd = 'rbd create data_%s --size 30720 --pool data' % index
                p = setup.fact.get_remote(cmd, [self.options.ceph_master]).run()

            logger.info('Create volume in CEPH master ... DONE')

            self._setup_ceph(setup)

            backing_file_dir = '/tmp/backing_file'
            data_file_dir = '/tmp/data'
            
        elif self.options.disk == 'opendedup':
            logger.info('Install OpenDedup on host')
            setup._other_packages(other_packages='fuse')
            setup.fact.get_remote("wget http://www.opendedup.org/downloads/sdfs-latest.deb", self.hosts).run()
            setup.fact.get_remote("dpkg -i sdfs-latest.deb", self.hosts).run()

            logger.info('Change the maximum of openfile on host')
            setup.fact.get_remote('echo "* hard nofile 65535" >> /etc/security/limits.conf', self.hosts).run()
            setup.fact.get_remote('echo "* soft nofile 65535" >> /etc/security/limits.conf', self.hosts).run()

            logger.info('Make dir for OpenDedup volume')
            setup.fact.get_remote('mkdir /tmp/dedup', self.hosts).run()

            total_machines = 16
            logger.info('Creating OpenDedup volume of %s GB' % (4 * total_machines))
            setup.fact.get_remote('mkfs.sdfs --volume-name=pool_dedup --volume-capacity=%sGB' %
                                  (4 * total_machines), self.hosts).run()

            logger.info('Mounting -> umount OpenDedup volume')

            setup.fact.get_remote(
                "mount.sdfs pool_dedup /tmp/dedup && sleep 10 && umount /tmp/dedup && echo -ne '\n'", self.hosts).run()

            setup.fact.get_remote(
                "mount.sdfs pool_dedup /tmp/dedup && sleep 10 && echo -ne '\n'", self.hosts).run()

            backing_file_dir = '/tmp/dedup'
            data_file_dir = '/tmp/dedup'

            logger.info('Mounting OpenDedup volume ... DONE')
            sleep(50)
        else:
            backing_file_dir = '/tmp'
            data_file_dir = '/tmp'

        if self.options.ceph_backing_local:
            backing_file_dir = '/tmp'
            data_file_dir = '/tmp/data'

        setup._start_disk_copy(disks, backing_file_dir=backing_file_dir)

        logger.info('Configure libvirt')
        setup.configure_libvirt()
        logger.info('Create backing file')

        if self.options.disk == 'ssd':
            self._setup_ssd(setup)
        setup._create_backing_file(disks=disks, backing_file_dir=backing_file_dir)
        if self.options.bandwidth:
            self._setup_iperf3_host(setup)

        logger.info('Install vmtouch')
        cmd = '''git clone https://github.com/hoytech/vmtouch.git && ''' + \
            '''cd vmtouch && ''' + \
            '''make && ''' \
            '''make install'''
        p = setup.fact.get_remote(cmd, self.hosts).run()
        setup._actions_hosts(p)
        logger.info('Install vmtouch ... DONE')

        #Copy the network limit script
        logger.info('Copy the vmtouch script file')
        TaktukPut(setup.hosts, ['/home/lnguyen/boottime/dependency_boottime/vmtouch_command_ceph.sh'], remote_location='/tmp/').run()
        TaktukPut(setup.hosts, ['/home/lnguyen/boottime/dependency_boottime/vmtouch_command.sh'], remote_location='/tmp/').run()

        if self.options.ksm:
            logger.info('Enabling KSM')
            cmd = 'echo 1 > /sys/kernel/mm/ksm/run'
            p = setup.fact.get_remote(cmd, self.hosts).run()
            logger.info('Enabling KSM ... DONE')

        if self.options.latency or self.options.limit:
            logger.info('Add ifb interface')
            cmd = '''
                modprobe ifb &&
                ifconfig ifb0 up &&
                tc qdisc add dev br0 ingress &&
                tc filter add dev br0 parent ffff: protocol ip prio 10 u32 match ip src 0.0.0.0/0 flowid ffff: action mirred egress redirect dev ifb0
            '''
            p = setup.fact.get_remote(cmd, self.hosts).run()
            logger.info('Add ifb interface ... DONE')

            is_error = False
            get_pid = setup.fact.get_remote('netstat -i', self.hosts).run()
            for p in get_pid.processes:
                if p.error:
                    is_error = True
                    continue
                print 'stdout: %s' % p.stdout
                print 'stderr: %s' % p.stderr

                if 'ifb0' not in p.stdout and 'ifb0' not in p.stderr:
                    logger.info('ifb interface error')
                    raise Exception('ifb error')
            if is_error:
                logger.info('ifb interface error')
                raise Exception('ifb error in commands')


    def run(self):
        """The main experimental workflow, as described in
        ``Using the Execo toolkit to perform ...``
        """

        p = Process('grep -m 1 cache_mode /home/lnguyen/boottime/vm5k/src/vm5k/actions.py').run()
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

        if self.options.disk == 'ceph' and self.cluster not in ['econome', 'paravance', 'parasilo', 'graphene', 'grimoire', 'grisou']:
            logger.info('Ceph only runs on Nantes and Rennes clusters')
            exit()

        if self.options.disk == 'ceph' and not self.options.ceph_master:
            logger.info('CEPH disk needs server the CEPH master node hostname')
            return None

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
    engine = VMBootMeasurement()
    engine.start()
