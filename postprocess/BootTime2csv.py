#!/usr/bin/env python
import os
import sys
from argparse import ArgumentParser
import pandas as pd
from datetime import datetime


def get_boot_time(path, comb):
    result = []
    vm_boot_time = {}
    dir_name = comb.replace('/', ' ').strip()
    i = iter(dir_name.split('-'))
    data = dict(zip(i, i))
    try:
        with open(path + '/' + dir_name + '/boot_time.txt', "rb") as f:

            for vm, line in enumerate(f):
                vm_index, boot_time_gl, boot_time_ssh, cpuset = line.strip().split(',')
                if not boot_time_gl and not boot_time_ssh:
                    print '[ERROR][BootTime][parse]', comb, '\n', line
                    continue
                if not boot_time_gl:
                    boot_time_gl = '-1'
                if not boot_time_ssh:
                    boot_time_ssh = '-1'
                vm_index = int(vm_index.strip())
                boot_time_gl = float(boot_time_gl.strip())
                boot_time_ssh = float(boot_time_ssh.strip())
                cpuset = int(cpuset.strip())
                vms = {
                    'vm_index': vm_index,
                    'boot_time_gl': boot_time_gl,
                    'boot_time': boot_time_ssh,
                    'cpuset': cpuset
                }
                vm_boot_time[vm_index] = {
                    'boot_time': boot_time_ssh,
                    'boot_time_gl': boot_time_gl
                }
                result.append(dict(data, **vms))
    except IOError as e:
        print '[ERROR][BootTime-IO]', str(e)
    return result, vm_boot_time


def get_performance(path, comb):
    result = []
    vm_performance = {}
    dir_name = comb.replace('/', ' ').strip()
    i = iter(dir_name.split('-'))
    data = dict(zip(i, i))
    try:
        with open(path + '/' + dir_name + '/performance.txt', "rb") as f:

            for vm, line in enumerate(f):
                t = line.strip().split(',')
                if len(t) == 3:
                    vm_index, performance, _ = line.strip().split(',')
                else:
                    vm_index, performance = line.strip().split(',')
                if not performance:
                    print '[ERROR][Performance][parse]', comb, '\n', line
                    continue
                vm_index = int(vm_index.strip())
                performance = float(performance.strip())
                vms = {
                    'vm_index': vm_index,
                    'performance': performance
                }
                vm_performance[vm_index] = {
                    'performance': performance
                }
                result.append(dict(data, **vms))
    except IOError as e:
        print '[ERROR][Performance-IO]', str(e)
    return result, vm_performance


def get_iostat(path, comb):
    final = []
    result = []
    dir_name = comb.replace('/', ' ').strip()
    i = iter(dir_name.split('-'))
    data_comb = dict(zip(i, i))
    try:
        with open(path + '/' + dir_name + '/iostat.txt', "rb") as f:
            period = 0
            for index, line in enumerate(f):
                if 'Linux' in line or 'cpu' in line or 'kB' in line or not line.strip() or '/' in line:
                    continue
                data = line.split()
                # try:
                #     device_name = float(data[0])
                #     period += 1
                #     continue
                # except:
                #     device_name = data[0]
                device_name = data[0]
                # tps, readps, writeps, read, write = data[1:]
                readps, writeps, read, write = data[3:7]

                iostat = {
                    'period': period,
                    'device_name': device_name,
                    # 'tps': float(tps),
                    'read_per_s': float(readps),
                    'write_per_s': float(writeps),
                    'read': float(read),
                    'write': float(write)
                }
                result.append(iostat)
    except IOError as e:
        print '[ERROR][IOStat-IO]', str(e)
        return final
    except Exception as e2:
        print '[ERROR][IOStat-Other]', str(e2)
        return final
    df = pd.DataFrame(result)
    s = df.groupby('device_name').sum()
    s['device_name'] = s.index
    result = s.to_dict('records')
    for r in result:
        final.append(dict(data_comb, **r))
    return final


def get_vmstat(path, comb):
    final = []
    result = []
    dir_name = comb.replace('/', ' ').strip()
    i = iter(dir_name.split('-'))
    data_comb = dict(zip(i, i))
    try:
        with open(path + '/' + dir_name + '/vmstat.txt', "rb") as f:
            period = 0
            for index, line in enumerate(f):
                if 'memory' in line or 'buff' in line or not line.strip():
                    continue
                data = line.split()[5:]
                _, _, swap, free, buff, cache, _, _, bi, bo, interrupt, context_switch, _, _, _, _, _ = data
                vmstat = {
                    'swap': float(swap),
                    'free': float(free),
                    'buff': float(buff),
                    'cache': float(cache),
                    'bi': float(bi),
                    'bo': float(bo),
                    'interrupt': float(interrupt),
                    'context_switch': float(context_switch)
                }
                result.append(vmstat)
    except IOError as e:
        print '[ERROR][VMStat-IO]', str(e)
        return final
    df = pd.DataFrame(result)
    s = df.sum()
    result = s.to_dict()
    final.append(dict(data_comb, **result))
    return final


def get_cache_time(path, comb):
    final = []
    result = []
    dir_name = comb.replace('/', ' ').strip()
    i = iter(dir_name.split('-'))
    data_comb = dict(zip(i, i))
    try:
        with open(path + '/' + dir_name + '/time_cache.txt', "rb") as f:
            for index, line in enumerate(f):
                if 'real' in line:
                    t = float(line.split()[1])
                    vm_cache = {
                        'cache_time': t
                    }
                    result.append(vm_cache)
    except IOError as e:
        print '[ERROR][CacheTime-IO]', str(e)
        return final
    df = pd.DataFrame(result)
    s = df.sum()
    result = s.to_dict()
    final.append(dict(data_comb, **result))
    return final


def get_disk_speed(path, comb, vm_boot_time, host_speed):
    result = []
    speeds = []
    dir_name = comb.replace('/', ' ').strip()
    i = iter(dir_name.split('-'))
    data = dict(zip(i, i))
    try:
        with open(path + '/' + dir_name + '/disk_speed.txt', "rb") as f:

            for vm, line in enumerate(f):
                vm_index, speed, unit, temp = line.strip().split(',')
                host_speed = host_speed if host_speed else float(temp.strip())
                if not speed:
                    print '[ERROR][parse]', comb, '\n', line
                    continue
                vm_index = int(vm_index.strip())
                speed = float(speed.strip())
                # host_speed = float(host_speed.strip())
                unit = unit.strip()
                speeds.append(100 * (1.0 - (speed / host_speed)) if speed <= host_speed else 0.0)
                vms = {
                    'vm_index': vm_index,
                    'speed': speed,
                    'unit': unit,
                    'host_speed': host_speed,
                    # 'p_co_vms': 100 * (1.0 - (speed / host_speed)) if speed <= host_speed else 0.0,
                    'boot_time': vm_boot_time.get(vm_index, dict()).get('boot_time', None),
                    'boot_time_gl': vm_boot_time.get(vm_index, dict()).get('boot_time_gl', None)
                }
            try:
                vms['p_co_vms'] = sum(speeds) / len(speeds)
                result.append(dict(data, **vms))
            except ZeroDivisionError as e:
                print '[ERROR] [DivisionError]', str(e)
    except IOError as e:
        print '[ERROR][DiskSpeed-IO]', str(e)
    return result


def main(options):
    parser = ArgumentParser(prog='execo2csv_index')
    parser.add_argument('-i', '--input', dest='input', type=str, required=True,
                        help='The file path to the input directory of execo.')
    parser.add_argument('-o', '--output', dest='output', type=str, required=True,
                        help='The file path to the output result file.')
    parser.add_argument('-s', '--hostspeed', dest='hostspeed', type=float, required=False, default=None,
                        help='The host speed in MB/s.')
    parser.add_argument('--performance', dest='parse_performance', action='store_true',
                        help='Parse performance')
    parser.add_argument('--iostat', dest='parse_iostat', action='store_true',
                        help='Parse iostat')
    parser.add_argument('--vmstat', dest='parse_vmstat', action='store_true',
                        help='Parse vmstat')
    parser.add_argument('--image_cache', dest='parse_image_cache', action='store_true',
                        help='Parse image cache')
    parser.add_argument('--dd', dest='parse_dd', action='store_true',
                        help='Parse dd')
    args = parser.parse_args()
    print 'Running with the following parameters: %s' % args
    # read input
    print 'Reading input directory', args.input
    list_comb = os.listdir(args.input)
    result = []
    speed = []
    for comb in list_comb:
        if comb not in ['test', 'sweeps', 'stdout+stderr', 'graphs', '.DS_Store']:
            if args.parse_performance:
                r, vm_performance = get_performance(args.input, comb)
            else:
                r, vm_boot_time = get_boot_time(args.input, comb)
            result += r
            if args.parse_dd:
                speed += get_disk_speed(args.input, comb, vm_boot_time, args.hostspeed)

    print 'Total %s result rows' % len(result)
    df = pd.DataFrame(result)
    print 'Exporting to', args.output
    df.to_csv(args.output, index=False)
    if args.parse_iostat:
        result2 = []
        for comb in list_comb:
            if comb not in ['test', 'sweeps', 'stdout+stderr', 'graphs', '.DS_Store']:
                result2 += get_iostat(args.input, comb)
        df2 = pd.DataFrame(result2)
        print 'Exporting iostat'
        df2.to_csv(args.output + '_iostat.csv', index=False)
    if args.parse_vmstat:
        result2 = []
        for comb in list_comb:
            if comb not in ['test', 'sweeps', 'stdout+stderr', 'graphs', '.DS_Store']:
                result2 += get_vmstat(args.input, comb)
        df2 = pd.DataFrame(result2)
        print 'Exporting vmstat'
        df2.to_csv(args.output + '_vmstat.csv', index=False)
    if args.parse_image_cache:
        result2 = []
        for comb in list_comb:
            if comb not in ['test', 'sweeps', 'stdout+stderr', 'graphs', '.DS_Store']:
                result2 += get_cache_time(args.input, comb)
        df2 = pd.DataFrame(result2)
        print 'Exporting cache time'
        df2.to_csv(args.output + '_cache_time.csv', index=False)
    if args.parse_dd:
        df = pd.DataFrame(speed)
        mean_df = df.groupby(['image_policy', 'n_co_vms'])[['boot_time', 'p_co_vms']].mean().to_dict()
        median_df = df.groupby(['image_policy', 'n_co_vms'])[['boot_time', 'p_co_vms']].median().to_dict()
        df['boot_time_mean'] = df.apply(lambda x: mean_df['boot_time'][(x['image_policy'], x['n_co_vms'])], axis=1)
        df['boot_time_median'] = df.apply(lambda x: median_df['boot_time'][(x['image_policy'], x['n_co_vms'])], axis=1)

        mean_df = df.groupby(['image_policy', 'n_co_vms'])[['boot_time_gl', 'p_co_vms']].mean().to_dict()
        median_df = df.groupby(['image_policy', 'n_co_vms'])[['boot_time_gl', 'p_co_vms']].median().to_dict()
        df['boot_time_gl_mean'] = df.apply(lambda x: mean_df['boot_time_gl']
                                           [(x['image_policy'], x['n_co_vms'])], axis=1)
        df['boot_time_gl_median'] = df.apply(lambda x: median_df['boot_time_gl']
                                             [(x['image_policy'], x['n_co_vms'])], axis=1)

        df['p_co_vms_mean'] = df.apply(lambda x: mean_df['p_co_vms'][(x['image_policy'], x['n_co_vms'])], axis=1)
        df['p_co_vms_median'] = df.apply(lambda x: median_df['p_co_vms'][(x['image_policy'], x['n_co_vms'])], axis=1)

        df.to_csv(args.output, index=False)


if __name__ == "__main__":
    main(sys.argv[1:])
