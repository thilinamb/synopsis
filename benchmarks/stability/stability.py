import math
import matplotlib.pyplot as plt
import numpy as np
import pylab
from pylab import *
import matplotlib.dates as mdate
from numpy import genfromtxt
import matplotlib.patches as patches

def parse_scaling_activities(dataset):
    count = dataset.shape[0]
    activities = []
    for i in range(0, count, 2):
        activities.append([dataset[i,1], dataset[i+1,1]])

    return activities

def main():
    version = 'partial'
    base_dir = './data/' + version
    backlog = genfromtxt(base_dir + '/backlog.stat', delimiter=',')
    input_rates = genfromtxt(base_dir +'/input_rates.stat', delimiter=',')
    scale_in = genfromtxt(base_dir +'/scale_in.stat', delimiter=',')
    scale_out = genfromtxt(base_dir +'/scale_out.stat', delimiter=',')
    throughput = genfromtxt(base_dir +'/throughput.stat', delimiter=',')

    scale_out_activities = parse_scaling_activities(scale_out)
    scale_in_activities = parse_scaling_activities(scale_in)

    colors = ['#332288', '#88CCEE', '#44AA99', '#117733', '#999933', '#DDCC77', '#CC6677', '#882255', '#AA4499']
    col_1= colors[0]
    col_2= 'black'
    col_3 = colors[3]
    col_4 = colors[7]

    fig,ax2 = plt.subplots(figsize=(8.5,4))
    leg_backlog = plt.plot(backlog[:,0], backlog[:,1], lw=0.8, label='Backlog Size', color=col_1)
    xmin, xmax = ax2.get_xlim()
    leg_threshold = plt.plot((xmin, xmax), (20, 20), '-.', lw=1.2, color=col_2, label='Scale Out Threshold')
    ax2.xaxis.set_major_formatter(NullFormatter())

    pylab.ylabel('Input Rate(Messages/s)', fontsize=12)
    pylab.xlabel('Time', fontsize=12)

    leg_input_rate = plt.plot(input_rates[:,0], input_rates[:,1]/2527 ,dashes=(3,1), color=col_3, label='Input Rate')

    for entry in scale_out_activities:
        rect_scale_out = ax2.add_patch(patches.Rectangle((entry[0],0), (entry[1] - entry[0]), 350, alpha=0.4, color='orange', label='Scale Out'))

    for entry in scale_in_activities:
        rect_scale_in = ax2.add_patch(patches.Rectangle((entry[0],0), (entry[1] - entry[0]), 350, alpha=0.4, color='deepskyblue', label='Scale In'))

    leg_thru = plt.plot(throughput[:,0], throughput[:,1], color=col_4, lw=1.3, label='Throughput')

    lns = leg_backlog + leg_threshold + leg_input_rate + leg_thru
    lns = lns + [rect_scale_out, rect_scale_in]
    labs = [l.get_label() for l in lns]
    leg = ax2.legend(lns, labs, fontsize=11, ncol=4, bbox_to_anchor=(0.5, -0.26), loc=8, borderaxespad=0., handlelength=3)
    leg.get_frame().set_linewidth(0.1)
    pylab.tight_layout()

    plt.savefig('./figs/stability_{}.pdf'.format(version), dpi=300, bbox_extra_artists=(leg,), bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    main()
