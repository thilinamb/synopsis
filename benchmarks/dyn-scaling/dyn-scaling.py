import math
import matplotlib.pyplot as plt
import numpy as np
import pylab
from pylab import *
import matplotlib.dates as mdate
from numpy import genfromtxt

def main():
    thru = genfromtxt('./data/v3/throughput-profile.stat', delimiter=',')
    instances = genfromtxt('./data/v3/instance-count.stat', delimiter=',')
    fig,ax2 = plt.subplots(figsize=(8,4))

    colors = ['#332288', '#88CCEE', '#44AA99', '#117733', '#999933', '#DDCC77', '#CC6677', '#882255', '#AA4499']
    col_1 = colors[0]
    col_2 = colors[7]

    leg_thru = plt.plot(thru[:,0], thru[:,1], color=col_1, dashes=(1,2), lw=1.3, label='Data Ingestion Rate',)
    pylab.ylabel('Data Ingestion Rate(Msgs/s)', fontsize=13)
    pylab.ylim((0,400))
    ax2.tick_params(axis='y', colors=col_1)
    ax2.yaxis.label.set_color(col_1)
    ax2.xaxis.set_major_formatter(NullFormatter())
    pylab.xlabel('Time', fontsize=13)

    ax = ax2.twinx()
    leg_comp = ax.step(instances[:,0], instances[:,1], lw=1.5, color=col_2, label='Number of Sketchlets')
    ax.tick_params(axis='y', colors=col_2)
    ax.yaxis.label.set_color(col_2)
    pylab.ylim((0,1100))

    pylab.xlabel('Time', fontsize=14)
    pylab.ylabel('Number of Sketchlets', fontsize=13)
    plt.tick_params(axis='x', which='major', labelsize=12)
    plt.tick_params(axis='y', which='major', labelsize=12)
    plt.tick_params(axis='both', which='minor', labelsize=10)

    lns = leg_thru + leg_comp
    labs = [l.get_label() for l in lns]
    leg = ax.legend(lns, labs, fontsize=12, ncol=2, bbox_to_anchor=(0.5, -0.20), loc=8, borderaxespad=0., handlelength=3)
    leg.get_frame().set_linewidth(0.1)
    pylab.tight_layout()
    plt.savefig('figs/dyn_scaling_v4.pdf', dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    main()
