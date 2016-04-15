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

    leg_thru = plt.plot(thru[:,0], thru[:,1], color='darkblue', label='Data Ingestion Rate')
    pylab.ylabel('Data Ingestion Rate(Msgs/s)', fontsize=10)
    pylab.ylim((0,400))
    ax2.tick_params(axis='y', colors='darkblue')
    ax2.yaxis.label.set_color('darkblue')
    ax2.xaxis.set_major_formatter(NullFormatter())
    pylab.xlabel('Time', fontsize=10)

    ax = ax2.twinx()
    leg_comp = ax.step(instances[:,0], instances[:,1], color='magenta', label='Number of Sketch Instances')
    ax.tick_params(axis='y', colors='magenta')
    ax.yaxis.label.set_color('magenta')
    pylab.ylim((0,1100))

    pylab.xlabel('Time', fontsize=10)
    pylab.ylabel('Number of Sketch Instances', fontsize=10)
    plt.tick_params(axis='x', which='major', labelsize=10)
    plt.tick_params(axis='y', which='major', labelsize=10)
    plt.tick_params(axis='both', which='minor', labelsize=9)
    
    lns = leg_comp + leg_thru
    labs = [l.get_label() for l in lns]
    leg = ax.legend(lns, labs, fontsize=9, ncol=2)
    leg.get_frame().set_linewidth(0.1)
    pylab.tight_layout()
    plt.savefig('figs/dyn_scaling_v3.pdf', dpi=300)
    plt.close()

if __name__ == '__main__':
    main()
