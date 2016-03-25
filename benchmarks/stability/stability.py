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

    fig,ax2 = plt.subplots(figsize=(8,4))
    plt.plot(backlog[:,0], backlog[:,1], lw=0.6)
    xmin, xmax = ax2.get_xlim()
    plt.plot((xmin, xmax), (20, 20), '--', color='orange')

    plt.plot(input_rates[:,0], input_rates[:,1]/2527, color='g')

    for entry in scale_out_activities:
        ax2.add_patch(patches.Rectangle((entry[0],0), (entry[1] - entry[0]), 350, alpha=0.4, color='r'))

    for entry in scale_in_activities:
        ax2.add_patch(patches.Rectangle((entry[0],0), (entry[1] - entry[0]), 350, alpha=0.4, color='g'))

    plt.plot(throughput[:,0], throughput[:,1], color='magenta')

    plt.savefig('stability_{}.pdf'.format(version), dpi=300)
    plt.close()

if __name__ == '__main__':
    main()
