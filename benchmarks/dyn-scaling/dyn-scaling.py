import math
import matplotlib.pyplot as plt
import numpy as np
import pylab
from pylab import *
import matplotlib.dates as mdate
from numpy import genfromtxt

def main():
	thru = genfromtxt('./data/v2/throughput-profile.stat', delimiter=',')
	instances = genfromtxt('./data/v2/instance-count.stat', delimiter=',')
	fig,ax2 = plt.subplots(figsize=(8,4))

	plt.plot(thru[:,0], thru[:,1], color='cornflowerblue')
	plt.tick_params(axis='x',which='both', bottom='off',top='off', labelbottom='off')
	pylab.ylabel('Data Ingestion Rate(Msgs/s)', fontsize=10)
	ax2.xaxis.set_major_formatter(NullFormatter())

	ax = ax2.twinx()
	ax.step(instances[:,0], instances[:,1], color='orange')
	plt.tick_params(axis='x',which='both', bottom='off',top='off', labelbottom='off')

	pylab.xlabel('Time', fontsize=10)
	pylab.ylabel('Number of Computations', fontsize=10)
	plt.tick_params(axis='x', which='major', labelsize=10)
	plt.tick_params(axis='y', which='major', labelsize=10)
	plt.tick_params(axis='both', which='minor', labelsize=9)

	plt.savefig('figs/dyn_scaling_v2.pdf', dpi=300)
	plt.close()

if __name__ == '__main__':
	main()