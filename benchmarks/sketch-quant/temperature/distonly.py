import matplotlib
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import scipy.integrate as integrate
import scipy.stats as stats

from matplotlib import rc
rc('font',**{'family':'sans-serif','sans-serif':['Arial']})

temps = np.loadtxt(fname='./all_temps.txt.bz2')
ticks = np.loadtxt(fname='./test4')

kernel = stats.kde.gaussian_kde(temps)
kernel.covariance_factor = lambda : .20
kernel._compute_covariance()

plt.ion()
plt.clf()

plt.suptitle('Density-Driven Quantization', fontsize=16)

plt.xlabel('Surface Temperature (Kelvin)', fontsize=14)
plt.ylabel('Probability Density', fontsize=14)
plt.axis([237, 320, 0, 0.045])

x = np.array(range(int(temps.min()), int(temps.max()) + 1))
plt.fill_between(x, 0, kernel(x), color='#82c6f5', alpha=0.3)
for tick in ticks[1:, 0]:
    plt.plot([tick, tick], [0, kernel(tick)], color='#2d7fb7', alpha=0.75)
plt.plot(x, kernel(x), color='#5aaae2', lw=4)

plt.savefig('../../../paper/figures/quantization-dist.pdf', bbox_inches='tight')
