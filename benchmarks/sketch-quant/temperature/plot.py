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

fig1 = plt.figure(1)

gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1])
ax0 = plt.subplot(gs[0])
ax1 = plt.subplot(gs[1], sharex=ax0)

ax0.set_ylabel('Probability Density', fontsize=14)
ax0.axis([237, 320, 0, 0.045])

ax1.set_xlabel('Surface Temperature (Kelvin)', fontsize=14)
ax1.set_ylabel('NRMSE (%)', fontsize=14)
ax1.axis([237, 320, 0, 10])

plt.setp(ax0.get_xticklabels(), visible=False)

x = np.array(range(int(temps.min()), int(temps.max()) + 1))
ax0.fill_between(x, 0, kernel(x), color='#82c6f5', alpha=0.3)
for tick in ticks[1:, 0]:
    ax0.plot([tick, tick], [0, kernel(tick)], color='#2d7fb7', alpha=0.75)
ax0.plot(x, kernel(x), color='#5aaae2', lw=4)

r = np.array(range(280, 310))
z = integrate.simps(kernel(r), r)
print(z)

t = []
for temp in x:
    found = 0
    for i, tick in enumerate(ticks[:, 0]):
        if temp >= tick:
            found = i
    print(found)
    t.append(ticks[found, 2] * 100.0)

ax1.plot(x, t, lw=2, color='#da2b17')
ax1.grid()

plt.savefig('../../../paper/figures/quantization.pdf', bbox_inches='tight')
