import subprocess
import os
import numpy as np
import atlasopenmagic as atom
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


GeV = 1.0
xmin = 80 * GeV
xmax = 250 * GeV
step_size = 2.5 * GeV
lumi = 36.6
fraction = 1.0


MAX_WORKERS = 4  # number of parallel workers

defs = {
    'Data': {'dids': ['data'], 'label': 'Data'},

    'Background_ZtbarttbartplusVVVV': {
        'dids': [410470,410155,410218,410219,412043,
                 364243,364242,364246,364248,
                 700320,700321,700322,700323,700324,700325],
        'color': "#6b59d3",
        'label': r'Background $Z,t\bar{t},t\bar{t}+V,VVV$'
    },

    'Background_ZZstar': {
        'dids':[700600],
        'color':"#ff0000",
        'label': r'Background $ZZ^{*}$'
    },

    'Signal_(m_H_=_125_GeV)': {
        'dids':[345060,346228,346310,346311,346312,
                346340,346341,346342],
        'color':"#00cdff",
        'label': r'Signal ($m_H$ = 125 GeV)'
    }
}

atom.set_release("2025e-13tev-beta")
samples = atom.build_dataset(defs, skim="exactly4lep", protocol="https", cache=True)

os.makedirs("results", exist_ok=True)

#list of jobs

jobs = []
i = 0

for sample_name in samples:
    for file_url in samples[sample_name]["list"]:
        output = f"results/{sample_name}_{i}.npz"
        jobs.append((sample_name, file_url, output))
        i += 1

print(f"Total jobs: {len(jobs)}")

#call worker through cmd

def run_job(job):

    sample_name, file_url, output = job

    cmd = [
        "python", "src/workercheck.py",
        "--file", file_url,
        "--sample", sample_name,
        "--output", output,
        "--lumi", str(lumi),
        "--fraction", str(fraction)
    ]

    subprocess.run(cmd, check=True)

# Run in parallel

print(f"Running jobs with {MAX_WORKERS} workers...")

#start timing
start_time = time.time()

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = [executor.submit(run_job, j) for j in jobs]

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Job failed: {e}")

end_time = time.time()
print(f"Total processing runtime: {end_time - start_time:.2f} seconds")
print("All jobs finished. Merging results...")


#merge results

all_data = {}

for f in os.listdir("results"):

    data = np.load(f"results/{f}")

    sample = data["sample"][0]
    masses = data["masses"]
    weights = data["weights"]

    if sample not in all_data:
        all_data[sample] = {"mass":[], "weights":[]}

    all_data[sample]["mass"].extend(masses)
    all_data[sample]["weights"].extend(weights)


for s in all_data:
    all_data[s]["mass"] = np.array(all_data[s]["mass"])
    all_data[s]["weights"] = np.array(all_data[s]["weights"])


print("plotting results")



#plot results


bin_edges = np.arange(xmin, xmax+step_size, step_size)
bin_centres = np.arange(xmin+step_size/2, xmax+step_size/2, step_size)

data_x,_ = np.histogram(all_data['Data']['mass'], bins=bin_edges)
data_x_errors = np.sqrt(data_x)

signal = 'Signal_(m_H_=_125_GeV)'

signal_x = all_data[signal]["mass"]
signal_weights = all_data[signal]["weights"]
signal_color = defs[signal]["color"]

mc_x = []
mc_weights = []
mc_colors = []
mc_labels = []

for s in defs:
    if s not in ['Data', signal]:

        mc_x.append(all_data[s]["mass"])
        mc_weights.append(all_data[s]["weights"])
        mc_colors.append(defs[s]["color"])
        mc_labels.append(defs[s]["label"])


fig, main_axes = plt.subplots(figsize=(12, 8))

main_axes.errorbar(
    bin_centres,
    data_x,
    yerr=data_x_errors,
    fmt='ko',
    label='Data'
)

mc_heights = main_axes.hist(
    mc_x,
    bins=bin_edges,
    weights=mc_weights,
    stacked=True,
    color=mc_colors,
    label=mc_labels
)

mc_x_tot = mc_heights[0][-1]

mc_x_err = np.sqrt(
    np.histogram(
        np.hstack(mc_x),
        bins=bin_edges,
        weights=np.hstack(mc_weights) ** 2
    )[0]
)

main_axes.hist(
    signal_x,
    bins=bin_edges,
    bottom=mc_x_tot,
    weights=signal_weights,
    color=signal_color,
    label=r'Signal ($m_H$ = 125 GeV)'
)

main_axes.bar(
    bin_centres,
    2 * mc_x_err,
    alpha=0.5,
    bottom=mc_x_tot - mc_x_err,
    color='none',
    hatch="////",
    width=step_size,
    label='Stat. Unc.'
)

main_axes.set_xlim(xmin, xmax)
main_axes.set_ylim(0, np.amax(data_x) * 2)

main_axes.xaxis.set_minor_locator(AutoMinorLocator())
main_axes.yaxis.set_minor_locator(AutoMinorLocator())

main_axes.tick_params(which='both', direction='in', top=True, right=True)

main_axes.set_xlabel(
    r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
    fontsize=13,
    x=1,
    ha='right'
)

main_axes.set_ylabel(
    'Events / ' + str(step_size) + ' GeV',
    y=1,
    ha='right'
)

plt.text(0.05, 0.93, 'ATLAS Open Data', transform=main_axes.transAxes, fontsize=16)
plt.text(0.05, 0.88, 'for education', transform=main_axes.transAxes, fontsize=12, style='italic')
plt.text(0.05, 0.82,
         r'$\sqrt{s}$=13 TeV,$\int$L dt = ' + str(lumi) + r' fb$^{-1}$',
         transform=main_axes.transAxes,
         fontsize=16)

plt.text(0.05, 0.76,
         r'$H \rightarrow ZZ^* \rightarrow 4\ell$',
         transform=main_axes.transAxes,
         fontsize=16)

main_axes.legend(frameon=False, fontsize=13)

fig.savefig("higgs_plot.png", dpi=150, bbox_inches='tight')


plt.show()