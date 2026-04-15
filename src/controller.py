import subprocess
import os
import numpy as np
import atlasopenmagic as atom
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
matplotlib.use('Agg')
import time
import json
import pika
import socket


GeV = 1.0
xmin = 80 * GeV
xmax = 250 * GeV
step_size = 2.5 * GeV
lumi = 36.6
fraction = 1.0


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

#retry loop to connect to broker - additionally adding socker to catch DNS errors.
def connecting_rabbitmq(host="rabbitmq", retries=30, delay=10):
    for attempt in range(retries):
        try:
            print(f"Connecting to rabbitmq (attempt {attempt + 1})...")
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
        except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
            print(f"Not ready ({e}), waiting {delay}s...")
            time.sleep(delay)
    raise RuntimeError("couldn't connect to rabbitmq")


connection = connecting_rabbitmq()
channel = connection.channel()
channel.queue_declare(queue='jobs',    durable=True)
channel.queue_declare(queue='results', durable=True)

# purge queues to delelte previous npz files
channel.queue_purge(queue='jobs')
channel.queue_purge(queue='results')
print("[controller] Queues purged — starting fresh")

atom.set_release("2025e-13tev-beta")
samples = atom.build_dataset(defs, skim="exactly4lep", protocol="https", cache=True)
os.makedirs("results", exist_ok=True)

# Queue all of the jobs
try:
    total_jobs = 0
    for i, (sample_name, file_url) in enumerate(
        (name, url)
        for name in samples
        for url in samples[name]["list"]
    ):
        channel.basic_publish(
            exchange='',
            routing_key='jobs',
            body=json.dumps({
                "file_url":    file_url,
                "sample_name": sample_name,
                "output":      f"results/{sample_name}_{i}.npz",
                "lumi":        lumi,
                "fraction":    fraction,
            }),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"Queued: {sample_name} -> {file_url}")
        total_jobs += 1
except Exception as e:
    print(f"ERROR during publishing: {e}")
    raise

print(f"All {total_jobs} jobs queued")

start_time = time.time() #start timing 
# Collect results 

jobs_done = 0

while jobs_done < total_jobs:
    try:
        # reconnect freshl every time 
        connection = connecting_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue='results', durable=True)

        def on_result(ch, method, properties, body):
            global jobs_done
            msg = json.loads(body)
            jobs_done += 1
            print(f"[controller] {jobs_done}/{total_jobs} complete — {msg.get('output', '')}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if jobs_done >= total_jobs:
                end_time = time.time() 
                print(f"Total processing runtime: {end_time - start_time:.2f} seconds")
                ch.stop_consuming()

        channel.basic_consume(queue='results', on_message_callback=on_result)
        channel.start_consuming()

    except (pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPHeartbeatTimeout) as e:
        print(f"[controller] Connection lost ({e}), reconnecting in 10s... ({jobs_done}/{total_jobs} done so far)")
        time.sleep(10)
        

#merge and plot results
print("All results collected. Running merge and plot...")

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


os.makedirs("/app/output", exist_ok=True)
fig.savefig("/app/output/higgs_plot.png", dpi=150, bbox_inches='tight')
print("Plot saved to /app/output/higgs_plot.png")
plt.close()