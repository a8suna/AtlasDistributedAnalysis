import awkward as ak
import vector
import uproot

variables = [
    'lep_pt', 'lep_eta', 'lep_phi', 'lep_e',
    'lep_charge', 'lep_type',
    'trigE', 'trigM',
    'lep_isTrigMatched',
    'lep_isLooseID', 'lep_isMediumID', 'lep_isLooseIso',
]

def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:,0] + lep_type[:,1] + lep_type[:,2] + lep_type[:,3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

def cut_lep_charge(lep_charge):
    return (lep_charge[:,0] + lep_charge[:,1] +
            lep_charge[:,2] + lep_charge[:,3]) != 0

def calc_mass(pt, eta, phi, e):
    p4 = vector.zip({"pt": pt, "eta": eta, "phi": phi, "E": e})
    return (p4[:,0] + p4[:,1] + p4[:,2] + p4[:,3]).M

def process_file(file_url):
    tree = uproot.open(file_url + ":analysis")
# Define empty list to hold all data for this sample
    sample_data = []

# Perform the cuts for each data entry in the tree
    for data in tree.iterate(variables, library="ak"): # the data will be in the form of an awkward array
        # We can use data[~boolean] to remove entries from the data set
        lep_type = data['lep_type']
        data = data[~cut_lep_type(lep_type)]
        lep_charge = data['lep_charge']
        data = data[~cut_lep_charge(lep_charge)]

        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_e'])

        # Append data to the whole sample data list
        sample_data.append(data)

    # turns sample_data back into an awkward array
    data_A = ak.concatenate(sample_data)
    return data_A 