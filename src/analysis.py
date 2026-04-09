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
    sample_data = []

    for data in tree.iterate(variables, library="ak"):

        # Trigger cut
        data = data[data['trigE'] | data['trigM']]

       
        data = data[ak.sum(data['lep_isTrigMatched'], axis=1) >= 1]

        # Transverse momentum cuts
        data = data[data['lep_pt'][:,0] > 20]  
        data = data[data['lep_pt'][:,1] > 15]  
        data = data[data['lep_pt'][:,2] > 10]  

        # ID and isolation
        pid = data['lep_type']
        data = data[
            ak.sum(
                ((pid == 13) & data['lep_isMediumID'] & data['lep_isLooseIso']) |
                ((pid == 11) & data['lep_isLooseID']  & data['lep_isLooseIso']),
                axis=1
            ) == 4
        ]

        # Lepton type and charge cuts
        data = data[~cut_lep_type(data['lep_type'])]
        data = data[~cut_lep_charge(data['lep_charge'])]

        # Invariant mass
        data['mass'] = calc_mass(
            data['lep_pt'], data['lep_eta'],
            data['lep_phi'], data['lep_e']
        )

        sample_data.append(data)

    return ak.concatenate(sample_data)