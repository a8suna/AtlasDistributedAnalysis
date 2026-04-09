import awkward as ak
import vector
import uproot

# Units
MeV = 0.001
GeV = 1.0

# Variables
variables = [
    'lep_pt', 'lep_eta', 'lep_phi', 'lep_e',
    'lep_charge', 'lep_type',
    'trigE', 'trigM',
    'lep_isTrigMatched',
    'lep_isLooseID', 'lep_isMediumID', 'lep_isLooseIso',
    'lep_n',
]

# Weight variables 
weight_variables = [
    "filteff", "kfac", "xsec", "mcWeight",
    "ScaleFactor_PILEUP", "ScaleFactor_ELE",
    "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER",
]


def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool # True means we should remove this entry (lepton type does not match)

def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge # True means we should remove this entry (sum of lepton charges is not equal to 0)

def calc_mass(lep_pt, lep_eta, lep_phi, lep_e):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_e})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M # .M calculates the invariant mass
    return invariant_mass


def cut_trig_match(lep_trigmatch):
    trigmatch = lep_trigmatch
    cut1 = ak.sum(trigmatch, axis=1) >= 1
    return cut1

def cut_trig(trigE,trigM):
    return trigE | trigM


def ID_iso_cut(IDel,IDmu,isoel,isomu,pid):
    thispid = pid
    return (ak.sum(((thispid == 13) & IDmu & isomu) | ((thispid == 11) & IDel & isoel), axis=1) == 4)


def calc_weight(weight_variables, events, lumi):
    
    total_weight = lumi * 1000 / events["sum_of_weights"]
    for variable in weight_variables:
        total_weight = total_weight * abs(events[variable])
    return total_weight


def process_file(file_url, sample_name, lumi, fraction=1.0):
    """Process a single ROOT file and return an awkward array.

    Parameters:
    file_url    : str - path to the root file
    sample_name : str - sample key 
    lumi        : float - luminosity in fb-1
    fraction    : float- fraction of events 
    """

    is_data = (sample_name == 'Data')

    read_vars = variables + ['sum_of_weights']
    if not is_data:
        read_vars = read_vars + weight_variables

    tree = uproot.open(file_url + ':analysis')
    entry_stop = int(tree.num_entries * fraction)

    sample_data = []

    for data in tree.iterate(read_vars, library="ak", entry_stop=entry_stop):

        # Trigger cuts
        data = data[cut_trig(data.trigE, data.trigM)]
        data = data[cut_trig_match(data.lep_isTrigMatched)]

        # Transverse momentum cuts
        data['leading_lep_pt']      = data['lep_pt'][:, 0]
        data['sub_leading_lep_pt']  = data['lep_pt'][:, 1]
        data['third_leading_lep_pt'] = data['lep_pt'][:, 2]
        data['last_lep_pt'] = data['lep_pt'][:,3]

        data = data[data['leading_lep_pt']       > 20]
        data = data[data['sub_leading_lep_pt']   > 15]
        data = data[data['third_leading_lep_pt'] > 10]

        # ID and isolation cuts
        data = data[ID_iso_cut(
            data.lep_isLooseID,
            data.lep_isMediumID,
            data.lep_isLooseIso,
            data.lep_isLooseIso,
            data.lep_type,
        )]

        # Lepton type and charge cuts
        data = data[~cut_lep_type(data['lep_type'])]
        data = data[~cut_lep_charge(data['lep_charge'])]

        # Invariant mass
        data['mass'] = calc_mass(
            data['lep_pt'], data['lep_eta'],
            data['lep_phi'], data['lep_e'],
        )

        # MC weights
        if not is_data:
            data['totalWeight'] = calc_weight(weight_variables, data, lumi)

        sample_data.append(data)

    if len(sample_data) == 0:
        return None

    return ak.concatenate(sample_data)