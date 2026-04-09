import argparse
import numpy as np
import awkward as ak
from analysis import process_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--sample",required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--lumi",type=float, default=36.6)
    parser.add_argument("--fraction",type=float, default=1.0)
    args = parser.parse_args()

    print(f"Processing: {args.file}")

    result = process_file(file_url=args.file,sample_name=args.sample,lumi=args.lumi,fraction=args.fraction,)

    masses = ak.to_numpy(result['mass'])

    is_data = (args.sample == 'Data')
    if is_data:
        weights = np.ones(len(masses))
    else:
        weights = ak.to_numpy(result['totalWeight'])

    np.savez(args.output, masses=masses, weights=weights,sample=np.array([args.sample]),)


if __name__ == "__main__":
    main()