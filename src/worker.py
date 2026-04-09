import argparse
import numpy as np
import awkward as ak
from analysis import process_file

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    print("Processing:", args.file)

    result = process_file(args.file)

    masses = ak.to_numpy(result["mass"])

    np.savez(args.output, masses=masses)

    print("Saved", len(masses), "events")

if __name__ == "__main__":
    main()