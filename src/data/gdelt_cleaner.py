import argparse
from src.data import GdeltCleaner.GdeltCleaner

def clean_data(filename):
    cleaner = GdeltCleaner()

    cleaner.load_raw_dataset

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="File name to load from (no .csv extension needed")
    
    args = parser.parse_args()

    clean_data(args.filename)
    print("Cleaned!")