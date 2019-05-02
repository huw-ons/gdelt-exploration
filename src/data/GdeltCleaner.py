import pandas as pd
import numpy as np
import src.config as cfg


class GdeltCleaner():
    def __init__(self):
        self.raw_dataset = None
        self.raw_dataset_path = f"{cfg.ROOT_DIR}/data/raw"
        self.raw_dataset_filename = None
        
        self.clean_dataset = None
        self.clean_dataset_path = f"{cfg.ROOT_DIR}/data/processed"
        
        self.TONE_COLUMNS = ["V2Tone", "Positive", "Negative", "Polarity", "Activity_density", "Self_density"]
        self.TO_DROP_COLUMNS = ["SourceCollectionIdentifier", "DocumentIdentifier", "Counts", "Themes", "Locations", "Persons", 
        "V2Persons", "Organizations", "Dates", "SharingImage", "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds", "TranslationInfo",
        "Extras"]

    def clean_data(self):
        to_be_cleaned = self.raw_dataset.copy()
 
        to_be_cleaned["DATE"] = pd.to_datetime(to_be_cleaned["DATE"].astype(str).str[0:8], format="%Y%m%d")

        counts_temp = to_be_cleaned["V2Counts"].apply(lambda row: self.split_delimitted_string(row, ";"))
        to_be_cleaned["V2Counts"] = counts_temp.apply(lambda row: self.split_delimitted_list(row, "#"))
        to_be_cleaned["V2Counts_V2Counts"] = to_be_cleaned["V2Counts"].apply(self.list_len)

        to_be_cleaned["Themes"] = to_be_cleaned["Themes"].apply(lambda row: self.split_delimitted_string(row, ";"))
        
        locations_temp = to_be_cleaned["V2Locations"].apply(lambda row: self.split_delimitted_string(row, ";"))
        to_be_cleaned["V2Locations"] = locations_temp.apply(lambda row: self.split_delimitted_list(row, "#"))
        to_be_cleaned["V2Locations_COUNT"] = to_be_cleaned["V2Locations"].apply(self.list_len)
        
        persons_temp = to_be_cleaned["V2Persons"].apply(lambda row: self.split_delimitted_string(row, ";"))
        to_be_cleaned["V2Persons_COUNT"] = persons_temp.apply(self.list_len)

        to_be_cleaned["V2Organizations"] = to_be_cleaned["V2Organizations"].apply(lambda row: self.split_delimitted_string(row, ";"))
        to_be_cleaned["V2Organizations_COUNT"] = to_be_cleaned["V2Organizations"].apply(self.list_len)

        tone_split = to_be_cleaned["V2Tone"].apply(lambda row: self.split_delimitted_string(row, ","))
        tone_explode = pd.DataFrame(tone_split.values.tolist(), columns=self.TONE_COLUMNS, index=tone_split.index)
        tone_explode_numeric = tone_explode.apply(pd.to_numeric)
        to_be_cleaned[self.TONE_COLUMNS] = tone_explode_numeric[self.TONE_COLUMNS]
        
        to_be_cleaned.drop(self.TO_DROP_COLUMNS, axis=1, inplace=True)

        self.clean_dataset = to_be_cleaned

    def split_delimitted_string(self, to_split, delimiter):
        split_string = to_split

        if type(to_split) is str:
            split_string = to_split.split(delimiter)

            if type(split_string) is list:
                split_string = list(filter(None, split_string))

        return split_string

    def split_delimitted_list(self, to_split, delimiter):
        split_string = to_split

        if type(to_split) is list:
            split_string = [string_to_split.split(delimiter) for string_to_split in to_split]

        return split_string

    def list_len(self, to_measure):
        if type(to_measure) is list:
            measured = len(to_measure)

        elif type(to_measure) is float:
            measured = 0

        else:
            print(f"ISSUE WITH {to_measure}")
            measured = -1

        return measured

    def get_raw_dataset(self):
        return self.raw_dataset

    def get_clean_dataset(self):
        return self.clean_dataset

    def load_raw_dataset(self, filename):
        self.raw_dataset = pd.read_csv(f"{self.raw_dataset_path}/{filename}.csv", index_col=[0])
        self.raw_dataset_filename = filename

    def save_clean_dataset(self):
        self.clean_dataset.to_csv(f"{self.clean_dataset_path}/{self.raw_dataset_filename}_CLEANED.csv")
