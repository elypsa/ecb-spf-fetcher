# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np

class SurveyData:
    def __init__(self, path):
        self.path = path
        self.csv_files = None
        self.data_raw = None
        self.data = None
        pass
    
    
    def list_csv_files(self):
        # List all files in the specified folder
        self.csv_files = [self.path + f for f in os.listdir(self.path) if f.endswith('.csv')]
    
    @staticmethod
    def _load_csv(file):
        df = pd.read_csv(file, usecols=[0], header = None)
        start_pos = df.index[df.iloc[:, 0] == "TARGET_PERIOD"].to_numpy()
        df_names = df.iloc[start_pos-1,0].to_list()
        csv_dict = {}
        for i, start_ in enumerate(start_pos):
            csv_iterator = pd.read_csv(file, 
                                       skiprows = np.arange(0, start_),
                                       iterator = True, chunksize = 1)
            data = []
            for chunk in csv_iterator:
                if chunk.isnull().all().all():
                    break  # Stop if the line is blank
                data.append(chunk)
            
            data = pd.concat(data, ignore_index=True).dropna(axis=1, how='all')
            data.insert(0, "survey_round", os.path.basename(file).replace('.csv', ''))
            csv_dict[df_names[i]] = data
        return csv_dict
    
    def load_files(self):
        if self.csv_files is None: 
            self.list_csv_files()
        
        self.data_raw = {os.path.basename(csv_file): self._load_csv(csv_file) for csv_file in self.csv_files}
        
    def clean_data(self):
        if self.data_raw is None:
            self.load_files()
        
        all_variables = set() # stores all surveyed vars
        for file_dict in self.data_raw.values():
            all_variables.update(file_dict.keys())
        
        self.data = {
            key: pd.concat(
                [file_dict[key] for file_dict in self.data_raw.values() if key in file_dict],
                ignore_index=True)
            for key in all_variables}
       


if __name__ == "__main__":
    ecb = SurveyData(path = "data/")
    ecb.clean_data()



