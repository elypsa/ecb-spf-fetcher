# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np

class SurveyData:
    """
    A class to handle ECB survey data.
    """
    def __init__(self, path):
        self.path = path
        self.csv_files = None
        self.data_raw = None
        self.data = None
        pass
    
    
    def list_csv_files(self):
        """
        List all CSV files in the specified folder.
        """
        self.csv_files = [self.path + f for f in os.listdir(self.path) if f.endswith('.csv')]
        self.csv_files.sort()
        return self.csv_files
    
    @staticmethod
    def _load_csv(file):
        print(f"Loading {file}")
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
        
    def tidy_survey(self):
        """
        Tidy the survey data.
        """
        if self.data is None:
            self.clean_data()
        
        pattern = (
           r'^(?:F(?P<from_sign>N)?(?P<from_int>\d+)_?(?P<from_dec>\d+))?'
           r'(?:T(?P<to_sign>N)?(?P<to_int>\d+)_?(?P<to_dec>\d+))?$'
        )

        def parse_value(sign, integer, decimal):
            if pd.isna(integer):
                return None
            val = float(f"{'-' if sign == 'N' else ''}{integer}.{decimal}")
            return val

        for key, df in self.data.items():
            if key == "ASSUMPTIONS":
                continue  # Skip the ASSUMPTIONS table
            
            # pivot longer
            df = df.melt(id_vars=["survey_round", "TARGET_PERIOD", "FCT_SOURCE"], var_name="FORECAST", value_name="FORECAST_VALUE")
            # decode FORECAST
            extract_forecast = df["FORECAST"].str.extract(pattern)
            df['from'] = extract_forecast.apply(
                lambda row: parse_value(row['from_sign'], row['from_int'], row['from_dec']),
                axis=1
            )
            df['to'] = extract_forecast.apply(
                lambda row: parse_value(row['to_sign'], row['to_int'], row['to_dec']),
                axis=1
            )
            df['from'] = df['from'].fillna(-np.inf)
            df['to'] = df['to'].fillna(np.inf)
            df["FORECAST_TYPE"] = np.where(df["FORECAST"] == "POINT", "POINT", "DENSITY")
            df["from"] = np.where(df["FORECAST"] == "POINT", np.nan, df["from"])
            df["to"] = np.where(df["FORECAST"] == "POINT", np.nan, df["to"])
            df.dropna(subset=["FORECAST_VALUE"], inplace=True)
            df = df[df["FORECAST_VALUE"] != 0]
            # order by survey_round and TARGET_PERIOD
            df = df.sort_values(by=["survey_round", "TARGET_PERIOD", "FCT_SOURCE", "from", "to"], ignore_index=True)

            # return the tidied dataframe back to dict
            self.data[key] = df
        return self.data


       


if __name__ == "__main__":
    ecb = SurveyData(path = "data/")
    ecb.tidy_survey()
