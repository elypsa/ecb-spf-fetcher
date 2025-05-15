# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import pickle

class SurveyData:
    """
    A class to handle ECB survey data.
    Methods include:
    - tidy_survey: Tidy the survey data.
    - save_data: Save the tidied data to a pickle file.
    - clean_data: Clean the survey data.
    - load_files: Load the survey data from CSV files.
    """
    def __init__(self, path):
        """
        Initialize the SurveyData class.
        :param path: Path to the folder containing the CSV files.
        """
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
        """
        Load a CSV file and return a dictionary of DataFrames.
        :param file: Path to the CSV file.
        :return: Dictionary of DataFrames.
        """
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
            data.insert(0, "SURVEY_ROUND", os.path.basename(file).replace('.csv', ''))
            csv_dict[df_names[i]] = data
        return csv_dict
    
    def load_files(self):
        """
        Load the CSV files and store them in a dictionary.
        """
        if self.csv_files is None: 
            self.list_csv_files()
        
        self.data_raw = {os.path.basename(csv_file): self._load_csv(csv_file) for csv_file in self.csv_files}
        
    def clean_data(self):
        """
        Clean the survey data by loading and concatenating all CSV files.
        """
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
        - Reshape the data from wide to long format.
        - Extract forecast values and types.
        - Clean and sort the data.
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
            df = df.melt(id_vars=["SURVEY_ROUND", "TARGET_PERIOD", "FCT_SOURCE"], var_name="FORECAST", value_name="FORECAST_VALUE")
            # decode FORECAST
            extract_forecast = df["FORECAST"].str.extract(pattern)
            df['FROM'] = extract_forecast.apply(
                lambda row: parse_value(row['from_sign'], row['from_int'], row['from_dec']),
                axis=1
            )
            df['TO'] = extract_forecast.apply(
                lambda row: parse_value(row['to_sign'], row['to_int'], row['to_dec']),
                axis=1
            )
            df['FROM'] = df['FROM'].fillna(-np.inf)
            df['TO'] = df['TO'].fillna(np.inf)
            df["FORECAST_TYPE"] = np.where(df["FORECAST"] == "POINT", "POINT", "DENSITY")
            df["FROM"] = np.where(df["FORECAST"] == "POINT", np.nan, df["FROM"])
            df["TO"] = np.where(df["FORECAST"] == "POINT", np.nan, df["TO"])
            df.dropna(subset=["FORECAST_VALUE"], inplace=True)
            df = df[~((df["FORECAST_VALUE"] == 0) & (df["FORECAST_TYPE"] == "DENSITY"))]
            # order by SURVEY_ROUND and TARGET_PERIOD
            df = df.sort_values(by=["SURVEY_ROUND", "TARGET_PERIOD", "FCT_SOURCE", "FROM", "TO"], ignore_index=True)
            # convert to category 
            char_cols = df.select_dtypes(include=['object']).columns
            df[char_cols] = df[char_cols].apply(lambda x: x.astype('category'))
            # return the tidied dataframe back to dict
            self.data[key] = df
        return self.data

    def save_data(self, filename):
        """
        Save the tidied data to a pickle file.
        """
        if self.data is None:
            self.tidy_survey()
        
        with open(filename, 'wb') as f:
            pickle.dump(self.data, f)
       


if __name__ == "__main__":
    ecb = SurveyData(path = "data/")
    ecb.save_data("ecb_survey_data.pkl")
    print("Data tidied and saved to ecb_survey_data.pkl")

