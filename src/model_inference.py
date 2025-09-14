import json
import logging
import joblib
import os
import pickle
from typing import Any, Dict, List, Optional, Tuple, Union
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from config import get_binning_config , get_encoding_config
import numpy as np
import pandas as pd
from feature_binning import CustomBinningStratergy
from feature_encoding import OrdinalEncodingStratergy
from sklearn.base import BaseEstimator
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ModelInference:
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None
        self.encoders ={}

    def load_model(self, model_path):
        if not os.path.exists(model_path):
            raise ValueError("Can't find the model")
        
        self.model = joblib.load(model_path)

    def load_encoders(self, encoders_dir):
        for file in os.listdir(encoders_dir):
            feature_name = file.split('_encoder.json')[0]
            with open(os.path.join(encoders_dir, file), 'r') as f:
                self.encoders[feature_name] = json.load(f)

    def preprocess_input(self,data):
        data = pd.DataFrame([data])

        data = data.drop(columns=["CustomerId", "RowNumber", "Lastname", "Firstname"], errors="ignore")

        binning = CustomBinningStratergy(get_binning_config()['credit_score_bins'])

        data = binning.bin_feature(data,'CreditScore')

        ordinal_encoder = OrdinalEncodingStratergy(get_encoding_config()['ordinal_mappings'])

        data = ordinal_encoder.encode(data)

        for col, encoder in self.encoders.items():
            data[col] = data[col].map(encoder)

        return data
    
    def predict(self,data):
        # Lazy loading: load model if not already loaded
        if self.model is None:
            self.load_model(self.model_path)
            
        pp_data = self.preprocess_input(data)

        # print(pp_data)
        Y_pred = self.model.predict(pp_data)
        Y_prob = self.model.predict_proba(pp_data)[:,1]

        # Return structured result like teacher's code
        result = {
            'Status': 'Churn' if Y_pred[0] == 1 else 'No Churn',
            'Confidence': f"{Y_prob[0] * 100:.1f}%"
        }
        
        return result

        # print(Y_pred, Y_prob)

# data = {
#     "Lastname": "hungry",
#     "CreditScore": 390,
#     "Geography": "Spain",
#     "Gender": "Male",
#     "Age": 36,
#     "Tenure": 1,
#     "Balance": 45000.75,
#     "NumOfProducts": 4,
#     "HasCrCard": 1,
#     "IsActiveMember": 1,
#     "EstimatedSalary": 56234.50
#     }

# inf = ModelInference(model_path='../artifacts/models')

# model = inf.load_model('../artifacts/models/churn_analysis_model.joblib')
# inf.load_encoders('../artifacts/encode')
# inf.predict(data)
# # print(model)

