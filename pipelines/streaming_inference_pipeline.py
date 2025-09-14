import os
import sys
import json
import pandas as pd
import logging
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from model_inference import ModelInference
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from config import get_model_config, get_inference_config

# Get project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
MODEL_PATH = os.path.join(PROJECT_ROOT, 'artifacts', 'models', 'churn_analysis_model.joblib')
ENCODERS_PATH = os.path.join(PROJECT_ROOT, 'artifacts', 'encode')

logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

inference = ModelInference(model_path=MODEL_PATH)
data = {
    "Lastname": "hungry",
    "CreditScore": 390,
    "Geography": "Spain",
    "Gender": "Male",
    "Age": 36,
    "Tenure": 1,
    "Balance": 45000.75,
    "NumOfProducts": 4,
    "HasCrCard": 1,
    "IsActiveMember": 1,
    "EstimatedSalary": 56234.50
    }

def stream_inference( inference, data):
    inference.load_encoders(ENCODERS_PATH)
    inference.load_model(MODEL_PATH)
    
    pred = inference.predict(data)
    return pred

pred = stream_inference(inference, data)
print(pred)
