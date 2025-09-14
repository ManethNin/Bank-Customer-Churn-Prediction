import os
import sys
import logging
import pandas as pd
import joblib
from typing import Dict, Any, Tuple, Optional
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_pipeline import data_pipeline
from model_building import XGboostModelBuilder, RandomForestModelBuilder
from model_training import ModelTrainer
from model_evaluation import ModelEvaluator
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from mlflow_utils import MLflowTracker, setup_mlflow_autolog, create_mlflow_run_tags
import mlflow
from config import get_model_config, get_data_paths
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def training_pipeline(  data_path: str='data/raw/ChurnModelling.csv', model_params: Optional[Dict[str, Any]] = None,test_size: float=0.2, random_state : int=42,model_path: str = 'artifacts/models/churn_analysis_model.joblib'):

    if (not os.path.exists(get_data_paths()['data_artifacts_dir'] +'/X_train.csv')) or \
        (not os.path.exists(get_data_paths()['data_artifacts_dir']+ '/Y_train.csv')) or \
        (not os.path.exists(get_data_paths()['data_artifacts_dir'] + '/X_test.csv')) or \
        (not os.path.exists(get_data_paths()['data_artifacts_dir'] + '/Y_test.csv')):

        data_pipeline()

    else:
        print("Loading Data..")

    mlflow_tracker = MLflowTracker()
    run_tags = create_mlflow_run_tags('training_pipeline', {
        'model_type': 'XGboost',
        'training_strategy' : 'simple',
        'other_models': 'random_forest'
    })
    run = mlflow_tracker.start_run(run_name='training_pipeline', tags=run_tags)

    X_train = pd.read_csv(get_data_paths()['data_artifacts_dir'] +'/X_train.csv')
    Y_train = pd.read_csv(get_data_paths()['data_artifacts_dir'] +'/Y_train.csv')
    X_test = pd.read_csv(get_data_paths()['data_artifacts_dir'] +'/X_test.csv')
    Y_test = pd.read_csv(get_data_paths()['data_artifacts_dir'] +'/Y_test.csv')


    model_builder = XGboostModelBuilder()
    model = model_builder.build_model()

    trainer = ModelTrainer()
    model, score = trainer.train(model=model, X_train= X_train,y_train= Y_train.squeeze())
    # print(score)

    trainer.save_model(model , model_path)

    evaluator = ModelEvaluator(model, 'XGboost')

    results = evaluator.evaluate(X_test=X_test, Y_test= Y_test)

    model_params = get_model_config()['model_params']

    mlflow_tracker.log_training_metrics(model, results, model_params)

    mlflow_tracker.end_run()


if __name__ == "__main__":
    training_pipeline()