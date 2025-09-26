"""
Professional Airflow Task Wrappers


This module provides clean, testable, and maintainable task functions
for Airflow DAGs, following best practices for production environments.
"""


import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_input_data(data_path: str = 'data/raw/ChurnModelling.csv') -> Dict[str, Any]:
    """
    Lightweight validation that input data exists.
    
    Args:
        data_path: Path to input data file
        
    Returns:
        Dict with validation results
    """
    project_root = setup_project_environment()
    full_path = Path(project_root) / data_path
    
    logger.info(f"Validating input data at: {full_path}")
    
    if not full_path.exists():
        logger.warning(f"Input data file not found: {full_path}")
        return {
            'status': 'warning',
            'message': 'Input data file not found',
            'file_path': str(full_path)
        }
    
    # Check file size
    file_size = full_path.stat().st_size
    if file_size == 0:
        logger.warning(f"Input data file is empty: {full_path}")
        return {
            'status': 'warning',
            'message': 'Input data file is empty',
            'file_path': str(full_path)
        }
    
    logger.info(f"✅ Input data validation passed: {file_size} bytes")
    
    return {
        'status': 'success',
        'file_path': str(full_path),
        'file_size_bytes': file_size,
        'message': 'Input data file exists and has content'
    }


def validate_processed_data(data_path: str = 'data/processed/imputed.csv') -> Dict[str, Any]:
    """
    Lightweight validation that processed data exists.
    
    Args:
        data_path: Path to processed data file
        
    Returns:
        Dict with validation results
    """
    project_root = setup_project_environment()
    full_path = Path(project_root) / data_path
    
    logger.info(f"Validating processed data at: {full_path}")
    
    if not full_path.exists():
        logger.warning(f"Processed data file not found: {full_path}")
        return {
            'status': 'warning',
            'message': 'Processed data file not found. Run data pipeline first.',
            'file_path': str(full_path)
        }
    
    file_size = full_path.stat().st_size
    if file_size == 0:
        logger.warning(f"Processed data file is empty: {full_path}")
        return {
            'status': 'warning',
            'message': 'Processed data file is empty',
            'file_path': str(full_path)
        }
    
    logger.info(f"✅ Processed data validation passed: {file_size} bytes")
    
    return {
        'status': 'success',
        'file_path': str(full_path),
        'file_size_bytes': file_size,
        'message': 'Processed data file exists and has content'
    }


def validate_trained_model(model_path: str = 'artifacts/models') -> Dict[str, Any]:
    """
    Lightweight validation that trained model exists.
    
    Args:
        model_path: Path to model artifacts directory
        
    Returns:
        Dict with validation results
    """
    project_root = setup_project_environment()
    model_dir = Path(project_root) / model_path
    
    logger.info(f"Validating trained model at: {model_dir}")
    
    if not model_dir.exists():
        logger.warning(f"Model directory not found: {model_dir}")
        return {
            'status': 'warning',
            'message': 'Model directory not found. Run training pipeline first.',
            'model_directory': str(model_dir)
        }
    
    # Check for any model files
    model_files = list(model_dir.glob('**/*'))
    
    if not model_files:
        logger.warning(f"No model files found in: {model_dir}")
        return {
            'status': 'warning',
            'message': 'No model files found. Run training pipeline first.',
            'model_directory': str(model_dir)
        }
    
    logger.info(f"✅ Model validation passed: {len(model_files)} file(s) found")
    
    return {
        'status': 'success',
        'model_directory': str(model_dir),
        'model_files_count': len(model_files),
        'message': 'Model files found'
    }


def trigger_training_if_needed(**context) -> Dict[str, Any]:
    """
    Check if model exists, and trigger training DAG if not.
    
    Returns:
        Dict with action taken
    """
    try:
        # Try to validate model
        result = validate_trained_model()
        logger.info("✅ Model exists and is valid")
        return {
            'status': 'model_exists',
            'action': 'none',
            'message': 'Model is ready for inference'
        }
    except FileNotFoundError as e:
        logger.warning(f"⚠️ Model not found: {e}")
        
        # Trigger training DAG
        from airflow.models import DagBag
        from airflow.api.client.local_client import Client
        
        try:
            client = Client(None, None)
            client.trigger_dag('training_pipeline_dag')
            logger.info("🚀 Triggered training_pipeline_dag")
            
            return {
                'status': 'model_missing',
                'action': 'triggered_training',
                'message': 'Training DAG triggered due to missing model'
            }
        except Exception as trigger_error:
            logger.error(f"❌ Failed to trigger training DAG: {trigger_error}")
            raise RuntimeError(f"Model missing and failed to trigger training: {trigger_error}")


