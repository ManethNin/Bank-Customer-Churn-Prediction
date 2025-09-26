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



