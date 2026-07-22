"""Configuration Utilities for Spark Jobs."""

import yaml
import os
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURATION LOADING METHODS
# =========================================================================

def get_config(config_path: str) -> Dict[str, Any]:
    """
    Private method to return a config file.

    Args:
        config_path: Path to the configuration file.

    Returns:
        Dictionary containing configuration data.

    Raises:
        FileNotFoundError: If the config file is not found.
    """
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return config

    except FileNotFoundError:
        logger.error(f"Error: file '{config_path}' not found.")
        return None


def get_config(env: str, path: str) -> Dict[str, Any]:
    """
    Read a configuration YAML file and return its contents as a dict.

    This function reads a configuration YAML file from the specified
    environment and path, then returns its contents as a dictionary.

    Args:
        env: Environment name (e.g., 'dev', 'prod', 'staging').
        path: Path to the configuration file within the environment.

    Returns:
        Dictionary containing the configuration data.

    Raises:
        FileNotFoundError: If the config file is not found.
    """
    return _get_config(f"{path}/environments/{env}/values.yaml")


def get_config_yaml(path: str) -> Dict[str, Any]:
    """
    Read a configuration YAML file based on input path and return its contents as a dict.

    This function reads a configuration YAML file from the specified path
    and returns its contents as a dictionary.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Dictionary containing the configuration data.

    Raises:
        FileNotFoundError: If the config file is not found.
    """
    return _get_config(f"{path}")


def get_config_data_loader(path: str, yaml_file: str) -> Dict[str, Any]:
    """
    Read a configuration YAML file and return its contents as a dict.

    This function reads a configuration YAML file from the specified
    path and filename, then returns its contents as a dictionary.

    Args:
        path: Directory path containing the YAML file.
        yaml_file: Name of the YAML file to load.

    Returns:
        Dictionary containing the configuration data.

    Raises:
        FileNotFoundError: If the config file is not found.
    """
    return _get_config(f"{path}/{yaml_file}")


# =========================================================================
# HELPER FUNCTIONS
# =========================================================================

def _get_config(config_path: str) -> Dict[str, Any]:
    """
    Internal helper to load and return a YAML configuration file.

    Args:
        config_path: Full path to the configuration file.

    Returns:
        Dictionary containing the configuration data.

    Raises:
        FileNotFoundError: If the config file is not found.
        yaml.YAMLError: If there's an error parsing the YAML file.
    """
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        logger.info(f"Successfully loaded configuration from {config_path}")
        return config

    except FileNotFoundError as e:
        logger.error(f"Error: {str(e)}")
        raise

    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file '{config_path}': {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error loading configuration: {str(e)}")
        raise


def load_config_section(config: Dict[str, Any], section: str, default: Any = None) -> Any:
    """
    Load a specific section from a configuration dictionary.

    Args:
        config: Configuration dictionary.
        section: Section name (supports dot notation for nested keys, e.g., 'database.host').
        default: Default value if section not found.

    Returns:
        Configuration section value or default.
    """
    try:
        keys = section.split('.')
        value = config

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default

        return value if value is not None else default

    except Exception as e:
        logger.warning(f"Error loading config section '{section}': {str(e)}")
        return default


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two configuration dictionaries, with override taking precedence.

    Args:
        base_config: Base configuration dictionary.
        override_config: Configuration dictionary to override base values.

    Returns:
        Merged configuration dictionary.
    """
    merged = base_config.copy()

    for key, value in override_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value

    return merged


def validate_config(config: Dict[str, Any], required_keys: list) -> bool:
    """
    Validate that all required keys are present in the configuration.

    Args:
        config: Configuration dictionary to validate.
        required_keys: List of required keys.

    Returns:
        True if all required keys are present, False otherwise.
    """
    missing_keys = [key for key in required_keys if key not in config]

    if missing_keys:
        logger.error(f"Configuration missing required keys: {missing_keys}")
        return False

    return True
