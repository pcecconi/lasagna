#!/usr/bin/env python3
"""
Pipeline Configuration Management

Provides YAML-based configuration for pipeline definitions and orchestration.
"""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
import yaml


@dataclass
class PipelineDefinition:
    """Definition of a single pipeline"""
    name: str
    class_name: str
    dependencies: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    description: str = ""


@dataclass
class PipelineGroup:
    """Group of related pipelines"""
    name: str
    pipelines: List[PipelineDefinition] = field(default_factory=list)
    description: str = ""
    enabled: bool = True


class PipelineConfigManager:
    """
    Manages pipeline configurations from YAML files
    
    Provides functionality for:
    - Loading pipeline definitions from YAML
    - Validating pipeline configurations
    - Dependency resolution
    - Configuration inheritance
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        self.config_dir = Path(config_dir) if config_dir else Path("pipeline_configs")
        self.config_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # Cache for loaded configurations
        self._config_cache: Dict[str, Any] = {}
        self._pipeline_definitions: Dict[str, PipelineDefinition] = {}
        self._pipeline_groups: Dict[str, PipelineGroup] = {}
    
    def load_pipeline_config(self, config_file: str, environment: Optional[str] = None) -> Dict[str, Any]:
        """
        Load pipeline configuration from YAML file
        
        Args:
            config_file: Path to configuration file
            environment: Optional environment override (development, production, etc.)
            
        Returns:
            Configuration dictionary
        """
        # Handle environment-specific configs
        if environment:
            config_path = self.config_dir / f"{environment}.yml"
        else:
            config_path = self.config_dir / config_file if not Path(config_file).is_absolute() else Path(config_file)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Check cache first
        cache_key = f"{str(config_path)}_{environment or 'default'}"
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Validate configuration
            self._validate_config(config)
            
            # Cache the configuration
            self._config_cache[cache_key] = config
            
            # Parse pipeline definitions
            self._parse_pipeline_definitions(config)
            
            env_info = f" (environment: {environment})" if environment else ""
            self.logger.info(f"Loaded pipeline configuration from {config_path}{env_info}")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration from {config_path}: {e}")
            raise
    
    def get_pipeline_definition(self, pipeline_name: str) -> Optional[PipelineDefinition]:
        """Get pipeline definition by name"""
        return self._pipeline_definitions.get(pipeline_name)
    
    def get_pipeline_group(self, group_name: str) -> Optional[PipelineGroup]:
        """Get pipeline group by name"""
        return self._pipeline_groups.get(group_name)
    
    def get_all_pipeline_definitions(self) -> Dict[str, PipelineDefinition]:
        """Get all pipeline definitions"""
        return self._pipeline_definitions.copy()
    
    def get_pipeline_dependencies(self, pipeline_name: str) -> List[str]:
        """
        Get dependencies for a pipeline (recursive)
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            List of dependency pipeline names in execution order
        """
        if pipeline_name not in self._pipeline_definitions:
            return []
        
        dependencies = []
        visited = set()
        
        def _resolve_deps(pipeline_name: str):
            if pipeline_name in visited:
                return  # Avoid circular dependencies
            
            visited.add(pipeline_name)
            pipeline_def = self._pipeline_definitions.get(pipeline_name)
            
            if pipeline_def:
                for dep in pipeline_def.dependencies:
                    _resolve_deps(dep)
                    if dep not in dependencies:
                        dependencies.append(dep)
        
        _resolve_deps(pipeline_name)
        return dependencies
    
    def get_execution_order(self, pipeline_names: List[str]) -> List[str]:
        """
        Get execution order for a list of pipelines based on dependencies
        
        Args:
            pipeline_names: List of pipeline names
            
        Returns:
            List of pipeline names in execution order
        """
        execution_order = []
        visited = set()
        
        def _add_pipeline(pipeline_name: str):
            if pipeline_name in visited or pipeline_name not in pipeline_names:
                return
            
            visited.add(pipeline_name)
            pipeline_def = self._pipeline_definitions.get(pipeline_name)
            
            if pipeline_def:
                # Add dependencies first
                for dep in pipeline_def.dependencies:
                    if dep in pipeline_names:
                        _add_pipeline(dep)
                
                # Add this pipeline
                execution_order.append(pipeline_name)
        
        for pipeline_name in pipeline_names:
            _add_pipeline(pipeline_name)
        
        return execution_order
    
    def create_default_config(self) -> Dict[str, Any]:
        """Create a default pipeline configuration"""
        return {
            "version": "1.0",
            "description": "Payments Pipeline Configuration",
            "pipeline_groups": {
                "bronze_layer": {
                    "description": "Bronze layer ingestion pipelines",
                    "enabled": True,
                    "pipelines": {
                        "bronze_merchants": {
                            "class_name": "BronzeMerchantsPipeline",
                            "config": {
                                "table_name": "merchants_raw",
                                "namespace": "payments_bronze",
                                "quality_checks": ["required_columns", "null_values", "duplicates"]
                            },
                            "enabled": True,
                            "description": "Ingest raw merchant data"
                        },
                        "bronze_transactions": {
                            "class_name": "BronzeTransactionsPipeline",
                            "config": {
                                "table_name": "transactions_raw",
                                "namespace": "payments_bronze",
                                "quality_checks": ["required_columns", "null_values", "duplicates", "amount_validation"]
                            },
                            "enabled": True,
                            "description": "Ingest raw transaction data"
                        }
                    }
                },
                "silver_layer": {
                    "description": "Silver layer transformation pipelines",
                    "enabled": True,
                    "pipelines": {
                        "silver_merchants": {
                            "class_name": "SilverMerchantsPipeline",
                            "dependencies": ["bronze_merchants"],
                            "config": {
                                "table_name": "dim_merchants",
                                "namespace": "payments_silver",
                                "scd_type": 2,
                                "business_key": "merchant_id",
                                "quality_checks": ["required_columns", "null_values", "scd_validation"]
                            },
                            "enabled": True,
                            "description": "Transform merchants data with SCD Type 2"
                        },
                        "silver_transactions": {
                            "class_name": "SilverTransactionsPipeline",
                            "dependencies": ["bronze_transactions", "silver_merchants"],
                            "config": {
                                "table_name": "fact_payments",
                                "namespace": "payments_silver",
                                "quality_checks": ["required_columns", "null_values", "amount_validation", "merchant_lookup"]
                            },
                            "enabled": True,
                            "description": "Transform transaction data into fact table"
                        }
                    }
                },
                "gold_layer": {
                    "description": "Gold layer analytics pipelines",
                    "enabled": False,  # Disabled by default
                    "pipelines": {
                        "gold_merchant_analytics": {
                            "class_name": "GoldMerchantAnalyticsPipeline",
                            "dependencies": ["silver_merchants", "silver_transactions"],
                            "config": {
                                "table_name": "merchant_analytics",
                                "namespace": "payments_gold"
                            },
                            "enabled": False,
                            "description": "Create merchant analytics aggregations"
                        }
                    }
                }
            },
            "global_config": {
                "spark_config": {
                    "app_name": "PaymentsPipeline",
                    "master": "spark://spark-master:7077"
                },
                "iceberg_config": {
                    "catalog": "iceberg",
                    "uri": "thrift://hive-metastore:9083",
                    "warehouse_dir": "s3a://warehouse/"
                },
                "quality_config": {
                    "enabled": True,
                    "fail_on_error": True,
                    "generate_reports": True
                }
            }
        }
    
    def save_config(self, config: Dict[str, Any], config_file: str):
        """Save configuration to YAML file"""
        config_path = self.config_dir / config_file if not Path(config_file).is_absolute() else Path(config_file)
        
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)
        
        self.logger.info(f"Saved configuration to {config_path}")
    
    def _validate_config(self, config: Dict[str, Any]):
        """Validate pipeline configuration structure"""
        required_sections = ["version", "pipeline_groups"]
        
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate pipeline groups
        pipeline_groups = config.get("pipeline_groups", {})
        for group_name, group_config in pipeline_groups.items():
            if "pipelines" not in group_config:
                raise ValueError(f"Pipeline group '{group_name}' missing 'pipelines' section")
            
            # Validate individual pipelines
            for pipeline_name, pipeline_config in group_config["pipelines"].items():
                self._validate_pipeline_config(pipeline_name, pipeline_config)
    
    def _validate_pipeline_config(self, pipeline_name: str, pipeline_config: Dict[str, Any]):
        """Validate individual pipeline configuration"""
        required_fields = ["class_name"]
        
        for field in required_fields:
            if field not in pipeline_config:
                raise ValueError(f"Pipeline '{pipeline_name}' missing required field: {field}")
        
        # Validate dependencies exist
        dependencies = pipeline_config.get("dependencies", [])
        for dep in dependencies:
            if not self._dependency_exists(dep):
                self.logger.warning(f"Pipeline '{pipeline_name}' has dependency '{dep}' that may not exist")
    
    def _dependency_exists(self, dependency_name: str) -> bool:
        """Check if a dependency pipeline exists"""
        for group in self._pipeline_groups.values():
            if dependency_name in [p.name for p in group.pipelines]:
                return True
        return False
    
    def _parse_pipeline_definitions(self, config: Dict[str, Any]):
        """Parse pipeline definitions from configuration"""
        pipeline_groups = config.get("pipeline_groups", {})
        
        for group_name, group_config in pipeline_groups.items():
            pipelines = []
            
            for pipeline_name, pipeline_config in group_config.get("pipelines", {}).items():
                pipeline_def = PipelineDefinition(
                    name=pipeline_name,
                    class_name=pipeline_config.get("class_name"),
                    dependencies=pipeline_config.get("dependencies", []),
                    config=pipeline_config.get("config", {}),
                    enabled=pipeline_config.get("enabled", True),
                    description=pipeline_config.get("description", "")
                )
                
                pipelines.append(pipeline_def)
                self._pipeline_definitions[pipeline_name] = pipeline_def
            
            pipeline_group = PipelineGroup(
                name=group_name,
                pipelines=pipelines,
                description=group_config.get("description", ""),
                enabled=group_config.get("enabled", True)
            )
            
            self._pipeline_groups[group_name] = pipeline_group
    
    def get_enabled_pipelines(self, group_name: Optional[str] = None) -> List[PipelineDefinition]:
        """Get all enabled pipeline definitions"""
        enabled_pipelines = []
        
        if group_name:
            group = self._pipeline_groups.get(group_name)
            if group and group.enabled:
                enabled_pipelines = [p for p in group.pipelines if p.enabled]
        else:
            for group in self._pipeline_groups.values():
                if group.enabled:
                    enabled_pipelines.extend([p for p in group.pipelines if p.enabled])
        
        return enabled_pipelines
