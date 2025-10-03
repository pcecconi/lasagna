#!/usr/bin/env python3
"""
Pipeline Orchestrator

Provides simple dependency management and orchestration for pipeline execution.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession

from .pipeline_config import PipelineConfigManager, PipelineDefinition
from .base_pipeline import BasePipeline, PipelineResult


class PipelineOrchestrator:
    """
    Simple pipeline orchestrator with dependency management
    
    Provides functionality for:
    - Loading pipeline configurations
    - Dependency resolution
    - Sequential pipeline execution
    - Error handling and rollback
    """
    
    def __init__(self, spark_session: SparkSession, config_manager: Optional[PipelineConfigManager] = None):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        self.config_manager = config_manager or PipelineConfigManager()
        
        # Pipeline registry
        self._pipeline_classes: Dict[str, type] = {}
        
        # Execution state
        self._execution_results: Dict[str, PipelineResult] = {}
        self._execution_order: List[str] = []
    
    def register_pipeline_class(self, pipeline_name: str, pipeline_class: type):
        """Register a pipeline class for execution"""
        if not issubclass(pipeline_class, BasePipeline):
            raise ValueError(f"Pipeline class must inherit from BasePipeline")
        
        self._pipeline_classes[pipeline_name] = pipeline_class
        self.logger.info(f"Registered pipeline class: {pipeline_name}")
    
    def load_configuration(self, config_file: str = None, environment: str = None):
        """
        Load pipeline configuration
        
        Args:
            config_file: Specific config file to load (optional)
            environment: Environment name (development, production, etc.)
        """
        if environment:
            self.config_manager.load_pipeline_config("", environment=environment)
            self.logger.info(f"Loaded pipeline configuration for environment: {environment}")
        elif config_file:
            self.config_manager.load_pipeline_config(config_file)
            self.logger.info(f"Loaded pipeline configuration from {config_file}")
        else:
            raise ValueError("Either config_file or environment must be specified")
    
    def execute_pipeline_group(self, group_name: str, 
                             pipeline_config: Optional[Dict[str, Any]] = None) -> Dict[str, PipelineResult]:
        """
        Execute all pipelines in a group
        
        Args:
            group_name: Name of the pipeline group
            pipeline_config: Optional configuration overrides
            
        Returns:
            Dictionary of pipeline results
        """
        group = self.config_manager.get_pipeline_group(group_name)
        if not group:
            raise ValueError(f"Pipeline group '{group_name}' not found")
        
        if not group.enabled:
            self.logger.info(f"Pipeline group '{group_name}' is disabled, skipping")
            return {}
        
        # Get enabled pipelines in the group
        enabled_pipelines = [p for p in group.pipelines if p.enabled]
        if not enabled_pipelines:
            self.logger.info(f"No enabled pipelines in group '{group_name}'")
            return {}
        
        # Get execution order based on dependencies
        pipeline_names = [p.name for p in enabled_pipelines]
        execution_order = self.config_manager.get_execution_order(pipeline_names)
        
        self.logger.info(f"Executing pipeline group '{group_name}' with {len(execution_order)} pipelines")
        self.logger.info(f"Execution order: {execution_order}")
        
        return self._execute_pipelines(execution_order, pipeline_config)
    
    def execute_pipelines(self, pipeline_names: List[str], 
                         pipeline_config: Optional[Dict[str, Any]] = None) -> Dict[str, PipelineResult]:
        """
        Execute specific pipelines
        
        Args:
            pipeline_names: List of pipeline names to execute
            pipeline_config: Optional configuration overrides
            
        Returns:
            Dictionary of pipeline results
        """
        # Get execution order based on dependencies
        execution_order = self.config_manager.get_execution_order(pipeline_names)
        
        self.logger.info(f"Executing {len(execution_order)} pipelines")
        self.logger.info(f"Execution order: {execution_order}")
        
        return self._execute_pipelines(execution_order, pipeline_config)
    
    def execute_single_pipeline(self, pipeline_name: str, 
                               pipeline_config: Optional[Dict[str, Any]] = None,
                               existing_results: Optional[Dict[str, PipelineResult]] = None) -> PipelineResult:
        """
        Execute a single pipeline
        
        Args:
            pipeline_name: Name of the pipeline to execute
            pipeline_config: Optional configuration overrides
            existing_results: Optional existing results to avoid re-execution
            
        Returns:
            Pipeline result
        """
        pipeline_def = self.config_manager.get_pipeline_definition(pipeline_name)
        if not pipeline_def:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        
        if not pipeline_def.enabled:
            self.logger.info(f"Pipeline '{pipeline_name}' is disabled, skipping")
            return PipelineResult(
                success=False,
                message="Pipeline is disabled",
                metrics={},
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=0
            )
        
        # Check dependencies first
        dependencies = self.config_manager.get_pipeline_dependencies(pipeline_name)
        if dependencies:
            self.logger.info(f"Pipeline '{pipeline_name}' has dependencies: {dependencies}")
            
            # Filter out dependencies that have already been executed
            remaining_deps = [dep for dep in dependencies if not (existing_results and dep in existing_results)]
            
            if remaining_deps:
                self.logger.info(f"Executing {len(remaining_deps)} remaining dependencies: {remaining_deps}")
                # Execute only remaining dependencies
                deps_results = self.execute_pipelines(remaining_deps, pipeline_config)
                
                # Merge with existing results
                if existing_results:
                    deps_results.update(existing_results)
            else:
                self.logger.info(f"All dependencies already executed: {dependencies}")
                deps_results = existing_results or {}
            
            # Check if dependencies succeeded
            failed_deps = [name for name, result in deps_results.items() if not result.success]
            if failed_deps:
                error_msg = f"Dependencies failed: {failed_deps}"
                self.logger.error(error_msg)
                return PipelineResult(
                    success=False,
                    message=error_msg,
                    metrics={},
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration_seconds=0
                )
        
        # Execute the pipeline
        return self._execute_single_pipeline(pipeline_def, pipeline_config)
    
    def _execute_pipelines(self, execution_order: List[str], 
                          pipeline_config: Optional[Dict[str, Any]] = None) -> Dict[str, PipelineResult]:
        """Execute pipelines in the specified order"""
        results = {}
        
        for pipeline_name in execution_order:
            # Skip if already executed in this session
            if pipeline_name in results:
                self.logger.info(f"Skipping pipeline '{pipeline_name}' - already executed")
                continue
                
            self.logger.info(f"Executing pipeline: {pipeline_name}")
            
            try:
                result = self.execute_single_pipeline(pipeline_name, pipeline_config, results)
                results[pipeline_name] = result
                
                if result.success:
                    self.logger.info(f"✅ Pipeline '{pipeline_name}' completed successfully")
                else:
                    self.logger.error(f"❌ Pipeline '{pipeline_name}' failed: {result.message}")
                    # Optionally continue with other pipelines or stop on first failure
                    # For now, we'll continue with other pipelines
                    
            except Exception as e:
                self.logger.error(f"❌ Pipeline '{pipeline_name}' failed with exception: {e}")
                results[pipeline_name] = PipelineResult(
                    success=False,
                    message=f"Pipeline failed with exception: {str(e)}",
                    metrics={},
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration_seconds=0
                )
        
        return results
    
    def _execute_single_pipeline(self, pipeline_def: PipelineDefinition, 
                                pipeline_config: Optional[Dict[str, Any]] = None) -> PipelineResult:
        """Execute a single pipeline"""
        pipeline_name = pipeline_def.name
        
        # Get pipeline class
        pipeline_class = self._pipeline_classes.get(pipeline_def.class_name)
        if not pipeline_class:
            raise ValueError(f"Pipeline class '{pipeline_def.class_name}' not registered")
        
        # Merge configurations
        config = pipeline_def.config.copy()
        if pipeline_config:
            config.update(pipeline_config)
        
        try:
            # Create pipeline instance
            pipeline_instance = pipeline_class(
                config=config,
                spark_session=self.spark,
                pipeline_name=pipeline_name
            )
            
            # Execute pipeline
            self.logger.info(f"🚀 Starting pipeline: {pipeline_name}")
            result = pipeline_instance.execute()
            
            # Store result
            self._execution_results[pipeline_name] = result
            
            return result
            
        except Exception as e:
            error_msg = f"Failed to create or execute pipeline '{pipeline_name}': {str(e)}"
            self.logger.error(error_msg)
            
            result = PipelineResult(
                success=False,
                message=error_msg,
                metrics={},
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=0
            )
            
            self._execution_results[pipeline_name] = result
            return result
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of pipeline executions"""
        if not self._execution_results:
            return {"message": "No pipelines executed yet"}
        
        total_pipelines = len(self._execution_results)
        successful_pipelines = sum(1 for result in self._execution_results.values() if result.success)
        failed_pipelines = total_pipelines - successful_pipelines
        
        total_duration = sum(result.duration_seconds for result in self._execution_results.values())
        
        return {
            "total_pipelines": total_pipelines,
            "successful_pipelines": successful_pipelines,
            "failed_pipelines": failed_pipelines,
            "success_rate": successful_pipelines / total_pipelines if total_pipelines > 0 else 0,
            "total_duration_seconds": total_duration,
            "execution_order": self._execution_order,
            "results": {
                name: {
                    "success": result.success,
                    "message": result.message,
                    "duration_seconds": result.duration_seconds
                }
                for name, result in self._execution_results.items()
            }
        }
    
    def rollback_failed_pipelines(self) -> Dict[str, bool]:
        """Attempt to rollback failed pipelines"""
        rollback_results = {}
        
        for pipeline_name, result in self._execution_results.items():
            if not result.success:
                self.logger.info(f"Attempting rollback for failed pipeline: {pipeline_name}")
                
                try:
                    pipeline_def = self.config_manager.get_pipeline_definition(pipeline_name)
                    if pipeline_def:
                        pipeline_class = self._pipeline_classes.get(pipeline_def.class_name)
                        if pipeline_class:
                            pipeline_instance = pipeline_class(
                                config=pipeline_def.config,
                                spark_session=self.spark,
                                pipeline_name=pipeline_name
                            )
                            rollback_success = pipeline_instance.rollback()
                            rollback_results[pipeline_name] = rollback_success
                        else:
                            rollback_results[pipeline_name] = False
                    else:
                        rollback_results[pipeline_name] = False
                        
                except Exception as e:
                    self.logger.error(f"Rollback failed for pipeline '{pipeline_name}': {e}")
                    rollback_results[pipeline_name] = False
        
        return rollback_results
    
    def list_available_pipelines(self) -> Dict[str, Any]:
        """List all available pipeline definitions"""
        available_pipelines = {}
        
        for group_name, group in self.config_manager._pipeline_groups.items():
            available_pipelines[group_name] = {
                "description": group.description,
                "enabled": group.enabled,
                "pipelines": {
                    pipeline.name: {
                        "class_name": pipeline.class_name,
                        "description": pipeline.description,
                        "enabled": pipeline.enabled,
                        "dependencies": pipeline.dependencies
                    }
                    for pipeline in group.pipelines
                }
            }
        
        return available_pipelines
