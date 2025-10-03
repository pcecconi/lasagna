#!/usr/bin/env python3
"""
Unit tests for pipeline orchestrator
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from payments_pipeline.common.pipeline_orchestrator import PipelineOrchestrator
from payments_pipeline.common.base_pipeline import BasePipeline, PipelineResult
from payments_pipeline.common.pipeline_config import PipelineDefinition


class TestPipelineOrchestrator:
    """Test PipelineOrchestrator"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session"""
        return Mock()
    
    @pytest.fixture
    def orchestrator(self, mock_spark):
        """Create PipelineOrchestrator"""
        return PipelineOrchestrator(mock_spark)
    
    @pytest.fixture
    def mock_pipeline_class(self):
        """Mock pipeline class"""
        class MockPipeline(BasePipeline):
            def __init__(self, config=None, spark_session=None, pipeline_name="MockPipeline"):
                super().__init__(config, spark_session, pipeline_name)
            
            def execute(self):
                return self._end_execution(True, "Mock pipeline completed")
        
        return MockPipeline
    
    def test_initialization(self, mock_spark):
        """Test PipelineOrchestrator initialization"""
        orchestrator = PipelineOrchestrator(mock_spark)
        
        assert orchestrator.spark == mock_spark
        assert orchestrator._pipeline_classes == {}
        assert orchestrator._execution_results == {}
        assert orchestrator._execution_order == []
    
    def test_register_pipeline_class_success(self, orchestrator, mock_pipeline_class):
        """Test successful pipeline class registration"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        assert "MockPipeline" in orchestrator._pipeline_classes
        assert orchestrator._pipeline_classes["MockPipeline"] == mock_pipeline_class
    
    def test_register_pipeline_class_invalid(self, orchestrator):
        """Test registering invalid pipeline class"""
        class InvalidClass:
            pass  # Doesn't inherit from BasePipeline
        
        with pytest.raises(ValueError, match="Pipeline class must inherit from BasePipeline"):
            orchestrator.register_pipeline_class("InvalidPipeline", InvalidClass)
    
    def test_load_configuration_with_file(self, orchestrator):
        """Test loading configuration from file"""
        with patch.object(orchestrator.config_manager, 'load_pipeline_config') as mock_load:
            orchestrator.load_configuration(config_file="test.yml")
            
            mock_load.assert_called_once_with("test.yml")
    
    def test_load_configuration_with_environment(self, orchestrator):
        """Test loading configuration with environment"""
        with patch.object(orchestrator.config_manager, 'load_pipeline_config') as mock_load:
            orchestrator.load_configuration(environment="development")
            
            mock_load.assert_called_once_with("", environment="development")
    
    def test_load_configuration_no_parameters(self, orchestrator):
        """Test loading configuration with no parameters"""
        with pytest.raises(ValueError, match="Either config_file or environment must be specified"):
            orchestrator.load_configuration()
    
    def test_execute_single_pipeline_success(self, orchestrator, mock_pipeline_class):
        """Test executing single pipeline successfully"""
        # Setup
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        pipeline_def = PipelineDefinition(
            name="test_pipeline",
            class_name="MockPipeline",
            dependencies=[],
            config={"setting": "value"},
            enabled=True,
            description="Test pipeline"
        )
        
        with patch.object(orchestrator.config_manager, 'get_pipeline_definition', return_value=pipeline_def):
            result = orchestrator.execute_single_pipeline("test_pipeline")
        
        assert result.success is True
        assert "Mock pipeline completed" in result.message
        assert "test_pipeline" in orchestrator._execution_results
    
    def test_execute_single_pipeline_not_found(self, orchestrator):
        """Test executing non-existent pipeline"""
        with patch.object(orchestrator.config_manager, 'get_pipeline_definition', return_value=None):
            with pytest.raises(ValueError, match="Pipeline 'nonexistent' not found"):
                orchestrator.execute_single_pipeline("nonexistent")
    
    def test_execute_single_pipeline_disabled(self, orchestrator, mock_pipeline_class):
        """Test executing disabled pipeline"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        pipeline_def = PipelineDefinition(
            name="disabled_pipeline",
            class_name="MockPipeline",
            dependencies=[],
            config={},
            enabled=False,  # Disabled
            description="Disabled pipeline"
        )
        
        with patch.object(orchestrator.config_manager, 'get_pipeline_definition', return_value=pipeline_def):
            result = orchestrator.execute_single_pipeline("disabled_pipeline")
        
        assert result.success is False
        assert "Pipeline is disabled" in result.message
    
    def test_execute_single_pipeline_with_dependencies(self, orchestrator, mock_pipeline_class):
        """Test executing pipeline with dependencies"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        # Mock dependency execution
        with patch.object(orchestrator, 'execute_pipelines') as mock_execute_deps:
            mock_execute_deps.return_value = {"dep_pipeline": PipelineResult(
                success=True, message="Dep completed", metrics={},
                start_time=datetime.now(), end_time=datetime.now(), duration_seconds=1.0
            )}
            
            # Mock dependency resolution
            with patch.object(orchestrator.config_manager, 'get_pipeline_dependencies', return_value=["dep_pipeline"]):
                pipeline_def = PipelineDefinition(
                    name="test_pipeline",
                    class_name="MockPipeline",
                    dependencies=["dep_pipeline"],
                    config={},
                    enabled=True,
                    description="Test pipeline"
                )
                
                with patch.object(orchestrator.config_manager, 'get_pipeline_definition', return_value=pipeline_def):
                    result = orchestrator.execute_single_pipeline("test_pipeline")
            
            assert result.success is True
            mock_execute_deps.assert_called_once_with(["dep_pipeline"], None)
    
    def test_execute_single_pipeline_dependency_failure(self, orchestrator, mock_pipeline_class):
        """Test executing pipeline with failed dependency"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        # Mock failed dependency execution
        with patch.object(orchestrator, 'execute_pipelines') as mock_execute_deps:
            mock_execute_deps.return_value = {"dep_pipeline": PipelineResult(
                success=False, message="Dep failed", metrics={},
                start_time=datetime.now(), end_time=datetime.now(), duration_seconds=1.0
            )}
            
            # Mock dependency resolution
            with patch.object(orchestrator.config_manager, 'get_pipeline_dependencies', return_value=["dep_pipeline"]):
                pipeline_def = PipelineDefinition(
                    name="test_pipeline",
                    class_name="MockPipeline",
                    dependencies=["dep_pipeline"],
                    config={},
                    enabled=True,
                    description="Test pipeline"
                )
                
                with patch.object(orchestrator.config_manager, 'get_pipeline_definition', return_value=pipeline_def):
                    result = orchestrator.execute_single_pipeline("test_pipeline")
            
            assert result.success is False
            assert "Dependencies failed" in result.message
    
    def test_execute_pipeline_group(self, orchestrator, mock_pipeline_class):
        """Test executing pipeline group"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        # Mock pipeline group
        mock_group = Mock()
        mock_group.enabled = True
        mock_group.pipelines = [
            PipelineDefinition("pipeline1", "MockPipeline", enabled=True),
            PipelineDefinition("pipeline2", "MockPipeline", enabled=True)
        ]
        
        with patch.object(orchestrator.config_manager, 'get_pipeline_group', return_value=mock_group):
            with patch.object(orchestrator.config_manager, 'get_execution_order', return_value=["pipeline1", "pipeline2"]):
                with patch.object(orchestrator, '_execute_pipelines') as mock_execute:
                    mock_execute.return_value = {
                        "pipeline1": PipelineResult(True, "Success", {}, datetime.now(), datetime.now(), 1.0),
                        "pipeline2": PipelineResult(True, "Success", {}, datetime.now(), datetime.now(), 1.0)
                    }
                    
                    results = orchestrator.execute_pipeline_group("test_group")
                    
                    assert len(results) == 2
                    assert "pipeline1" in results
                    assert "pipeline2" in results
                    mock_execute.assert_called_once_with(["pipeline1", "pipeline2"], None)
    
    def test_execute_pipeline_group_disabled(self, orchestrator):
        """Test executing disabled pipeline group"""
        mock_group = Mock()
        mock_group.enabled = False
        
        with patch.object(orchestrator.config_manager, 'get_pipeline_group', return_value=mock_group):
            results = orchestrator.execute_pipeline_group("disabled_group")
            
            assert results == {}
    
    def test_execute_pipeline_group_not_found(self, orchestrator):
        """Test executing non-existent pipeline group"""
        with patch.object(orchestrator.config_manager, 'get_pipeline_group', return_value=None):
            with pytest.raises(ValueError, match="Pipeline group 'nonexistent' not found"):
                orchestrator.execute_pipeline_group("nonexistent")
    
    def test_execute_pipelines(self, orchestrator, mock_pipeline_class):
        """Test executing specific pipelines"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        with patch.object(orchestrator.config_manager, 'get_execution_order', return_value=["pipeline1", "pipeline2"]):
            with patch.object(orchestrator, 'execute_single_pipeline') as mock_execute_single:
                mock_execute_single.side_effect = [
                    PipelineResult(True, "Success 1", {}, datetime.now(), datetime.now(), 1.0),
                    PipelineResult(True, "Success 2", {}, datetime.now(), datetime.now(), 1.0)
                ]
                
                results = orchestrator.execute_pipelines(["pipeline1", "pipeline2"])
                
                assert len(results) == 2
                assert mock_execute_single.call_count == 2
    
    def test_execute_pipelines_with_failure(self, orchestrator, mock_pipeline_class):
        """Test executing pipelines with failure"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        with patch.object(orchestrator.config_manager, 'get_execution_order', return_value=["pipeline1", "pipeline2"]):
            with patch.object(orchestrator, 'execute_single_pipeline') as mock_execute_single:
                mock_execute_single.side_effect = [
                    PipelineResult(True, "Success 1", {}, datetime.now(), datetime.now(), 1.0),
                    PipelineResult(False, "Failure 2", {}, datetime.now(), datetime.now(), 1.0)
                ]
                
                results = orchestrator.execute_pipelines(["pipeline1", "pipeline2"])
                
                assert len(results) == 2
                assert results["pipeline1"].success is True
                assert results["pipeline2"].success is False
    
    def test_get_execution_summary_no_executions(self, orchestrator):
        """Test getting execution summary with no executions"""
        summary = orchestrator.get_execution_summary()
        
        assert "No pipelines executed yet" in summary["message"]
    
    def test_rollback_failed_pipelines(self, orchestrator, mock_pipeline_class):
        """Test rolling back failed pipelines"""
        orchestrator.register_pipeline_class("MockPipeline", mock_pipeline_class)
        
        # Add failed pipeline result
        orchestrator._execution_results = {
            "failed_pipeline": PipelineResult(False, "Failed", {}, datetime.now(), datetime.now(), 1.0)
        }
        
        # Mock pipeline definition
        pipeline_def = PipelineDefinition("failed_pipeline", "MockPipeline")
        
        with patch.object(orchestrator.config_manager, 'get_pipeline_definition', return_value=pipeline_def):
            rollback_results = orchestrator.rollback_failed_pipelines()
            
            assert "failed_pipeline" in rollback_results
            assert rollback_results["failed_pipeline"] is True  # Mock pipeline rollback succeeds
    
    def test_rollback_failed_pipelines_no_failures(self, orchestrator):
        """Test rolling back when no pipelines failed"""
        # Add only successful pipeline result
        orchestrator._execution_results = {
            "successful_pipeline": PipelineResult(True, "Success", {}, datetime.now(), datetime.now(), 1.0)
        }
        
        rollback_results = orchestrator.rollback_failed_pipelines()
        
        assert rollback_results == {}
    
    def test_list_available_pipelines(self, orchestrator):
        """Test listing available pipelines"""
        # Mock pipeline groups
        mock_group1 = Mock()
        mock_group1.description = "Bronze layer"
        mock_group1.enabled = True
        mock_group1.pipelines = [
            PipelineDefinition("pipeline1", "Class1", enabled=True),
            PipelineDefinition("pipeline2", "Class2", enabled=False)
        ]
        
        mock_group2 = Mock()
        mock_group2.description = "Silver layer"
        mock_group2.enabled = False
        mock_group2.pipelines = [
            PipelineDefinition("pipeline3", "Class3", enabled=True)
        ]
        
        orchestrator.config_manager._pipeline_groups = {
            "bronze_layer": mock_group1,
            "silver_layer": mock_group2
        }
        
        available = orchestrator.list_available_pipelines()
        
        assert "bronze_layer" in available
        assert "silver_layer" in available
        
        # Check bronze layer
        bronze_info = available["bronze_layer"]
        assert bronze_info["description"] == "Bronze layer"
        assert bronze_info["enabled"] is True
        assert "pipeline1" in bronze_info["pipelines"]
        assert "pipeline2" in bronze_info["pipelines"]
        
        # Check silver layer
        silver_info = available["silver_layer"]
        assert silver_info["description"] == "Silver layer"
        assert silver_info["enabled"] is False
        assert "pipeline3" in silver_info["pipelines"]


if __name__ == "__main__":
    pytest.main([__file__])
