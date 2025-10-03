#!/usr/bin/env python3
"""
Unit tests for pipeline configuration management
"""

import pytest
from unittest.mock import Mock, patch, mock_open
from pathlib import Path
import yaml

from payments_pipeline.common.pipeline_config import (
    PipelineConfigManager,
    PipelineDefinition,
    PipelineGroup
)


class TestPipelineDefinition:
    """Test PipelineDefinition dataclass"""
    
    def test_pipeline_definition_creation(self):
        """Test PipelineDefinition creation"""
        definition = PipelineDefinition(
            name="test_pipeline",
            class_name="TestPipeline",
            dependencies=["dep1", "dep2"],
            config={"setting1": "value1"},
            enabled=True,
            description="Test description"
        )
        
        assert definition.name == "test_pipeline"
        assert definition.class_name == "TestPipeline"
        assert definition.dependencies == ["dep1", "dep2"]
        assert definition.config == {"setting1": "value1"}
        assert definition.enabled is True
        assert definition.description == "Test description"


class TestPipelineGroup:
    """Test PipelineGroup dataclass"""
    
    def test_pipeline_group_creation(self):
        """Test PipelineGroup creation"""
        pipeline1 = PipelineDefinition("pipeline1", "Class1")
        pipeline2 = PipelineDefinition("pipeline2", "Class2")
        
        group = PipelineGroup(
            name="test_group",
            pipelines=[pipeline1, pipeline2],
            description="Test group",
            enabled=True
        )
        
        assert group.name == "test_group"
        assert len(group.pipelines) == 2
        assert group.pipelines[0].name == "pipeline1"
        assert group.pipelines[1].name == "pipeline2"
        assert group.description == "Test group"
        assert group.enabled is True


class TestPipelineConfigManager:
    """Test PipelineConfigManager"""
    
    @pytest.fixture
    def temp_config_dir(self, tmp_path):
        """Create temporary config directory"""
        return tmp_path / "configs"
    
    @pytest.fixture
    def config_manager(self, temp_config_dir):
        """Create PipelineConfigManager with temp directory"""
        return PipelineConfigManager(str(temp_config_dir))
    
    def test_initialization(self, temp_config_dir):
        """Test PipelineConfigManager initialization"""
        manager = PipelineConfigManager(str(temp_config_dir))
        
        assert manager.config_dir == temp_config_dir
        assert manager._config_cache == {}
        assert manager._pipeline_definitions == {}
        assert manager._pipeline_groups == {}
    
    def test_initialization_default_directory(self):
        """Test initialization with default directory"""
        with patch('payments_pipeline.common.pipeline_config.Path') as mock_path:
            mock_dir = Mock()
            mock_dir.mkdir = Mock()
            mock_path.return_value = mock_dir
            
            manager = PipelineConfigManager()
            
            assert manager.config_dir == mock_dir
            mock_dir.mkdir.assert_called_once_with(exist_ok=True)
    
    def test_load_pipeline_config_success(self, config_manager, temp_config_dir):
        """Test successful configuration loading"""
        # Create test config file
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "test_pipeline": {
                            "class_name": "TestPipeline",
                            "config": {"setting": "value"},
                            "enabled": True
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test_config.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        result = config_manager.load_pipeline_config("test_config.yml")
        
        assert result == test_config
        assert "test_pipeline" in config_manager._pipeline_definitions
        assert "bronze_layer" in config_manager._pipeline_groups
    
    def test_load_pipeline_config_environment_specific(self, config_manager, temp_config_dir):
        """Test loading environment-specific configuration"""
        # Create environment-specific config
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "test_pipeline": {
                            "class_name": "TestPipeline",
                            "config": {"env": "development"}
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "development.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        result = config_manager.load_pipeline_config("", environment="development")
        
        assert result == test_config
        assert "test_pipeline" in config_manager._pipeline_definitions
    
    def test_load_pipeline_config_file_not_found(self, config_manager):
        """Test loading non-existent configuration file"""
        with pytest.raises(FileNotFoundError):
            config_manager.load_pipeline_config("nonexistent.yml")
    
    def test_load_pipeline_config_invalid_yaml(self, config_manager, temp_config_dir):
        """Test loading invalid YAML configuration"""
        config_file = temp_config_dir / "invalid.yml"
        with open(config_file, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        with pytest.raises(yaml.YAMLError):
            config_manager.load_pipeline_config("invalid.yml")
    
    def test_load_pipeline_config_invalid_structure(self, config_manager, temp_config_dir):
        """Test loading configuration with invalid structure"""
        invalid_config = {
            "version": "1.0"
            # Missing required pipeline_groups
        }
        
        config_file = temp_config_dir / "invalid.yml"
        with open(config_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        with pytest.raises(ValueError, match="Missing required configuration section"):
            config_manager.load_pipeline_config("invalid.yml")
    
    def test_config_caching(self, config_manager, temp_config_dir):
        """Test configuration caching"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "test_pipeline": {
                            "class_name": "TestPipeline"
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        # Load first time
        result1 = config_manager.load_pipeline_config("test.yml")
        
        # Load second time - should use cache
        result2 = config_manager.load_pipeline_config("test.yml")
        
        assert result1 == result2
        assert len(config_manager._config_cache) == 1
    
    def test_get_pipeline_definition(self, config_manager, temp_config_dir):
        """Test getting pipeline definition by name"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "test_pipeline": {
                            "class_name": "TestPipeline",
                            "dependencies": ["dep1"],
                            "config": {"setting": "value"}
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        definition = config_manager.get_pipeline_definition("test_pipeline")
        
        assert definition is not None
        assert definition.name == "test_pipeline"
        assert definition.class_name == "TestPipeline"
        assert definition.dependencies == ["dep1"]
        assert definition.config == {"setting": "value"}
    
    def test_get_pipeline_definition_not_found(self, config_manager):
        """Test getting non-existent pipeline definition"""
        definition = config_manager.get_pipeline_definition("nonexistent")
        assert definition is None
    
    def test_get_pipeline_group(self, config_manager, temp_config_dir):
        """Test getting pipeline group by name"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "description": "Bronze layer",
                    "pipelines": {
                        "test_pipeline": {
                            "class_name": "TestPipeline"
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        group = config_manager.get_pipeline_group("bronze_layer")
        
        assert group is not None
        assert group.name == "bronze_layer"
        assert group.description == "Bronze layer"
        assert len(group.pipelines) == 1
        assert group.pipelines[0].name == "test_pipeline"
    
    def test_get_pipeline_group_not_found(self, config_manager):
        """Test getting non-existent pipeline group"""
        group = config_manager.get_pipeline_group("nonexistent")
        assert group is None
    
    def test_get_pipeline_dependencies(self, config_manager, temp_config_dir):
        """Test getting pipeline dependencies"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "bronze_pipeline": {
                            "class_name": "BronzePipeline"
                        }
                    }
                },
                "silver_layer": {
                    "pipelines": {
                        "silver_pipeline": {
                            "class_name": "SilverPipeline",
                            "dependencies": ["bronze_pipeline"]
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        dependencies = config_manager.get_pipeline_dependencies("silver_pipeline")
        
        assert dependencies == ["bronze_pipeline"]
    
    def test_get_pipeline_dependencies_nested(self, config_manager, temp_config_dir):
        """Test getting nested pipeline dependencies"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "bronze_pipeline": {
                            "class_name": "BronzePipeline"
                        }
                    }
                },
                "silver_layer": {
                    "pipelines": {
                        "silver_pipeline": {
                            "class_name": "SilverPipeline",
                            "dependencies": ["bronze_pipeline"]
                        }
                    }
                },
                "gold_layer": {
                    "pipelines": {
                        "gold_pipeline": {
                            "class_name": "GoldPipeline",
                            "dependencies": ["silver_pipeline"]
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        dependencies = config_manager.get_pipeline_dependencies("gold_pipeline")
        
        assert dependencies == ["bronze_pipeline", "silver_pipeline"]
    
    def test_get_execution_order(self, config_manager, temp_config_dir):
        """Test getting execution order for pipelines"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "pipelines": {
                        "bronze_pipeline": {
                            "class_name": "BronzePipeline"
                        }
                    }
                },
                "silver_layer": {
                    "pipelines": {
                        "silver_pipeline": {
                            "class_name": "SilverPipeline",
                            "dependencies": ["bronze_pipeline"]
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        execution_order = config_manager.get_execution_order(["silver_pipeline", "bronze_pipeline"])
        
        assert execution_order == ["bronze_pipeline", "silver_pipeline"]
    
    def test_get_execution_order_circular_dependency(self, config_manager, temp_config_dir):
        """Test execution order with circular dependency"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "test_layer": {
                    "pipelines": {
                        "pipeline_a": {
                            "class_name": "PipelineA",
                            "dependencies": ["pipeline_b"]
                        },
                        "pipeline_b": {
                            "class_name": "PipelineB",
                            "dependencies": ["pipeline_a"]
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        execution_order = config_manager.get_execution_order(["pipeline_a", "pipeline_b"])
        
        # Should handle circular dependency gracefully
        assert len(execution_order) == 2
        assert "pipeline_a" in execution_order
        assert "pipeline_b" in execution_order
    
    def test_get_enabled_pipelines_all_groups(self, config_manager, temp_config_dir):
        """Test getting all enabled pipelines"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "enabled": True,
                    "pipelines": {
                        "enabled_pipeline": {
                            "class_name": "EnabledPipeline",
                            "enabled": True
                        },
                        "disabled_pipeline": {
                            "class_name": "DisabledPipeline",
                            "enabled": False
                        }
                    }
                },
                "silver_layer": {
                    "enabled": False,
                    "pipelines": {
                        "silver_pipeline": {
                            "class_name": "SilverPipeline",
                            "enabled": True
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        enabled_pipelines = config_manager.get_enabled_pipelines()
        
        assert len(enabled_pipelines) == 1
        assert enabled_pipelines[0].name == "enabled_pipeline"
    
    def test_get_enabled_pipelines_specific_group(self, config_manager, temp_config_dir):
        """Test getting enabled pipelines from specific group"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "bronze_layer": {
                    "enabled": True,
                    "pipelines": {
                        "enabled_pipeline": {
                            "class_name": "EnabledPipeline",
                            "enabled": True
                        },
                        "disabled_pipeline": {
                            "class_name": "DisabledPipeline",
                            "enabled": False
                        }
                    }
                }
            }
        }
        
        config_file = temp_config_dir / "test.yml"
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        config_manager.load_pipeline_config("test.yml")
        
        enabled_pipelines = config_manager.get_enabled_pipelines("bronze_layer")
        
        assert len(enabled_pipelines) == 1
        assert enabled_pipelines[0].name == "enabled_pipeline"
    
    def test_create_default_config(self, config_manager):
        """Test creating default configuration"""
        default_config = config_manager.create_default_config()
        
        assert "version" in default_config
        assert "pipeline_groups" in default_config
        assert "bronze_layer" in default_config["pipeline_groups"]
        assert "silver_layer" in default_config["pipeline_groups"]
        assert "gold_layer" in default_config["pipeline_groups"]
        
        # Check specific pipeline definitions
        bronze_merchants = default_config["pipeline_groups"]["bronze_layer"]["pipelines"]["bronze_merchants"]
        assert bronze_merchants["class_name"] == "BronzeMerchantsPipeline"
        assert "config" in bronze_merchants
    
    def test_save_config(self, config_manager, temp_config_dir):
        """Test saving configuration to file"""
        test_config = {
            "version": "1.0",
            "pipeline_groups": {
                "test_group": {
                    "pipelines": {
                        "test_pipeline": {
                            "class_name": "TestPipeline"
                        }
                    }
                }
            }
        }
        
        config_manager.save_config(test_config, "test_save.yml")
        
        saved_file = temp_config_dir / "test_save.yml"
        assert saved_file.exists()
        
        with open(saved_file, 'r') as f:
            loaded_config = yaml.safe_load(f)
        
        assert loaded_config == test_config
    
    def test_validate_config_missing_sections(self, config_manager):
        """Test configuration validation with missing sections"""
        invalid_config = {
            "version": "1.0"
            # Missing pipeline_groups
        }
        
        with pytest.raises(ValueError, match="Missing required configuration section"):
            config_manager._validate_config(invalid_config)
    
    def test_validate_config_missing_pipeline_fields(self, config_manager):
        """Test configuration validation with missing pipeline fields"""
        invalid_config = {
            "version": "1.0",
            "pipeline_groups": {
                "test_group": {
                    "pipelines": {
                        "test_pipeline": {
                            # Missing required class_name
                            "config": {}
                        }
                    }
                }
            }
        }
        
        with pytest.raises(ValueError, match="missing required field"):
            config_manager._validate_config(invalid_config)


if __name__ == "__main__":
    pytest.main([__file__])
