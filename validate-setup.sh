#!/bin/bash

# LASAGNA Setup Validation Script
# This script validates that the setup script worked correctly

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo "🔍 LASAGNA Setup Validation"
echo "=========================="

# Check if setup script exists
if [ -f "./setup-lasagna.sh" ]; then
    print_success "Setup script found"
else
    print_error "Setup script not found"
    exit 1
fi

# Check if setup script is executable
if [ -x "./setup-lasagna.sh" ]; then
    print_success "Setup script is executable"
else
    print_warning "Setup script is not executable (run: chmod +x setup-lasagna.sh)"
fi

# Check Trino configuration
if [ -f "./images/trino/conf/config.properties" ]; then
    print_success "Trino config.properties exists"
    
    # Check for memory settings
    if grep -q "query.max-memory=" "./images/trino/conf/config.properties"; then
        print_success "Trino memory settings configured"
    else
        print_error "Trino memory settings missing"
    fi
else
    print_error "Trino config.properties not found"
fi

if [ -f "./images/trino/conf/jvm.config" ]; then
    print_success "Trino jvm.config exists"
    
    # Check for heap settings
    if grep -q "Xmx" "./images/trino/conf/jvm.config"; then
        print_success "Trino JVM heap settings configured"
    else
        print_error "Trino JVM heap settings missing"
    fi
else
    print_error "Trino jvm.config not found"
fi

# Check Spark configuration
if [ -f "./images/workspace/conf/spark-defaults.conf" ]; then
    print_success "Spark spark-defaults.conf exists"
    
    # Check for memory settings
    if grep -q "spark.driver.memory" "./images/workspace/conf/spark-defaults.conf"; then
        print_success "Spark memory settings configured"
    else
        print_error "Spark memory settings missing"
    fi
else
    print_error "Spark spark-defaults.conf not found"
fi

# Check Docker Compose
if [ -f "./docker-compose.yml" ]; then
    print_success "docker-compose.yml exists"
    
    # Check for memory limits
    if grep -q "memory:" "./docker-compose.yml"; then
        print_success "Docker memory limits configured"
    else
        print_warning "Docker memory limits not found"
    fi
else
    print_error "docker-compose.yml not found"
fi

# Check Docker is running
if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
        print_success "Docker is running"
    else
        print_error "Docker is not running"
    fi
else
    print_error "Docker is not installed"
fi

echo ""
echo "🎯 Validation Complete!"
echo ""
echo "Next steps:"
echo "1. Run: ./setup-lasagna.sh -f    # Force regenerate configs"
echo "2. Run: docker-compose build     # Build images with new configs"
echo "3. Run: docker-compose up -d     # Start the stack"
echo ""
echo "For help: ./setup-lasagna.sh -h"
