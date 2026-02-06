#!/usr/bin/env bash

# ==========================================
# Wikipedia Spark Analysis - Setup Script
# ==========================================
# This script helps you set up the project quickly

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

check_command() {
    if command -v "$1" &> /dev/null; then
        print_success "$1 is installed"
        return 0
    else
        print_error "$1 is NOT installed"
        return 1
    fi
}

# ==========================================
# Main Setup Process
# ==========================================

print_header "Wikipedia Spark Analysis - Project Setup"

echo "This script will help you set up the project environment."
echo "It will check for required dependencies and create necessary directories."
echo ""
read -p "Press Enter to continue..."

# ==========================================
# 1. Check System Requirements
# ==========================================

print_header "Step 1: Checking System Requirements"

ALL_DEPS_OK=true

# Check Java
if check_command java; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    print_info "Java version: $JAVA_VERSION"
else
    print_warning "Please install Java 8 or 11"
    ALL_DEPS_OK=false
fi

# Check Scala
if check_command scala; then
    SCALA_VERSION=$(scala -version 2>&1 | grep version | cut -d' ' -f5)
    print_info "Scala version: $SCALA_VERSION"
else
    print_warning "Please install Scala 2.12.x"
    ALL_DEPS_OK=false
fi

# Check SBT
if check_command sbt; then
    print_info "SBT is ready"
else
    print_warning "Please install SBT 1.9.0+"
    ALL_DEPS_OK=false
fi

# Check Spark (optional for local dev)
if check_command spark-submit; then
    print_success "Spark is installed"
else
    print_warning "Spark not found (optional for local dev, required for running jobs)"
fi

# Check AWS CLI (optional)
if check_command aws; then
    print_success "AWS CLI is installed"
else
    print_info "AWS CLI not found (optional, needed for EMR deployment)"
fi

# Check aria2c (optional)
if check_command aria2c; then
    print_success "aria2c is installed (faster downloads)"
else
    print_info "aria2c not found (optional, will use curl for downloads)"
fi

if [ "$ALL_DEPS_OK" = false ]; then
    print_error "Some required dependencies are missing!"
    echo ""
    echo "Installation instructions:"
    echo "  Ubuntu/Debian: sudo apt update && sudo apt install openjdk-11-jdk scala sbt"
    echo "  macOS:         brew install openjdk@11 scala sbt"
    echo ""
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# ==========================================
# 2. Create Directory Structure
# ==========================================

print_header "Step 2: Creating Directory Structure"

# Create directories
mkdir -p dataset/wikimedia_dumps
mkdir -p dataset/categories_dump
mkdir -p dataset/sample
mkdir -p output
mkdir -p output_baseline
mkdir -p checkpoints
mkdir -p checkpoints_baseline
mkdir -p src/main/resources
mkdir -p docs

print_success "Created dataset directories"
print_success "Created output directories"
print_success "Created checkpoint directories"
print_success "Created resources directory"

# ==========================================
# 3. AWS Credentials Setup
# ==========================================

print_header "Step 3: AWS Credentials Setup"

echo "Do you want to set up AWS credentials now?"
echo "  (Required for running on AWS EMR and accessing S3)"
echo ""
read -p "Configure AWS credentials? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    
    # Create ~/.aws directory
    mkdir -p aws
    
    # AWS CLI credentials
    if [ ! -f aws/credentials ]; then
        echo "Creating aws/credentials file..."
        read -p "Enter AWS Access Key ID: " AWS_ACCESS_KEY_ID
        read -s -p "Enter AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY
        read -s -p "Enter AWS Session Token: " AWS_SECRET_ACCESS_KEY
        echo
        read -p "Enter AWS Region (default: us-east-1): " AWS_REGION
        AWS_REGION=${AWS_REGION:-us-east-1}
        
        cat > aws/credentials << EOF
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
aws_session_token= $AWS_SESSION_TOKEN
EOF
        
        cat > aws/config << EOF
[default]
region = $AWS_REGION
output = json
EOF
        
        chmod 600 aws/credentials
        chmod 600 aws/config
        
        print_success "Created aws/credentials"
        print_success "Created aws/config"
    else
        print_info "aws/credentials already exists, skipping"
    fi
    
    # Spark application credentials
    if [ ! -f src/main/resources/aws_credentials ]; then
        echo "Creating src/main/resources/aws_credentials file..."
        
        # Read from aws/credentials if available
        if [ -f aws/credentials ]; then
            AWS_KEY=$(grep "aws_access_key_id" aws/credentials | cut -d'=' -f2 | tr -d ' ')
            AWS_SECRET=$(grep "aws_secret_access_key" aws/credentials | cut -d'=' -f2 | tr -d ' ')
        else
            read -p "Enter AWS Access Key ID (for Spark app): " AWS_KEY
            read -s -p "Enter AWS Secret Access Key (for Spark app): " AWS_SECRET
            echo
        fi
        
        cat > src/main/resources/aws_credentials << EOF
$AWS_KEY
$AWS_SECRET
EOF
        
        chmod 600 src/main/resources/aws_credentials
        print_success "Created src/main/resources/aws_credentials"
        
        print_warning "IMPORTANT: This file contains secrets!"
        print_warning "Make sure it's in .gitignore and never commit it!"
    else
        print_info "src/main/resources/aws_credentials already exists, skipping"
    fi
    
    # S3 Bucket setup
    echo ""
    read -p "Do you want to create an S3 bucket now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "Enter S3 bucket name (e.g., my-wiki-analysis): " S3_BUCKET
        
        if aws s3 mb "s3://$S3_BUCKET" 2>/dev/null; then
            print_success "Created S3 bucket: s3://$S3_BUCKET"
            
            # Update Config.scala
            if [ -f src/main/scala/utils/Config.scala ]; then
                # Backup original
                cp src/main/scala/utils/Config.scala src/main/scala/utils/Config.scala.bak
                
                # Update bucket name
                sed -i.tmp "s/val s3BucketName = \".*\"/val s3BucketName = \"$S3_BUCKET\"/" src/main/scala/utils/Config.scala
                rm -f src/main/scala/utils/Config.scala.tmp
                
                print_success "Updated Config.scala with bucket name"
            fi
        else
            print_error "Failed to create bucket (may already exist or permission issue)"
            print_info "You can create it manually via AWS Console or:"
            print_info "  aws s3 mb s3://$S3_BUCKET"
        fi
    fi
    
else
    print_info "Skipping AWS setup. You can configure it later manually."
    print_info "See README.md 'Configuration' section for instructions."
fi

# ==========================================
# 4. Update Config.scala
# ==========================================

print_header "Step 4: Updating Configuration"

if [ -f src/main/scala/utils/Config.scala ]; then
    # Get current directory
    CURRENT_DIR=$(pwd)
    
    # Check if projectDir needs updating
    if grep -q "projectDir.*=.*C:\\\\Users" src/main/scala/utils/Config.scala; then
        print_warning "Detected Windows path in Config.scala"
        
        # Backup
        cp src/main/scala/utils/Config.scala src/main/scala/utils/Config.scala.bak
        
        # Update with current directory
        sed -i.tmp "s|val projectDir.*=.*|val projectDir = \"$CURRENT_DIR\"|" src/main/scala/utils/Config.scala
        rm -f src/main/scala/utils/Config.scala.tmp
        
        print_success "Updated projectDir in Config.scala"
    else
        print_info "Config.scala looks good"
    fi
else
    print_warning "Config.scala not found. Make sure project structure is correct."
fi

# ==========================================
# 5. Build Project
# ==========================================

print_header "Step 5: Building Project"

echo "Do you want to build the project now?"
echo "  This will download dependencies and compile the code (~5-10 minutes)"
echo ""
read -p "Build project? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Running: sbt assembly"
    print_warning "This may take several minutes..."

    if sbt assembly; then
        print_success "Build completed successfully!"

        if [ -f target/scala-2.12/wikipedia-analysis_2.12-1.0.jar ]; then
            JAR_SIZE=$(ls -lh target/scala-2.12/WikipediaAnalytics-0.1.0.jar | awk '{print $5}')
            print_success "JAR created: target/scala-2.12/WikipediaAnalytics-0.1.0.jar ($JAR_SIZE)"
        fi
    else
        print_error "Build failed. If this happen try running sbt assembly after the setup is complete."
    fi
else
    print_info "Skipping build. You can build later with: sbt assembly"
fi

# ==========================================
# 6. Download Sample Data (Optional)
# ==========================================

print_header "Step 6: Sample Data (Optional)"

echo "Do you want to download a small sample dataset for testing?"
echo "  Size: ~2-3 GB (takes 3-4 minutes)"
echo ""
read -p "Download sample data? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Downloading sample Wikipedia data..."

    if [ -f scripts/download_wiki_history.sh ]; then
        sh scripts/download_wiki_history.sh -s 2025-11 -e 2025-11
        sh scripts/download_categories.sh
        print_success "Sample data downloaded to dataset/"
    else
        print_error "Sample data download script not found!"
        print_info "You can create a sample dataset manually by downloading a small Wikipedia dump and placing it in dataset/sample"
    fi
else
    print_info "Skipping sample data download"
    print_info "You can download datasets later with: ./scripts/download_*.sh"
fi

# ==========================================
# 7. Setup Summary
# ==========================================

print_header "Setup Complete!"

echo "Summary:"
echo "─────────────────────────────────────────────────────────────"
echo ""

# Check what was set up
SETUP_ITEMS=()

if [ -f aws/credentials ]; then
    SETUP_ITEMS+=("${GREEN}✓${NC} AWS credentials configured")
else
    SETUP_ITEMS+=("${YELLOW}○${NC} AWS credentials not configured")
fi

if [ -f src/main/resources/aws_credentials ]; then
    SETUP_ITEMS+=("${GREEN}✓${NC} Spark AWS credentials configured")
else
    SETUP_ITEMS+=("${YELLOW}○${NC} Spark AWS credentials not configured")
fi

if [ -f target/scala-2.12/wikipedia-analysis_2.12-1.0.jar ]; then
    SETUP_ITEMS+=("${GREEN}✓${NC} Project built successfully")
else
    SETUP_ITEMS+=("${YELLOW}○${NC} Project not built yet")
fi

if [ -d dataset ] && [ "$(ls -A dataset)" ]; then
    SETUP_ITEMS+=("${GREEN}✓${NC} Sample data downloaded")
else
    SETUP_ITEMS+=("${YELLOW}○${NC} No sample data")
fi

# Print setup items
for item in "${SETUP_ITEMS[@]}"; do
    echo -e "$item"
done

print_success "Setup complete! Happy analyzing!"
