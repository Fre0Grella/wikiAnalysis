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
mkdir -p logs

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
    mkdir -p ~/.aws
    
    # AWS CLI credentials
    if [ ! -f ~/.aws/credentials ]; then
        echo "Creating ~/.aws/credentials file..."
        read -p "Enter AWS Access Key ID: " AWS_ACCESS_KEY_ID
        read -s -p "Enter AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY
        echo
        read -p "Enter AWS Region (default: us-east-1): " AWS_REGION
        AWS_REGION=${AWS_REGION:-us-east-1}
        
        cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
EOF
        
        cat > ~/.aws/config << EOF
[default]
region = $AWS_REGION
output = json
EOF
        
        chmod 600 ~/.aws/credentials
        chmod 600 ~/.aws/config
        
        print_success "Created ~/.aws/credentials"
        print_success "Created ~/.aws/config"
    else
        print_info "~/.aws/credentials already exists, skipping"
    fi
    
    # Spark application credentials
    if [ ! -f src/main/resources/aws_credentials ]; then
        echo "Creating src/main/resources/aws_credentials file..."
        
        # Read from ~/.aws/credentials if available
        if [ -f ~/.aws/credentials ]; then
            AWS_KEY=$(grep "aws_access_key_id" ~/.aws/credentials | cut -d'=' -f2 | tr -d ' ')
            AWS_SECRET=$(grep "aws_secret_access_key" ~/.aws/credentials | cut -d'=' -f2 | tr -d ' ')
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
    print_info "Running: sbt clean compile package"
    print_warning "This may take several minutes..."
    
    if sbt clean compile package; then
        print_success "Build completed successfully!"
        
        if [ -f target/scala-2.12/wikipedia-analysis_2.12-1.0.jar ]; then
            JAR_SIZE=$(ls -lh target/scala-2.12/wikipedia-analysis_2.12-1.0.jar | awk '{print $5}')
            print_success "JAR created: target/scala-2.12/wikipedia-analysis_2.12-1.0.jar ($JAR_SIZE)"
        fi
    else
        print_error "Build failed. Check error messages above."
    fi
else
    print_info "Skipping build. You can build later with: sbt package"
fi

# ==========================================
# 6. Download Sample Data (Optional)
# ==========================================

print_header "Step 6: Sample Data (Optional)"

echo "Do you want to download a small sample dataset for testing?"
echo "  Size: ~10 MB (takes 1-2 minutes)"
echo ""
read -p "Download sample data? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Downloading sample Wikipedia data..."
    
    # Download a small sample
    SAMPLE_URL="https://dumps.wikimedia.org/enwiki/20241101/enwiki-20241101-pages-articles-multistream1.xml-p1p41242.bz2"
    
    if command -v aria2c &> /dev/null; then
        aria2c -x 4 -d dataset/sample "$SAMPLE_URL"
    else
        curl -L -o dataset/sample/sample.xml.bz2 "$SAMPLE_URL"
    fi
    
    print_success "Sample data downloaded to dataset/sample/"
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

if [ -f ~/.aws/credentials ]; then
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

if [ -d dataset/sample ] && [ "$(ls -A dataset/sample)" ]; then
    SETUP_ITEMS+=("${GREEN}✓${NC} Sample data downloaded")
else
    SETUP_ITEMS+=("${YELLOW}○${NC} No sample data")
fi

# Print setup items
for item in "${SETUP_ITEMS[@]}"; do
    echo -e "$item"
done

echo ""
echo "─────────────────────────────────────────────────────────────"
echo ""
echo "Next Steps:"
echo ""
echo "  1. Review and edit configuration:"
echo "     ${BLUE}src/main/scala/utils/Config.scala${NC}"
echo ""
echo "  2. Download full datasets (47+ GB):"
echo "     ${BLUE}./scripts/download_wiki_history.sh -s 2024-01 -e 2024-12${NC}"
echo "     ${BLUE}./scripts/download_categories.sh${NC}"
echo ""
echo "  3. Test locally with sample data:"
echo "     ${BLUE}spark-submit --class JobLauncher --master local[*] \\${NC}"
echo "     ${BLUE}  target/scala-2.12/wikipedia-analysis_2.12-1.0.jar \\${NC}"
echo "     ${BLUE}  local cat overwrite optimized${NC}"
echo ""
echo "  4. Deploy to AWS EMR:"
echo "     ${BLUE}See README.md 'Running on AWS EMR' section${NC}"
echo ""
echo "  5. Read documentation:"
echo "     ${BLUE}README.md${NC} - Complete guide"
echo "     ${BLUE}docs/PERFORMANCE_ANALYSIS.md${NC} - Performance metrics"
echo ""
echo "For help: ${BLUE}https://github.com/yourusername/wikipedia-spark-analysis${NC}"
echo ""

print_success "Setup complete! Happy analyzing!"
