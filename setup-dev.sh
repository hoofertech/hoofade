#!/bin/bash

# Exit on any error
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print status messages
print_status() {
    echo -e "${GREEN}>>> $1${NC}"
}

print_error() {
    echo -e "${RED}>>> Error: $1${NC}"
}

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    print_status "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
print_status "Activating virtual environment..."
source .venv/bin/activate

# Install or upgrade pip
print_status "Upgrading pip..."
python -m pip install --upgrade pip

# Install poetry if not installed
if ! command -v poetry &> /dev/null; then
    print_status "Installing Poetry..."
    pip install poetry
fi

# Configure poetry to use the virtual environment
print_status "Configuring Poetry..."
poetry config virtualenvs.in-project true
poetry config virtualenvs.create false

# Install dependencies
print_status "Installing dependencies..."
poetry install

# Check if .env file exists
if [ ! -f ".env" ]; then
    print_status "Creating .env file..."
    cp .env.example .env
    print_status "Please update the .env file with your configuration"
    exit 1
fi

# Run the project
print_status "Starting the application..."
poetry run python -m main --log-cli-level=INFO
