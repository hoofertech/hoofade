#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}>>> Activating virtual environment...${NC}"
source .venv/bin/activate

echo -e "${GREEN}>>> Starting the application...${NC}"
python src/main.py 
