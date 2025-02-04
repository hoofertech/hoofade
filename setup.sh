#!/bin/bash

# Exit on any error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting setup...${NC}"

# Check if script is run as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Please run as root (use sudo)${NC}"
    exit 1
fi

# Function to print status messages
print_status() {
    echo -e "${GREEN}>>> $1${NC}"
}

# Stop existing services if running
print_status "Stopping existing services..."
systemctl stop hoofades || true  # || true prevents script from failing if service doesn't exist
systemctl disable hoofades || true

# Clear old nginx config if exists
rm -f /etc/nginx/sites-enabled/hoofades
rm -f /etc/nginx/sites-available/hoofades

# Update system
print_status "Updating system packages..."
apt update

# Install required system packages
print_status "Installing required packages..."
apt install -y python3-venv nginx curl

# Create application directory if it doesn't exist
APP_DIR="/opt/hoofades"
print_status "Creating application directory at ${APP_DIR}..."
mkdir -p $APP_DIR

# Copy application files (assuming script is run from project root)
print_status "Copying application files..."
cp -r ./* $APP_DIR/
cd $APP_DIR

# Setup Python virtual environment and install dependencies with Poetry
print_status "Setting up Python environment and installing dependencies..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install poetry
poetry config virtualenvs.create false
poetry install

# Create systemd service file
print_status "Creating systemd service..."
cat > /etc/systemd/system/hoofades.service << EOL
[Unit]
Description=Hoofades Application
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=$APP_DIR
Environment="PATH=$APP_DIR/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart="$APP_DIR/.venv/bin/poetry run python -m main --log-cli-level=INFO"
Restart=always

[Install]
WantedBy=multi-user.target
EOL

# Create nginx configuration
print_status "Configuring nginx..."
cat > /etc/nginx/sites-available/hoofades << EOL
server {
    listen 80;
    server_name _;  # Change this to your domain if needed

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_cache_bypass \$http_upgrade;
    }
}
EOL

# Create .env file if it doesn't exist
print_status "Creating .env file..."
if [ ! -f "$APP_DIR/.env" ]; then
    cat > $APP_DIR/.env << EOL
WEB_PORT=8000
WEB_HOST=127.0.0.1
DATABASE_URL=sqlite+aiosqlite:///hoofades.db
EOL
fi

# Set proper permissions
print_status "Setting permissions..."
chown -R www-data:www-data $APP_DIR
chmod -R 755 $APP_DIR
chmod -R 777 $APP_DIR/.venv  # Ensure venv is writable by www-data

# Enable and configure nginx
print_status "Enabling nginx configuration..."
ln -sf /etc/nginx/sites-available/hoofades /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default  # Remove default site
nginx -t  # Test nginx configuration

# Reload systemd and start services
print_status "Starting services..."
systemctl daemon-reload
systemctl enable hoofades
systemctl start hoofades
systemctl restart nginx

print_status "Setup complete! The application should now be running."
echo -e "You can check the status using:"
echo -e "  systemctl status hoofades"
echo -e "  systemctl status nginx"
echo -e "Logs can be viewed using:"
echo -e "  journalctl -u hoofades -f" 
