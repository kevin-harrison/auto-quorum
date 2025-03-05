#!/bin/bash
# Startup script for AutoQuorum GCP instances. This script is executed during instance creation.
# For debugging, SSH into the instance and run `sudo journalctl -u google-startup-scripts.service`.

# Get OS login user from instance metadata
OSLOGIN_USER=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/oslogin_user)

# Ensure OS login user is setup
useradd -m $OSLOGIN_USER
mkdir -p /home/$OSLOGIN_USER
chown $OSLOGIN_USER:$OSLOGIN_USER /home/$OSLOGIN_USER

# Configure Docker credentials for the user
sudo -u $OSLOGIN_USER docker-credential-gcr configure-docker --registries=gcr.io
sudo -u $OSLOGIN_USER echo "https://gcr.io" | docker-credential-gcr get
sudo groupadd docker
sudo usermod -aG docker $OSLOGIN_USER
