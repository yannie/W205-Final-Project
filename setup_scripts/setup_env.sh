#!/bin/bash

#Remove root password
passwd -d root

#Install UCB setup scripts.
echo "Downloading and setting up ucb_complete_plus_postgres.sh"
wget https://s3.amazonaws.com/ucbdatasciencew205/setup_ucb_complete_plus_postgres.sh
chmod +x ./setup_ucb_complete_plus_postgres.sh
./setup_ucb_complete_plus_postgres.sh

#Setup virtual env
echo "Setting up virtual env"
pip install virtualenv
virtualenv /opt/py27
virtualenv -p /usr/bin/python2.7 /opt/py27

source /opt/py27/bin/activate

#Check python version.
echo "Python version"
python --version

#Install pyhive dependencies
sudo yum install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64
/opt/py27/bin/pip install -r /home/w205/final_project/W205-Final-Project/setup_scripts/requirements.txt
pip install PyHive

