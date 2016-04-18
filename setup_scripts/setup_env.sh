#!/bin/bash

#Setup virtual env
su -
pip install virtualenv
virtualenv /opt/py27
virtualenv -p /usr/bin/python2.7 /opt/py27

su - w205
source /opt/py27/bin/activate

#Check python version.
echo "Python version"
python --version

#Install pyhive dependencies
sudo yum install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64
/opt/py27/bin/pip install -r requirements.txt
pip install PyHive

#Install other libraries
pip install pandas
