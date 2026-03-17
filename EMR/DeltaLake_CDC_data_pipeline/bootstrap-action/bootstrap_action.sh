#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
#!/usr/bin/env bash
set -e

#Install s3-dist-cp for copying large amount of data.
sudo yum -y install s3-dist-cp

sudo yum install openssl11-libs -y

#Needed for reading some gcc files
sudo yum -y install python3-devel

#Needed for packages that connect using the odbc drivers. Add the microsoft repo and download the unixODBC.
#sudo yum-config-manager --add-repo https://packages.microsoft.com/config/rhel/6/prod.repo
#sudo yum -y remove unixODBC #to avoid conflicts
#sudo ACCEPT_EULA=Y yum -y install msodbcsql-13.0.1.0-1 mssql-tools-14.0.2.0-1
#sudo yum -y install unixODBC-utf16-devel

touch py-virtualenv-requirements.txt
cat > ./py-virtualenv-requirements.txt <<EOF
asn1crypto==0.24.0
aws-requests-auth==0.4.2
boto==2.49.0
boto3==1.20.45
botocore==1.23.45
psycopg2==2.7.7
psycopg2-binary==2.7.7
redis==3.5.3
EOF

sudo pip3 install -r py-virtualenv-requirements.txt
