#!/bin/bash

# run_gas.sh
#
# Rns the GAS app using the Gunicorn server for production environments

cd /home/ec2-user/mpcs-cc/gas/web
source /home/ec2-user/mpcs-cc/gas/web/.env
[[ -d /home/ec2-user/mpcs-cc/gas/web/log ]] || mkdir /home/ec2-user/gas/web/log
if [ ! -e /home/ec2-user/mpcs-cc/gas/web/log/$GAS_LOG_FILE_NAME ]; then
    touch /home/ec2-user/mpcs-cc/gas/web/log/$GAS_LOG_FILE_NAME;
fi
if [ "$1" = "console" ]; then
    LOG_TARGET=-
else
    LOG_TARGET=/home/ec2-user/mpcs-cc/gas/web/log/$GAS_LOG_FILE_NAME
fi

aws s3 cp s3://mpcs-cc-resources/ssl/privkey.pem /home/ec2-user/mpcs-cc
aws s3 cp s3://mpcs-cc-resources/ssl/fullchain.pem /home/ec2-user/mpcs-cc

SSL_CERT_PATH="/home/ec2-user/mpcs-cc/fullchain.pem"
SSL_KEY_PATH="/home/ec2-user/mpcs-cc/privkey.pem"

/home/ec2-user/mpcs-cc/bin/gunicorn \
  --log-file=$LOG_TARGET \
  --log-level=debug \
  --workers=$GUNICORN_WORKERS \
  --certfile=$SSL_CERT_PATH \
  --keyfile=$SSL_KEY_PATH \
  --bind=$GAS_APP_HOST:$GAS_HOST_PORT gas:app

