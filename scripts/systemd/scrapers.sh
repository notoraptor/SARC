#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
sudo -u sarc SARC_MODE=scrapping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.json ../.local/bin/poetry run sarc acquire jobs -c narval cedar beluga graham -d auto
