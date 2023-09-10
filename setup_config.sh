#!/bin/bash

GREEN="\e[32m"
YELLOW="\e[93m"
NOCOLOR="\e[0m"
DIM_COLOR="\e[2m"

if [ ! -f .env ]
then
    echo -e $(printf "${GREEN}Creating .env file${NOCOLOR}")
    touch .env
    read -p  "$(printf "> ${YELLOW}Enter path to GCP service account key file":${NOCOLOR}) " gcp_key_path

    echo -e $(printf "${GREEN}Adding GCP service account key path to .env${NOCOLOR}")
    echo "export MSTREAM_SERVICE_ACCOUNT_KEY_PATH=${gcp_key_path}" >> .env    
fi

config_key=mstream-config.toml

if [ ! -f ${config_key} ]
then
    echo -e $(printf "${GREEN}Configure your change stream connectors in ${YELLOW}${config_key}${GREEN}${NOCOLOR}")

    cp mstream-config.toml.dist ${config_key}

    exit 0;
fi
