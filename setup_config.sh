#!/bin/bash

GREEN="\e[32m"
YELLOW="\e[93m"
NOCOLOR="\e[0m"
DIM_COLOR="\e[2m"
RED_COLOR="\e[31m"

config_key=mstream-config.toml

if [ -f ${config_key} ]
then
    read -p "$(printf "Configuration file ${YELLOW}${config_key}${NOCOLOR} already exists! Overwrite config? [y/N]${NOCOLOR}") " overwrite_config
    case ${overwrite_config} in
        [yY])
            echo -e $(printf "${GREEN}Overwriting ${YELLOW}${config_key}${NOCOLOR}")
            cp ${config_key} ${config_key}.bak
            ;;
        *)
            echo -e $(printf "${GREEN}Exiting interactive config${NOCOLOR}")
            exit 0;
            ;;
    esac
fi

# Configure GCP project
read -p "$(printf "\n> ${YELLOW}Enter GCP Service Account Key Path${NOCOLOR}:") " gc_key_path
echo "gcp_service_account_key_path = \"${gc_key_path}\"" > ${config_key}

# Configure change stream connectors
stream_count=0

echo -e "\n"$(printf "${DIM_COLOR}Entering Interactive config${NOCOLOR}")
while :
do
    if [ ! ${stream_count} -eq 0 ]
    then
        read -p "$(printf "> ${YELLOW}Add another stream configuration? [y/n]${NOCOLOR}") " default_config
    else
        default_config=y
    fi

    case ${default_config} in
        [nN])
            echo -e $(printf "${GREEN}Exiting interactive config${NOCOLOR}")
            break
            ;;
        *)
            read -p  "$(printf "> ${YELLOW}Enter stream name":${NOCOLOR}) " connector_name
            read -p  "$(printf "> ${YELLOW}Enter db connection string":${NOCOLOR}) " db_connection
            read -p  "$(printf "> ${YELLOW}Enter db name":${NOCOLOR}) " db_name
            read -p  "$(printf "> ${YELLOW}Enter db collection":${NOCOLOR}) " db_collection
            read -p  "$(printf "> ${YELLOW}Enter pubsub schema":${NOCOLOR}) " pubsub_schema
            read -p  "$(printf "> ${YELLOW}Enter pubsub topic":${NOCOLOR}) " pubsub_topic

            echo -e $(printf "${GREEN}Adding ${YELLOW}${connector_name}${GREEN} to ${YELLOW}${config_key}${GREEN}${NOCOLOR}")
            echo -e "\n[[connectors]]" >> ${config_key}
            echo "name = \"${connector_name}\"" >> ${config_key}
            echo "db_connection = \"${db_connection}\"" >> ${config_key}
            echo "db_name = \"${db_name}\"" >> ${config_key}
            echo "db_collection = \"${db_collection}\"" >> ${config_key}
            echo "schema = \"${pubsub_schema}\"" >> ${config_key}
            echo "topic = \"${pubsub_topic}\"" >> ${config_key}

            stream_count=$((stream_count+1))
    esac
done

if [ ! -f ${config_key} ]
then
    echo -e $(printf "${RED_COLOR}Error: configuration file ${YELLOW}${config_key}${RED_COLOR} does not exist!${NOCOLOR}")
    exit 1;
fi

# rm ${config_key}.bak
