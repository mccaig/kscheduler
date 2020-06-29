#!/bin/bash

echo "" >> "/opt/config/application.properties"

(
    function updateConfig() {
        key=$1
        value=$2
        file=$3

        # Omit $value here, in case there is sensitive information
        echo "[Configuring] '$key' in '$file'"

        # If config exists in file, replace it. Otherwise, append to file.
        if grep -E -q "^#?$key=" "$file"; then
            sed -r -i "s@^#?$key=.*@$key=$value@g" "$file" #note that no config values may contain an '@' char
        else
            echo "$key=$value" >> "$file"
        fi
    }

    # Fixes #312
    # KAFKA_VERSION + KAFKA_HOME + grep -rohe KAFKA[A-Z0-0_]* /opt/kafka/bin | sort | uniq | tr '\n' '|'
    EXCLUSIONS="|KAFKA_VERSION|KAFKA_HOME|KAFKA_DEBUG|KAFKA_GC_LOG_OPTS|KAFKA_HEAP_OPTS|KAFKA_JMX_OPTS|KAFKA_JVM_PERFORMANCE_OPTS|KAFKA_LOG|KAFKA_OPTS|"

    # Read in env as a new-line separated array. 
    IFS=$'\n'
    for VAR in $(env)
    do
        env_var=$(echo "$VAR" | cut -d= -f1)

        if [[ $env_var =~ ^KSCHEDULER_ ]]; then
            kscheduler_name=$(echo "$env_var" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .)
            updateConfig "$kscheduler_name" "${!env_var}" "/opt/config/application.properties"
        fi
    done
)

sleep 15s

exec java -Dconfig.file=/opt/config/application.properties $JAVA_OPTS -jar "$@"

