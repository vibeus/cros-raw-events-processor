#!/bin/bash

SOURCE_CONFIG=/app/config/redshift.json
TARGET_CONFIG=$SOURCE_CONFIG

STATE_FILE=s3://vibe-singer/vibe-cros-raw-events-processor/state.json

PREV_STATE=/tmp/prev-state.json
CURR_STATE=/tmp/state.json

MODE=$1
TARGET_CONFIG_ARG=$2
TEMP_CONFIG_ARG=$3

if [ "$MODE" = "debug" ]; then
  MODE_ARG="--debug"
elif [ "$MODE" = "production" ]; then
  MODE_ARG=""
else
  echo "Please specifiy a valid mode: \"debug\" or \"production\"."
  exit 1
fi

if [ -n "$TARGET_CONFIG_ARG" ]; then
  TARGET_CONFIG=$TARGET_CONFIG_ARG
fi

if [ -n "$TEMP_CONFIG_ARG" ]; then
  ADDITIONAL_TEMP_CONFIG_ARG="-i $TEMP_CONFIG_ARG"
fi

echo "In $MODE mode."

echo "Restoring previous state from $STATE_FILE..."

if (aws s3 cp $STATE_FILE $PREV_STATE); then
  STATE_ARG="-s $PREV_STATE"
  echo "Previous state successfully loaded."
else
  STATE_ARG=""
  PREV_STATE=""
  echo "Cannot find previous state."
fi

./run.py -r $SOURCE_CONFIG -c $TARGET_CONFIG $ADDITIONAL_TEMP_CONFIG_ARG $STATE_ARG $MODE_ARG > $CURR_STATE

if ["$MODE" = "debug"]; then
  echo "Do not save state file in debug mode."
else
  echo "Saving state to $STATE_FILE..."
  aws s3 cp $CURR_STATE $STATE_FILE
fi

rm $CURR_STATE $PREV_STATE