#!/bin/bash
sleep 10 && \
   curl -s -X PUT "http://localhost:8085/v1/projects/test-project/topics/test-topic" && \
   curl -s -X PUT "http://localhost:8085/v1/projects/test-project/subscriptions/test-subscription" \
     -H "Content-Type: application/json" \
     -d '{"topic":"projects/test-project/topics/test-topic"}' && \
   echo -e "\nEmulator resources created"
