#!/bin/bash
docker rm pubsub-emulator 2>/dev/null || true && \
   docker run -d -p 8085:8085 --name pubsub-emulator \
     gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
     gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 && \
   sleep 5 && \
   curl -s -X PUT "http://localhost:8085/v1/projects/test-project/topics/test-topic" > /dev/null && \
   curl -s -X PUT "http://localhost:8085/v1/projects/test-project/subscriptions/test-subscription" \
     -H "Content-Type: application/json" \
     -d '{"topic":"projects/test-project/topics/test-topic"}' > /dev/null && \
   echo "Emulator ready"
