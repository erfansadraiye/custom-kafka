# ATTENTION PLEASE: Don't Run This file,
# Just do the following steps in your terminal
# you can run maven commands with IDE
docker compose down
mvn clean
mvn package
docker build -t my-own-kafka .
docker compose up -d