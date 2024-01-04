SET DELAY=10

docker-compose --file docker-compose-replicaset.yml down

docker-compose --file docker-compose-replicaset.yml up -d

echo "****** Waiting for ${DELAY} seconds for containers to go up ******"
timeout /t %DELAY%

docker exec mongo1 /scripts/rs-init.sh
