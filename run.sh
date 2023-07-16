docker build -t sandiego-fetch .
docker stop sandiego-fetch
docker rm sandiego-fetch
docker run -d \
    -v /opt/sandiego-fetch/secrets:/usr/src/secrets \
    -v /opt/sandiego-fetch/configuration.yml:/usr/src/configuration.yml:ro \
    -u 1001 \
    --restart unless-stopped \
    --name sandiego-fetch \
    --network valinor_default \
    --env-file .env \
    sandiego-fetch
if [ "$1" = "-l" ]; then
    docker logs sandiego-fetch -f
fi
