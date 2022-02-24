#docker run --name dynamodb -p 8000:8000 amazon/dynamodb-local
# run as daemon
docker run -d --name dynamodb -p 8000:8000 amazon/dynamodb-local
# stop the conatiner with 'docker stop dynamodb && docker rm dynamodb'
