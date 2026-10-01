docker pull arizephoenix/phoenix:latest

docker run -d \
  --name phoenix \
  -p 6006:6006 \
  -p 4317:4317 \
  -v phoenix_data:/root/.phoenix \
  -e PHOENIX_HOST=0.0.0.0 \
  -e PHOENIX_PORT=6006 \
  --restart unless-stopped \
  arizephoenix/phoenix:latest



  docker run -d --name phoenix -p 6006:6006 -p 4317:4317 -v phoenix_data:/root/.phoenix -e PHOENIX_HOST=0.0.0.0 -e PHOENIX_PORT=6006 --restart unless-stopped arizephoenix/phoenix:latest