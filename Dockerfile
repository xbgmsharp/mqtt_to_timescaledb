# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from ubuntu:latest base image
FROM ubuntu:latest

# Make dir app
#RUN mkdir /app
WORKDIR /app
COPY . .
RUN apt-get -q update && \
    apt-get -qy upgrade && \
    apt-get -qy install python3-dateutil python3-paho-mqtt python3-psycopg2

# Run the executable
CMD ["python3", "mqtt_to_timescaledb.py"]
