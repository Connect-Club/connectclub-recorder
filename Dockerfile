FROM ubuntu:20.04

RUN apt update && apt install ca-certificates -y && apt-get clean autoclean

WORKDIR /app
ADD connectclub-recorder /app/

ENTRYPOINT ["./connectclub-recorder"]
