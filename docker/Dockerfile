FROM arm64v8/ubuntu
LABEL authors="tihomir.mateev"
WORKDIR /usr/src/lettuce-core
COPY . ./
RUN apt update
RUN apt install -y stunnel make git gcc openjdk-8-jdk maven pkg-config
EXPOSE 6443-6445
EXPOSE 6925-6926
EXPOSE 7442-7445
EXPOSE 8443-8444
EXPOSE 26822-26824
ENTRYPOINT ["make", "-n", "start"]
