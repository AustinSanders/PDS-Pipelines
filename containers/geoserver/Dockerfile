# For an example on how to run this docker container,
# please check the buildDocker.sh script... or just configure and run the script!
# This container creates an instance of GeoServer 2.15.1

FROM alpine:3.7
USER root

# Installs JDK8
RUN apk update
RUN apk fetch openjdk8
RUN apk add openjdk8

# Creates directories for geoserver and it's downloads
RUN mkdir /usr/share/geoserver
RUN mkdir ~/geoserver_download

RUN wget -P ~/geoserver_download https://sourceforge.net/projects/geoserver/files/GeoServer/2.15.1/geoserver-2.15.1-bin.zip
RUN ls -l ~/geoserver_download
RUN unzip ~/geoserver_download/geoserver-2.15.1-bin.zip -d /usr/share/geoserver

# Set environment variable to the correct home
ENV GEOSERVER_HOME="/usr/share/geoserver/geoserver-2.15.1/"

# Runs the geoserver startup.sh
ENTRYPOINT ["/usr/share/geoserver/geoserver-2.15.1/bin/startup.sh"]
