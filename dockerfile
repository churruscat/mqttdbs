#dockerfile
FROM python:3

# Build-time metadata as defined at http://label-schema.org
ARG BUILD_DATE
ARG VCS_REF
LABEL 	maintainer="chuRRuscat <luis.morras@gmail.com>" \
		org.label-schema.build-date=$BUILD_DATE \
		org.label-schema.docker.dockerfile="/Dockerfile" \
		org.label-schema.license="BSD 3-Clause" \
    	org.label-schema.name="mqttdb" \
		org.label-schema.url="https://hub.docker.com/r/churruscat/mqttdb/" \
		org.label-schema.vcs-ref=$VCS_REF \
		org.label-schema.vcs-type="Git" \
		org.label-schema.vcs-url="https://github.com/churruscat/mqttdb"

WORKDIR /
COPY  requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
#RUN ["pip3"," install"," -r requirements.txt"]

COPY  mqttdbs.py .
ADD mqttdbs.py .
VOLUME ["/etc/mqttdbs"]
ENTRYPOINT ["python", "mqttdbs.py"]



