DOCKER_IMAGE_VERSION=2.01
DOCKER_IMAGE_NAME=churruscat/mqttdbs
DOCKER_IMAGE_TAGNAME=$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)

default: build
# First you need to prepare buildx => docker buildx create --name multi && docker buildx use multi

build:
	docker buildx build . --platform=linux/arm/v7,linux/arm64 -t $(DOCKER_IMAGE_TAGNAME) -t $(DOCKER_IMAGE_NAME):latest --push

#	docker build -t $(DOCKER_IMAGE_TAGNAME) .
	docker tag $(DOCKER_IMAGE_TAGNAME) $(DOCKER_IMAGE_NAME):latest

push:
# First use a "docker login" with username, password and email address
	docker push $(DOCKER_IMAGE_NAME)

test:
	docker run --rm $(DOCKER_IMAGE_TAGNAME) /bin/echo "Success."

rmi:
	docker rmi -f $(DOCKER_IMAGE_TAGNAME)

rebuild: rmi build

