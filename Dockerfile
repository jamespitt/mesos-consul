FROM golang:1.7.5-alpine3.5

MAINTAINER Chris Aubuchon <Chris.Aubuchon@gmail.com>

COPY . /go/src/github.com/jamespitt/mesos-consul
COPY ./mesos-run /bin/
RUN apk add --update make git glide \
	&& cd /go/src/github.com/jamespitt/mesos-consul \
	&& make vendor \
	&& go build -o /bin/mesos-consul \
	&& rm -rf /go \
	&& apk del --purge make git glide

ENTRYPOINT [ "/bin/mesos-run" ]
