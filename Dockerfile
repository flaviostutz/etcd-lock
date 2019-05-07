FROM golang:1.12.4-stretch AS BUILD

#doing dependency build separated from source build optimizes time for developer, but is not required
#install external dependencies first
# ADD /main.go $GOPATH/src/etcdlock/main.go
# RUN go get -v etcdlock

# #now build source code
# ADD etcdlock $GOPATH/src/etcdlock
# RUN go test -v etcdlock

RUN mkdir /etcdlock
WORKDIR /etcdlock

ADD go.mod .
ADD go.sum .
RUN go mod download

ADD main.go .
ADD etcdlock/ ./

RUN go test -v
