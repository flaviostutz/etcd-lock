FROM golang:1.10 AS BUILD

#doing dependency build separated from source build optimizes time for developer, but is not required
#install external dependencies first
ADD /main.go $GOPATH/src/etcdlock/main.go
RUN go get -v etcdlock

#now build source code
ADD etcdlock $GOPATH/src/etcdlock
RUN go test -v etcdlock

