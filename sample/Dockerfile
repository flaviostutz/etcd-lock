FROM golang:1.10 AS BUILD

#now build source code
ADD main.go $GOPATH/src/sample/main.go
RUN go get -v sample

CMD [ "/go/bin/sample" ]
