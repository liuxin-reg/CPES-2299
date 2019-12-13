FROM golang:1.13.4 as builder

WORKDIR /go/src/github.com/liuxin-reg/CPES-2299/

COPY ["main.go", "config.json", "./"]

RUN GOPROXY=https://goproxy.cn,direct GO111MODULE=on go get -d -v github.com/Shopify/sarama \
	&& GOPROXY=https://goproxy.cn,direct GO111MODULE=on GOOS=linux go build -o app main.go \
	&& cp /go/src/github.com/liuxin-reg/CPES-2299/app /root
	
	
FROM ubuntu:18.04 as prod

WORKDIR /root/

COPY --from=0 /go/src/github.com/liuxin-reg/CPES-2299/app .

CMD ["./app"]


