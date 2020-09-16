stubs:
	python -m grpc.tools.protoc -I. --python_out=./ --grpc_python_out=./ scrapydd/grpcservice/service.proto

gen_key:
	openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 365 -out server.crt
