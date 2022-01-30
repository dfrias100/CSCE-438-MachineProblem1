all: server client

all-debug: server-debug client-debug

clean: 
	rm *.o
	
server: interface.h crsd.cpp
	g++ crsd.cpp -o crsd

client: interface.h crc.cpp
	g++ crc.cpp -o crc
	
server-debug: interface.h crsd.cpp
	g++ -g crsd.cpp -o crsd

client-debug: interface.h crc.cpp
	g++ -g crc.cpp -o crc