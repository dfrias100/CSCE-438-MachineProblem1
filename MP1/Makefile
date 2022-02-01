all: server client

all-debug: server-debug client-debug

clean: 
	rm *.o
	
server: interface.h crsd.c
	g++ crsd.c -o crsd

client: interface.h crc.c
	g++ crc.c -o crc
	
server-debug: interface.h crsd.c
	g++ -g crsd.c -o crsd

client-debug: interface.h crc.c
	g++ -g crc.c -o crc