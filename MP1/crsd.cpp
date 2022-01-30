#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include "interface.h"

int main(int argc, char* argv[]) {
    if (argc != 2) {
		fprintf(stderr,
				"usage: enter port number\n");
		exit(1);
	}
    
    fd_set client_cmd_set;
    FD_ZERO(&client_cmd_set);
    int max_fd = 0;
    
    int port_listener;
    int fd;
    
    while(1) {
        
    }
    return 0;
}