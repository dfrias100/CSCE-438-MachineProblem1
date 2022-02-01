#include <arpa/inet.h>
#include <fcntl.h>
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

#define CHATROOM_BASE_PORT 10500

struct Chatroom {
    char name[128];
    int num_members = 0;
    int port_no;
    int listener_socket;
    fd_set clients;
    int maxfd;
};

void DeleteChat(struct Chatroom chat) {
    for (int fd = 0; fd <= chat.maxfd; fd++) {
        if (FD_ISSET(fd, &chat.clients)) {
            if (fd != chat.listener_socket) {
                char killMessage[MAX_DATA] = "Warnning: the chatting room is going to be closed...\n";
                if (send(fd, killMessage, MAX_DATA, 0) < 0) {
                    fprintf(stderr, "Failure sending message.\n");
                    exit(1);
                }
            }
            
            close(fd);
        }
    }
    
    close(chat.listener_socket);
}

struct CString {
    char string[256];  
};

int CreateListener(int& port_offset) {
    // Set up the listening socket, this will monitor the commands
	int port_listener = CHATROOM_BASE_PORT + port_offset;
	int listener = socket(AF_INET, SOCK_STREAM, 0);
	
	
	if (listener < 0) {
	    fprintf(stderr, "Could not create socket. Exiting.\n");
	    exit(1);
	}
	
	// This struct will hold our IP address to bind the listener
	// socket to
	struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons(port_listener);
    
    while(bind(listener, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
        port_offset++;
        sin.sin_port = htons(port_listener + port_offset);
    }
    port_offset++;
    
    // Now we're going to try and listen to the socket
    if (listen(listener, 128) < 0) {
        fprintf(stderr, "Could not listen on this socket. Exiting.\n");
        exit(1);
    }

    return listener;
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
		fprintf(stderr,
				"usage: enter port number\n");
		exit(1);
	}
	
	int port_offset = 0;
	
	// Setting up a few vectors
	std::vector<struct Chatroom> chats;
	
	// Set up the listening socket, this will monitor the commands
	int port_listener = atoi(argv[1]);
	int listener = socket(AF_INET, SOCK_STREAM, 0);
	
	
	if (listener < 0) {
	    fprintf(stderr, "Could not create socket. Exiting.\n");
	    exit(1);
	}
	
	// This struct will hold our IP address to bind the listener
	// socket to
	struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons(port_listener);
    
    if(bind(listener, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
        fprintf(stderr, "Could not bind this socket. Exiting.\n");
        exit(1);
    }
    
    // Now we're going to try and listen to the socket
    if (listen(listener, 128) < 0) {
        fprintf(stderr, "Could not listen on this socket. Exiting.\n");
        exit(1);
    }

    fd_set client_cmd_set;
    FD_ZERO(&client_cmd_set);
    int max_fd = 0;
    
    FD_SET(listener, &client_cmd_set);
    max_fd = listener;
    
    while(1) {
        
        fd_set active_cmd_set = client_cmd_set;
        
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 500;
            
        if (select(max_fd + 1, &active_cmd_set, NULL, NULL, &timeout) > 0) {
            
            for(int fd = 0; fd <= max_fd; fd++) {
                
                if (FD_ISSET(fd, &active_cmd_set)) {
                    
                    if (fd == listener) {
                        
                        int client_sock;
                        struct sockaddr_in client;
                        socklen_t client_sock_sz = sizeof(struct sockaddr_in);
                        client_sock = accept(fd, (struct sockaddr*)&client, &client_sock_sz);

                        
                        FD_SET(client_sock, &client_cmd_set);
                        
                        if (client_sock > max_fd) {
                            max_fd = client_sock;
                        }
                        
                    } else {
                        char receivedCommand[MAX_DATA];
                        int bytes = recv(fd, receivedCommand, MAX_DATA, 0);
                        if (bytes <= 0) {
                            
                            close(fd);
                            FD_CLR(fd, &client_cmd_set);
                            
                        } else {
                            
                            struct Reply reply;
                            std::vector<CString> command_tokens;
                            char delimit[2] = " ";
                            char* token = strtok(receivedCommand, delimit);

                            while (token != NULL) {
                                struct CString str;
                                strcpy(str.string, token);
                                command_tokens.push_back(str);
                                
                                token = strtok(NULL, delimit);
                            }
                            
                            if (command_tokens.size() > 2) {
                                reply.status = FAILURE_INVALID;
                                send(fd, &reply, sizeof(reply), 0);
                            } else if (strncmp(command_tokens[0].string, "LIST", 4) == 0) {
                                char temp_list[MAX_DATA] = "";
                                    
                                for (int i = 0; i < (signed) chats.size() - 1; i++) {
                                    strncat(temp_list, chats[i].name, 128);
                                    strncat(temp_list, ",", 1);
                                }
                                
                                if (chats.size() > 0) {
                                    strncat(temp_list, chats[chats.size() - 1].name, 128);
                                } else {
                                    strncat(temp_list, "empty", 5);
                                }

                                strncpy(reply.list_room, temp_list, MAX_DATA);
                                
                                reply.status = SUCCESS;
                                send(fd, &reply, sizeof(reply), 0);
                            } else {
                                if (command_tokens.size() < 2) {
                                    reply.status = FAILURE_INVALID;
                                    send(fd, &reply, sizeof(reply), 0);
                                } else if (strncmp(command_tokens[0].string, "CREATE", 6) == 0) {
                                    bool chatFound = false;
                                    int chatIndex = 0;
                                    for (int i = 0; i < chats.size(); i++) {
                                        if (strcmp(command_tokens[1].string, chats[i].name) == 0) {
                                            chatFound = true;
                                            chatIndex = i;
                                            break;
                                        }
                                    }
                                    
                                    if (chatFound) {
                                        reply.status = FAILURE_ALREADY_EXISTS;
                                        send(fd, &reply, sizeof(reply), 0);
                                    } else {
                                        reply.status = SUCCESS;
                                        
                                        Chatroom chat;
                                        strncpy(chat.name, command_tokens[1].string, 128);
                                        int chat_fd = CreateListener(port_offset);
                                        chat.listener_socket = chat_fd;
                                        chat.port_no = CHATROOM_BASE_PORT + port_offset - 1;
                                        FD_ZERO(&chat.clients);
                                        FD_SET(chat_fd, &chat.clients);
                                        chat.maxfd = chat_fd; 
                                        chats.push_back(chat);
                                        
                                        send(fd, &reply, sizeof(reply), 0);
                                    }
                                } else if (strncmp(command_tokens[0].string, "JOIN", 4) == 0) {
                                    bool chatFound = false;
                                    int chatIndex = 0;
                                    for (int i = 0; i < chats.size(); i++) {
                                        if (strcmp(command_tokens[1].string, chats[i].name) == 0) {
                                            chatFound = true;
                                            chatIndex = i;
                                            break;
                                        }
                                    }
                                    
                                    if (chatFound) {
                                        reply.status = SUCCESS;
                                        reply.num_member = chats[chatIndex].num_members + 1;
                                        reply.port = chats[chatIndex].port_no;
                                        send(fd, &reply, sizeof(reply), 0);
                                    } else {
                                        reply.status = FAILURE_NOT_EXISTS;
                                        send(fd, &reply, sizeof(reply), 0);
                                    }
                                } else if (strncmp(command_tokens[0].string, "DELETE", 6) == 0) {
                                    bool chatFound = false;
                                    int chatIndex = 0;
                                    for (int i = 0; i < chats.size(); i++) {
                                        if (strcmp(command_tokens[1].string, chats[i].name) == 0) {
                                            chatFound = true;
                                            chatIndex = i;
                                            break;
                                        }
                                    }
                                    
                                    if (chatFound) {
                                        reply.status = SUCCESS;
                                        DeleteChat(chats[chatIndex]);
                                        chats.erase(chats.begin() + chatIndex);
                                        send(fd, &reply, sizeof(reply), 0);
                                    } else {
                                        reply.status = FAILURE_NOT_EXISTS;
                                        send(fd, &reply, sizeof(reply), 0);
                                    } 
                                } else {
                                    reply.status = FAILURE_INVALID;
                                    send(fd, &reply, sizeof(reply), 0);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        for (int i = 0; i < chats.size(); i++) {
            fd_set active_chat_set = chats[i].clients;

            if (select(chats[i].maxfd + 1, &active_chat_set, NULL, NULL, &timeout) > 0) {
                for(int fd = 0; fd <= chats[i].maxfd; fd++) {
                
                    if (FD_ISSET(fd, &active_chat_set)) {
                    
                        if (fd == chats[i].listener_socket) {
                        
                            int client_sock;
                            struct sockaddr_in client;
                            socklen_t client_sock_sz = sizeof(struct sockaddr_in);
                            client_sock = accept(fd, (struct sockaddr*)&client, &client_sock_sz);
                            
                            FD_SET(client_sock, &chats[i].clients);
                            chats[i].num_members++;
                            
                            if (client_sock > chats[i].maxfd) {
                                chats[i].maxfd = client_sock;
                            }
                        
                        } else {
                            char receivedMessage[MAX_DATA];
                            int bytes = recv(fd, receivedMessage, MAX_DATA, 0);
                            if (bytes <= 0) {
                                close(fd);
                                FD_CLR(fd, &chats[i].clients);
                                chats[i].num_members--;
                            } else {
                                if (strlen(receivedMessage) == MAX_DATA) {
                                    receivedMessage[strlen(receivedMessage) - 2] = '\n';
                                    receivedMessage[strlen(receivedMessage) - 1] = '\0';
                                } else if (strlen(receivedMessage) == MAX_DATA - 1) {
                                    receivedMessage[strlen(receivedMessage) - 1] = '\n';
                                    receivedMessage[strlen(receivedMessage)] = '\0';
                                } else {
                                    receivedMessage[strlen(receivedMessage)] = '\n';
                                    receivedMessage[strlen(receivedMessage) + 1] = '\0';
                                }
                                for (int fd2 = 0; fd2 <= chats[i].maxfd; fd2++) {
                                    if (FD_ISSET(fd2, &chats[i].clients)) {
                                        if (fd2 != chats[i].listener_socket && fd2 != fd) {
                                            if (send(fd2, receivedMessage, bytes, 0) < 0) {
                                                fprintf(stderr, "Failure sending message.\n");
                                                exit(1);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return 0;
}