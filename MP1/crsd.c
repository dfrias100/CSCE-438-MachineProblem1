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
    // Set up the master chat listening socket, this will monitor the chat that it is assigned to
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
	
	// This will hold our chat structs, the STL vector will make handling deletes *a lot* easier
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

    // This fdset will monitor the command processing side of the server
    fd_set client_cmd_set;
    FD_ZERO(&client_cmd_set);
    int max_fd = 0;
    
    // Add the command listener
    FD_SET(listener, &client_cmd_set);
    max_fd = listener;
    
    while(1) {
        
        fd_set active_cmd_set = client_cmd_set;
        
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 500;
        
        // Select is blocking and we might have other pending fds    
        if (select(max_fd + 1, &active_cmd_set, NULL, NULL, &timeout) > 0) {
            
            // Loop through all the possible fds
            for(int fd = 0; fd <= max_fd; fd++) {
                
                // Check if this fd is part of the read set
                if (FD_ISSET(fd, &active_cmd_set)) {
                    
                    // Someone connected to the server
                    if (fd == listener) {
                        
                        // Getting the inet information of the client
                        int client_sock;
                        struct sockaddr_in client;
                        socklen_t client_sock_sz = sizeof(struct sockaddr_in);
                        client_sock = accept(fd, (struct sockaddr*)&client, &client_sock_sz);

                        // Adding the client socket
                        FD_SET(client_sock, &client_cmd_set);
                        
                        // The socket could have a higher fd than the rest of the set
                        if (client_sock > max_fd) {
                            max_fd = client_sock;
                        }
                        
                    } else { // An existing connection submitted a command
                        char receivedCommand[MAX_DATA];
                        // Copy the command into the buffer
                        int bytes = recv(fd, receivedCommand, MAX_DATA, 0);
                        if (bytes <= 0) {
                            // We lost connection to the client
                            close(fd);
                            FD_CLR(fd, &client_cmd_set);
                            
                        } else {
                            // Create the reply and the token vector, delimited by spaces
                            struct Reply reply;
                            std::vector<CString> command_tokens;
                            char delimit[2] = " ";
                            char* token = strtok(receivedCommand, delimit);
                            
                            // Get all the possible tokens
                            while (token != NULL) {
                                struct CString str;
                                strcpy(str.string, token);
                                command_tokens.push_back(str);
                                
                                token = strtok(NULL, delimit);
                            }
                            
                            // No command has 2 arguments, it must be invalid
                            if (command_tokens.size() > 2) {
                                reply.status = FAILURE_INVALID;
                                send(fd, &reply, sizeof(reply), 0);
                            } else if (strncmp(command_tokens[0].string, "LIST", 4) == 0) {
                                // Process the LIST command
                                char temp_list[MAX_DATA] = "";
                                
                                // Go through all the chats and append their names
                                for (int i = 0; i < (signed) chats.size() - 1; i++) {
                                    strncat(temp_list, chats[i].name, 128);
                                    strncat(temp_list, ",", 1);
                                }
                                
                                // Treat the last element differently (don't end with a comma) 
                                // or we don't have any chats
                                if (chats.size() > 0) {
                                    strncat(temp_list, chats[chats.size() - 1].name, 128);
                                } else {
                                    strncat(temp_list, "empty", 5);
                                }

                                strncpy(reply.list_room, temp_list, MAX_DATA);
                                
                                reply.status = SUCCESS;
                                send(fd, &reply, sizeof(reply), 0);
                            } else { // Process commands that have 1 argument
                                if (command_tokens.size() < 2) {
                                    reply.status = FAILURE_INVALID;
                                    send(fd, &reply, sizeof(reply), 0);
                                } else if (strncmp(command_tokens[0].string, "CREATE", 6) == 0) {
                                    // Process CREATE
                                    // Given the name, loop through all of the chats, if we find one, 
                                    // mark its index and mark the boolean as true
                                    bool chatFound = false;
                                    int chatIndex = 0;
                                    for (int i = 0; i < chats.size(); i++) {
                                        if (strcmp(command_tokens[1].string, chats[i].name) == 0) {
                                            chatFound = true;
                                            chatIndex = i;
                                            break;
                                        }
                                    }
                                    
                                    // If we found a chat, we can't create another one with the same name
                                    if (chatFound) {
                                        reply.status = FAILURE_ALREADY_EXISTS;
                                        send(fd, &reply, sizeof(reply), 0);
                                    } else {
                                        // If we didn't find a chat, we must create a new one
                                        reply.status = SUCCESS;
                                        
                                        Chatroom chat;
                                        strncpy(chat.name, command_tokens[1].string, 128);  // Get the name from the user
                                        int chat_fd = CreateListener(port_offset); // Create a master listener port
                                        chat.listener_socket = chat_fd; // Assign the listener port
                                        chat.port_no = CHATROOM_BASE_PORT + port_offset - 1; // Mark down the port number
                                        FD_ZERO(&chat.clients); // Zero out the FDset of the chat and add the listener port to it
                                        FD_SET(chat_fd, &chat.clients);
                                        chat.maxfd = chat_fd; 
                                        chats.push_back(chat); // Add the chat to the vector of chats and send the reply
                                        
                                        send(fd, &reply, sizeof(reply), 0);
                                    }
                                } else if (strncmp(command_tokens[0].string, "JOIN", 4) == 0) {
                                    // Process JOIN
                                    // Same beginning procedure as with CREATE
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
                                        // We found the chat, send back its details
                                        reply.status = SUCCESS;
                                        reply.num_member = chats[chatIndex].num_members + 1; // Temporarily add one until we are for certain we received the connection
                                        reply.port = chats[chatIndex].port_no;
                                        send(fd, &reply, sizeof(reply), 0);
                                    } else {
                                        // Otherwise tell the user we don't have a chat
                                        reply.status = FAILURE_NOT_EXISTS;
                                        send(fd, &reply, sizeof(reply), 0);
                                    }
                                } else if (strncmp(command_tokens[0].string, "DELETE", 6) == 0) {
                                    // Process DELETE
                                    // Same beginning procedure as with CREATE
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
                                        // Call the delete chat routine which will close all the
                                        // sockets after broadcasting the kill message
                                        reply.status = SUCCESS;
                                        DeleteChat(chats[chatIndex]);
                                        chats.erase(chats.begin() + chatIndex);
                                        send(fd, &reply, sizeof(reply), 0);
                                    } else {
                                        reply.status = FAILURE_NOT_EXISTS;
                                        send(fd, &reply, sizeof(reply), 0);
                                    } 
                                } else {
                                    // We could not match any command
                                    reply.status = FAILURE_INVALID;
                                    send(fd, &reply, sizeof(reply), 0);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Process all the chats
        for (int i = 0; i < chats.size(); i++) {
            // Grab the fd_set of the nth chat
            fd_set active_chat_set = chats[i].clients;

            // Find which socket we have activity in
            if (select(chats[i].maxfd + 1, &active_chat_set, NULL, NULL, &timeout) > 0) {
                // Loop through all the possible clients
                for(int fd = 0; fd <= chats[i].maxfd; fd++) {
                    // Check if they're in our client set
                    if (FD_ISSET(fd, &active_chat_set)) {
                        // Handle new connections
                        if (fd == chats[i].listener_socket) {
                        
                            int client_sock;
                            struct sockaddr_in client;
                            socklen_t client_sock_sz = sizeof(struct sockaddr_in);
                            client_sock = accept(fd, (struct sockaddr*)&client, &client_sock_sz);
                            
                            FD_SET(client_sock, &chats[i].clients);
                            chats[i].num_members++; // Now we increment the member variable
                            
                            if (client_sock > chats[i].maxfd) {
                                chats[i].maxfd = client_sock;
                            }
                        
                        } else { // Someone sent a message and it needs to be broadcasted
                            char receivedMessage[MAX_DATA];
                            // Copy the message into the buffer
                            int bytes = recv(fd, receivedMessage, MAX_DATA, 0);
                            if (bytes <= 0) {
                                // Client disconnected, decrease membership
                                close(fd);
                                FD_CLR(fd, &chats[i].clients);
                                chats[i].num_members--;
                            } else {
                                // We want all messages to automatically end with a new line, but there are a few cases we need to consider
                                if (strlen(receivedMessage) == MAX_DATA) {
                                    // Message was larger than or equal to 256 bytes, replace the last two characters with new line and null terminator
                                    receivedMessage[strlen(receivedMessage) - 2] = '\n';
                                    receivedMessage[strlen(receivedMessage) - 1] = '\0';
                                } else if (strlen(receivedMessage) == MAX_DATA - 1) {
                                    // Message is exactly 255 bytes, apply the newline at index 254 and null terminator at 255
                                    receivedMessage[strlen(receivedMessage) - 1] = '\n';
                                    receivedMessage[strlen(receivedMessage)] = '\0';
                                } else {
                                    // Message is less than 255 bytes, apply the new line after the last character and the null
                                    // terminator right after it
                                    receivedMessage[strlen(receivedMessage)] = '\n';
                                    receivedMessage[strlen(receivedMessage) + 1] = '\0';
                                }
                                // Broadcast the message to all who can hear it
                                for (int fd2 = 0; fd2 <= chats[i].maxfd; fd2++) {
                                    if (FD_ISSET(fd2, &chats[i].clients)) {
                                        if (fd2 != chats[i].listener_socket && fd2 != fd) {
                                            if (send(fd2, receivedMessage, bytes, 0) < 0) {
                                                fprintf(stderr, "Failure sending message.\n");
                                                exit(1);
                                            }
                                        }
                                    }
                                } // End broadcast for loop
                            } // End process message
                        } // End process non-listener chat fd
                    } // End chat fdset check
                } // End chat fd loop
            } // End chat select
        } // End chat loop
    } // End while loop
    return 0;
}