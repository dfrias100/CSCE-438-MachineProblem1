#include <iostream>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "snsCoordinator.grpc.pb.h"
#include "snsFollowSync.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

using snsFollowSync::SNSFollowSync;
using snsFollowSync::Relat;
using snsFollowSync::User;
using snsFollowSync::Post;
using snsFollowSync::Reply;

class SNSFollowSyncImpl final : public SNSFollowSync::Service {
    Status SyncUsers(ServerContext* context, const User* user, Reply* reply) override {
        return Status::OK;
    }

    Status SyncFollow(ServerContext* context, const Relat* relat, Reply* reply) override {
        return Status::OK;
    }

    Status SyncTimeline(ServerContext* context, const Post* user, Reply* reply) override {
        return Status::OK;
    }
};

void RunServer(std::string coordinator_ip, std::string coordinator_port, 
              std::string port_no, int server_id, snsCoordinator::ServerType sv_type) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSFollowSyncImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  std::string port = "7000";
  std::string coord_ip = "localhost";
  std::string coord_port = "9090";
  int server_id = 1;
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:d:p:i:")) != -1){
    switch(opt) {
      case 'c':
          coord_ip = optarg;
          break;
      case 'd':
          coord_port = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      case 'i':
          server_id = atoi(optarg);
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(coord_ip, coord_port, port, server_id, snsCoordinator::SYNC);
  return 0;
}