#include <iostream>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fcntl.h>
#include <thread>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <sys/stat.h>

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
    const std::string master_prefix = "master";
    const std::string slave_prefix = "slave";
    int cluster_id;
    Status SyncUsers(ServerContext* context, const User* user, Reply* reply) override {
        std::cout << "Sync users called!" << std::endl;
        std::string master_users_filename = master_prefix + std::to_string(server_id) + "_users.ls";
        std::string slave_users_filename = slave_prefix + std::to_string(server_id) + "_users.ls";

        std::ofstream master_users_file;
        master_users_file.open(master_users_filename, std::ios::app);

        User copy_user = *user;
        for (int id : *copy_user.mutable_src_user_id()) {
            master_users_file << "u" + std::to_string(id) << std::endl;
        }

        std::ofstream slave_users_file;
        slave_users_file.open(slave_users_filename, std::ios::app);

        copy_user = *user;
        users_lock.lock();
        for (int id : *copy_user.mutable_src_user_id()) {
            slave_users_file << "u" + std::to_string(id) << std::endl;
            user_ids[id] = id;
        }
        users_lock.unlock();

        return Status::OK;
    }

    Status SyncFollow(ServerContext* context, const Relat* relat, Reply* reply) override {
        return Status::OK;
    }

    Status SyncTimeline(ServerContext* context, const Post* user, Reply* reply) override {
        return Status::OK;
    }

    public:
    std::unique_ptr<grpc::ClientReaderWriter<snsCoordinator::Heartbeat, snsCoordinator::Heartbeat>> cReaderWriter;
    std::shared_ptr<grpc::Channel> coordChannel;
    
    std::unique_ptr<snsCoordinator::SNSCoordinator::Stub> coordStub;
    snsCoordinator::ServerType server_type;

    std::unordered_map<int, std::shared_ptr<grpc::Channel>> followSyncChannels;
    std::unordered_map<int, std::shared_ptr<snsFollowSync::SNSFollowSync::Stub>> followSyncStubs;  

    int server_id;
    std::string port_no;

    std::unordered_map<int, int> user_ids;

    std::mutex users_lock;

    void SendHeartbeat() {
        while(true) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            snsCoordinator::Heartbeat hb;
            hb.set_server_id(server_id);
            hb.set_server_type(server_type);
            hb.set_server_port(port_no);

            time_t tsPostTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            auto gptPostTime = google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime);
            hb.mutable_timestamp()->set_seconds(gptPostTime.seconds());
            cReaderWriter->Write(hb);
        }
    }

    void CheckUsers() {
        struct stat sfile;
        std::string users_file = "slave" + std::to_string(server_id) + "_users.ls";
        stat(users_file.c_str(), &sfile);
        off_t current_file_size = sfile.st_size;
        off_t last_file_size = sfile.st_size;

        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            std::cout << "Pinging users file on cluster " << server_id << std::endl;
            stat(users_file.c_str(), &sfile);
            current_file_size = sfile.st_size;
            
            if (current_file_size > last_file_size) {
                std::cout << "Cluster " << server_id << " users file has changed." << std::endl;
                users_lock.lock();
                std::ifstream users_file_stream;
                users_file_stream.open(users_file);
                
                User users;

                std::vector<int> iUsers;
                std::string sUser;

                users_file_stream.seekg(last_file_size);

                while(getline(users_file_stream, sUser) && users_file_stream.is_open()) {
                    iUsers.push_back(std::stoi(sUser.substr(1, sUser.size() - 1)));
                }

                for (int i : iUsers) {
                    if (auto u = user_ids.find(i); u == user_ids.end()) {
                        users.add_src_user_id(i);
                        user_ids[i] = i;
                    }
                }
                users_lock.unlock();

                if (users.src_user_id_size() > 0) {
                    for (auto follow_stub : followSyncStubs) {
                        grpc::ClientContext grpcFollowSyncContext;
                        snsFollowSync::Reply reply;
                        follow_stub.second->SyncUsers(&grpcFollowSyncContext, users, &reply);
                    }
                }
            }
            
            last_file_size = current_file_size;
        }
    }
};

void RunServer(std::string coordinator_ip, std::string coordinator_port, 
              std::string port_no, int server_id, snsCoordinator::ServerType sv_type) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSFollowSyncImpl Service;
  Service.server_id = server_id;
  Service.port_no = port_no;
  Service.server_type = sv_type;

  std::ifstream user_listing;
  user_listing.open("slave" + std::to_string(server_id) + "_users.ls");
  std::string user;
  
  while (user_listing >> user) {
      Service.user_ids[std::stoi(user.substr(1, user.size() - 1))] = std::stoi(user.substr(1, user.size() - 1));
  }

  Service.coordChannel = grpc::CreateChannel(coordinator_ip + ":" + coordinator_port, grpc::InsecureChannelCredentials());
  Service.coordStub = snsCoordinator::SNSCoordinator::NewStub(Service.coordChannel);

  grpc::ClientContext grpcCoordContext;
  snsCoordinator::Heartbeat hb;
  hb.set_server_id(server_id);
  hb.set_server_type(sv_type);
  hb.set_server_port(port_no);

  time_t tsPostTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  auto gptPostTime = google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime);
  hb.mutable_timestamp()->set_seconds(gptPostTime.seconds());

  Service.cReaderWriter = Service.coordStub->ServerCommunicate(&grpcCoordContext);
  Service.cReaderWriter->Write(hb);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&Service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  {
      grpc::ClientContext grpcCoordContext;
      snsCoordinator::Request rq;
      snsCoordinator::ConnectionPoints conn_pts;
      rq.set_coordinatee(snsCoordinator::Coordinatee::SERVER);
      rq.set_server_type(snsCoordinator::ServerType::COORD);
      rq.set_id(server_id);
      Service.coordStub->ReturnFollowerSync(&grpcCoordContext, rq, &conn_pts);

      for (int i = 0; i < conn_pts.cluster_ids_size(); i++) {
          Service.followSyncChannels[conn_pts.cluster_ids(i)] = grpc::CreateChannel(conn_pts.connection_points(i), grpc::InsecureChannelCredentials());
          Service.followSyncStubs[conn_pts.cluster_ids(i)] = SNSFollowSync::NewStub(Service.followSyncChannels[conn_pts.cluster_ids(i)]);
      }
  }

  std::thread tHeartbeatThread(&SNSFollowSyncImpl::SendHeartbeat, &Service);
  std::thread tCheckUsersThread(&SNSFollowSyncImpl::CheckUsers, &Service);
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