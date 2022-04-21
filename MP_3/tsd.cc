#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <thread>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "snsCoordinator.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

typedef struct UserStream {
  std::string sUsername;
  ServerReaderWriter<Message, Message>* srwStream;
} UserStream;

bool operator==(const UserStream& usLeft, const UserStream& usRight) {
  return usLeft.srwStream == usRight.srwStream;
}

class SNSServiceImpl final : public SNSService::Service {

  Status List(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------
    std::string username = request->username();

    mLock.lock();
    // Todo change this
    //for (size_t i = 0; i < vUsers.size(); i++) {
    //  reply->add_all_users(vUsers[i]);
    //}

    std::ifstream ifsUsersListing;
    ifsUsersListing.open(file_prefix + "users.ls");
    std::string sUser;

    std::vector<std::string> vUsersFromFile;

    while(getline(ifsUsersListing, sUser) && ifsUsersListing.is_open()) {
      vUsersFromFile.push_back(sUser);
    }

    ifsUsersListing.close();

    std::ifstream ifsUserFollowers(file_prefix + username + ".fl");
    std::string sUserFollowers;

    if (!std::getline(ifsUserFollowers, sUserFollowers)) {
      sUserFollowers = "";
    }

    std::stringstream ss(sUserFollowers);
    std::string sUserFollower;

    while (ss >> sUserFollower) {
      reply->add_following_users(sUserFollower);
    }
    mLock.unlock();

    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // request from a user to follow one of the existing
    // users
    // ------------------------------------------------------------
    if (this->server_type == snsCoordinator::MASTER) {
      grpc::ClientContext grpcMasterClientContext;
      csce438::Request rq = *request;
      csce438::Reply rep;
      this->slaveStub->Follow(&grpcMasterClientContext, rq, &rep);
    }

    std::string username = request->username();

    mLock.lock();

    std::ifstream ifsUserData(file_prefix + username + ".fg");
    std::ofstream ofsUserData;

    // TODO: Change this
    if (std::find(vUsers.begin(), vUsers.end(), request->arguments(0)) == vUsers.end()) {
      ifsUserData.close();
      mLock.unlock();
      return Status(grpc::StatusCode::NOT_FOUND, "");
    }

    /* TODO: Change this to use fg file
    std::fstream fsTargetUserData(file_prefix + request->arguments(0) + ".fl", std::ios::out | std::ios::app);

    std::string sUserFollowers;
    std::string sUserFollower;
    std::vector<std::string> vUserFollowers;

    if (!std::getline(ifsUserData, sUserFollowers)) {
      sUserFollowers = "";
    }

    ifsUserData.close();

    std::stringstream ssFollowerParser(sUserFollowers);

    while (ssFollowerParser >> sUserFollower) {
      vUserFollowers.push_back(sUserFollower);
    }

    if (std::find(vUserFollowers.begin(), vUserFollowers.end(), request->arguments(0)) != vUserFollowers.end()) {
      mLock.unlock();
      return Status(grpc::StatusCode::ALREADY_EXISTS, "");
    }
    */

    ofsUserData.open(file_prefix + username + ".fg");
    // sUserFollowers.append(request->arguments(0) + " ");

    // fsTargetUserData << username << " ";
    // ofsUserData << sUserFollowers << std::endl;

    ofsUserData.close();
    //fsTargetUserData.close();

    mLock.unlock();

    return Status::OK; 
  }
  
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // a new user and verify if the username is available
    // or already taken
    // ------------------------------------------------------------
    if (this->server_type == snsCoordinator::MASTER) {
      grpc::ClientContext grpcMasterClientContext;
      csce438::Request rq = *request;
      csce438::Reply rep;
      this->slaveStub->Login(&grpcMasterClientContext, rq, &rep);
    }

    mLock.lock();
    
    std::string username = request->username();
    std::ofstream ofsUserListing(file_prefix + "users.ls", std::ios::app);

    std::ifstream ifsUserTimeline;
    std::ifstream ifsUserFollowers;
    std::ifstream ifsUserFollowing;

    std::ofstream ofsUserTimeline;
    std::ofstream ofsUserFollowers;
    std::ofstream ofsUserFollowing;

    ifsUserTimeline.open(file_prefix + username + ".tl");
    ifsUserFollowers.open(file_prefix + username + ".fl");
    ifsUserFollowing.open(file_prefix + username + ".fg");
    
    if (!ifsUserTimeline.is_open()) {
      ofsUserTimeline.open(file_prefix + username + ".tl");
    }   

    if (!ifsUserFollowers.is_open()) {
      ofsUserFollowers.open(file_prefix + username + ".fl");
      ofsUserFollowers << username << " ";
    }   

    if (!ifsUserFollowing.is_open()) {
      ofsUserFollowing.open(file_prefix + username + ".fg");
      ofsUserFollowing << username << " ";
    }   

    if (std::find(vUsers.begin(), vUsers.end(), username) == vUsers.end()) {
      vUsers.push_back(username);
      ofsUserListing << username << std::endl;
    }

    ofsUserTimeline.close();
    ofsUserFollowers.close();
    ofsUserFollowing.close();

    ifsUserTimeline.close();
    ifsUserFollowers.close();
    ifsUserFollowing.close();

    ofsUserListing.close();

    mLock.unlock();
    return Status::OK;
  }

  Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // receiving a message/post from a user, recording it in a file
    // and then making it available on his/her follower's streams
    // ------------------------------------------------------------
    Message snsPost;  
    stream->Read(&snsPost);

    UserStream usClient;
    usClient.sUsername = snsPost.username();
    usClient.srwStream = stream;
    std::vector<std::string> vFollowers;
    std::vector<Message> vSNSPosts;

    mLock.lock();
    std::ifstream ifsUserTimeline(file_prefix + usClient.sUsername + ".tl");
    std::string sPost;
    while (getline(ifsUserTimeline, sPost)) {
      std::string sPostUsername;
      std::string sPostMessage;
      std::string sPostTimestamp;
      google::protobuf::int64 tsPostTime;

      std::stringstream ssUserPostParser(sPost);
      getline(ssUserPostParser, sPostUsername, '%');
      getline(ssUserPostParser, sPostMessage, '%');
      getline(ssUserPostParser, sPostTimestamp, '%');
      sPostMessage.append("\n");

      tsPostTime = strtoll(sPostTimestamp.data(), NULL, 10);   

      snsPost.set_username(sPostUsername);
      snsPost.set_msg(sPostMessage);
      snsPost.mutable_timestamp()->set_seconds(google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime).seconds());

      vSNSPosts.push_back(snsPost);
    }
    ifsUserTimeline.close();
    mLock.unlock();

    size_t counter = 0;
    while (!vSNSPosts.empty() && counter < 20) {
      stream->Write(*(vSNSPosts.end() - 1));
      vSNSPosts.pop_back();
      counter++;
    }

    mStreamLock.lock();
    vStreams.push_back(usClient);
    mStreamLock.unlock();

    mLock.lock();
    std::ifstream ifsUserFollowers(file_prefix + usClient.sUsername + ".fl");
    std::string sUserFollowers, sUserFollower;
    getline(ifsUserFollowers, sUserFollowers);
    std::stringstream ssUserFollowerParser(sUserFollowers);
    while (ssUserFollowerParser >> sUserFollower) {
      vFollowers.push_back(sUserFollower);
    }
    ifsUserFollowers.close();
    mLock.unlock();
 
    while (stream->Read(&snsPost)) {
      mLock.lock();
      vFollowers.clear();
      std::ifstream ifsUserFollowers(file_prefix + usClient.sUsername + ".fl");
      std::string sUserFollowers, sUserFollower;
      getline(ifsUserFollowers, sUserFollowers);
      std::stringstream ssUserFollowerParser(sUserFollowers);
      while (ssUserFollowerParser >> sUserFollower) {
        vFollowers.push_back(sUserFollower);
      }
      ifsUserFollowers.close();
      mLock.unlock();

      for (UserStream usTarget : vStreams) {
        std::vector<std::string>::iterator itUsernamePointer;
        itUsernamePointer = std::find(vFollowers.begin(), vFollowers.end(), usTarget.sUsername);
        if ((stream != usTarget.srwStream) && (itUsernamePointer != vFollowers.end())) {
          usTarget.srwStream->Write(snsPost);
        }
      }

      mLock.lock();
      for (std::string sFollower : vFollowers) {
        std::ofstream ofsTimelineWriter(file_prefix + sFollower + ".tl", std::ios::app);
        ofsTimelineWriter << snsPost.username() << "%" << snsPost.msg().substr(0, snsPost.msg().size() - 1) << "%" << snsPost.timestamp().seconds() << std::endl;
        ofsTimelineWriter.close();
      }
      mLock.unlock();
    }

    mStreamLock.lock();
    std::vector<UserStream>::iterator itStreamPointerPosition;
    itStreamPointerPosition = std::find(vStreams.begin(), vStreams.end(), usClient);
    vStreams.erase(itStreamPointerPosition);
    mStreamLock.unlock();
    return Status::OK;
  }

public:
  std::vector<std::string> vUsers;
  std::vector<UserStream> vStreams;
  std::mutex mStreamLock;
  std::mutex mLock;

  std::unique_ptr<grpc::ClientReaderWriter<snsCoordinator::Heartbeat, snsCoordinator::Heartbeat>> cReaderWriter;
  std::shared_ptr<grpc::Channel> coordChannel;
  std::unique_ptr<snsCoordinator::SNSCoordinator::Stub> coordStub;
  snsCoordinator::ServerType server_type;

  std::unique_ptr<csce438::SNSService::Stub> slaveStub;
  std::shared_ptr<grpc::Channel> slaveChannel;

  int server_id;
  std::string port_no;
  std::string file_prefix;

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
};

void RunServer(std::string coordinator_ip, std::string coordinator_port, 
              std::string port_no, int server_id, snsCoordinator::ServerType sv_type) {
  // ------------------------------------------------------------
  // In this function, you are to write code 
  // which would start the server, make it listen on a particular
  // port number.
  // ------------------------------------------------------------
  std::string sServerAddress("localhost:" + port_no);
  SNSServiceImpl Service;
  Service.server_id = server_id;
  Service.port_no = port_no;
  Service.server_type = sv_type;

  if (sv_type == snsCoordinator::MASTER) {
    Service.file_prefix = "master" + std::to_string(server_id) + "_";
  } else {
    Service.file_prefix = "slave" + std::to_string(server_id) + "_";
  }

  /*
  std::ifstream ifsUsersListing;
  ifsUsersListing.open(Service.file_prefix + "users.ls");
  std::string sUser;

  while(getline(ifsUsersListing, sUser) && ifsUsersListing.is_open()) {
    Service.vUsers.push_back(sUser);
  }

  ifsUsersListing.close();
  */

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

  if (Service.server_type == snsCoordinator::MASTER) {
    grpc::ClientContext grpcCoordContext;
    snsCoordinator::Request req;
    snsCoordinator::Reply rep;

    req.set_coordinatee(snsCoordinator::SERVER);
    req.set_server_type(snsCoordinator::MASTER);
    req.set_id(server_id);

    Service.coordStub->Login(&grpcCoordContext, req, &rep);

    Service.slaveChannel = grpc::CreateChannel(rep.msg(), grpc::InsecureChannelCredentials());
    Service.slaveStub = csce438::SNSService::NewStub(Service.slaveChannel);
  }

  ServerBuilder builder;
  builder.AddListeningPort(sServerAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(&Service);
  std::unique_ptr<Server> grpcServer(builder.BuildAndStart());

  std::thread tHeartbeatThread(&SNSServiceImpl::SendHeartbeat, &Service);
  grpcServer->Wait();
}

int main(int argc, char** argv) {
  std::string port = "3010";
  std::string coord_ip = "localhost";
  std::string coord_port = "9090";
  snsCoordinator::ServerType server_type = snsCoordinator::MASTER;
  int server_id = 1;
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:d:p:i:t:")) != -1){
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
      case 't':
          if (strcmp(optarg, "master") == 0) {
            server_type = snsCoordinator::MASTER;
          } else {
            server_type = snsCoordinator::SLAVE;
          }
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(coord_ip, coord_port, port, server_id, server_type);
  return 0;
}
