#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fcntl.h>
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

#include <sys/stat.h>

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

class SNSServiceImpl final : public SNSService::Service {

  Status List(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------
    std::string username = request->username();

    mLock.lock();
    std::ifstream ifsUsersListing;
    ifsUsersListing.open(file_prefix + "users.ls");
    std::string sUser;

    while(getline(ifsUsersListing, sUser) && ifsUsersListing.is_open()) {
      reply->add_all_users(sUser);
    }

    ifsUsersListing.close();

    std::ifstream ifsUserFollowers(file_prefix + username + ".fl");
    std::string sUserFollower;

    while(std::getline(ifsUserFollowers, sUserFollower) && ifsUserFollowers.is_open()) {
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

    // Replicate the action on the slave
    if (this->server_type == snsCoordinator::MASTER) {
      grpc::ClientContext grpcMasterClientContext;
      csce438::Request rq = *request;
      csce438::Reply rep;
      this->slaveStub->Follow(&grpcMasterClientContext, rq, &rep);
    }

    std::string username = request->username();

    mLock.lock();

    // Only open the following file 
    std::ifstream ifsUserData(file_prefix + username + ".fg");
    std::ofstream ofsUserData;

    // Open the user listing
    std::ifstream ifsUsersListing;
    ifsUsersListing.open(file_prefix + "users.ls");
    std::string sUser;
    std::vector<std::string> vUserListing;

    while(getline(ifsUsersListing, sUser) && ifsUsersListing.is_open()) {
      vUserListing.push_back(sUser);
    }

    ifsUsersListing.close();

    // Check if the user-to-follow exists
    if (std::find(vUserListing.begin(), vUserListing.end(), request->arguments(0)) == vUserListing.end()) {
      ifsUserData.close();
      mLock.unlock();
      return Status(grpc::StatusCode::NOT_FOUND, "");
    }

    std::vector<std::string> vUserFollowing;
    std::string sUserFollowing;

    while (std::getline(ifsUserData, sUserFollowing) && ifsUserData.is_open()) {
      vUserFollowing.push_back(sUserFollowing);
    }

    ifsUserData.close();

    // Check if the user is already following the user-to-follow
    if (std::find(vUserFollowing.begin(), vUserFollowing.end(), request->arguments(0)) != vUserFollowing.end() || request->arguments(0) == username) {
      mLock.unlock();
      return Status(grpc::StatusCode::ALREADY_EXISTS, "");
    }
    
    // Append the new follower, the FS will pick this new follower up
    ofsUserData.open(file_prefix + username + ".fg", std::ios::app);
    ofsUserData << request->arguments(0) << std::endl;

    ofsUserData.close();
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
    std::ifstream ifsUserTempTimeline;

    std::ofstream ofsUserTimeline;
    std::ofstream ofsUserFollowers;
    std::ofstream ofsUserFollowing;
    std::ofstream ofsUserTempTimeline;

    ifsUserTimeline.open(file_prefix + username + ".tl");
    ifsUserTempTimeline.open(file_prefix + username + "_temp.tl");
    ifsUserFollowers.open(file_prefix + username + ".fl");
    ifsUserFollowing.open(file_prefix + username + ".fg");
    
    if (!ifsUserTimeline.is_open()) {
      ofsUserTimeline.open(file_prefix + username + ".tl");
      ofsUserListing << username << std::endl;
    }

    if (!ifsUserTempTimeline.is_open()) {
      ofsUserTempTimeline.open(file_prefix + username + "_temp.tl");
    }     

    if (!ifsUserFollowers.is_open()) {
      ofsUserFollowers.open(file_prefix + username + ".fl");
    }   

    if (!ifsUserFollowing.is_open()) {
      ofsUserFollowing.open(file_prefix + username + ".fg");
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

  Status ReplicaWriteTimeline(ServerContext* context, const Message* post, Reply* reply) override {
    std::ofstream ofsReplicaUserTempTimeline;
    int iUserID = std::stoi(post->username().substr(1, post->username().size() - 1));
    ofsReplicaUserTempTimeline.open(file_prefix + "u" + std::to_string(iUserID) + "_temp.tl", std::ios::app);
    ofsReplicaUserTempTimeline << post->username() << "%" << post->msg().substr(0, post->msg().size() - 1) << "%" << post->timestamp().seconds() << std::endl;

    return Status::OK;
  }

  Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // receiving a message/post from a user, recording it in a file
    // and then making it available on his/her follower's streams
    // ------------------------------------------------------------

    // This initial read gets us the user id
    Message snsPost;  
    stream->Read(&snsPost);
    int iUserID = std::stoi(snsPost.username().substr(1, snsPost.username().size() - 1));

    // Holds the initial posts from the context file
    std::vector<Message> vSNSPosts;
    bool stream_finished = false;

    // Load the most recent 20 posts
    mLock.lock();
    std::ifstream ifsUserTimeline(file_prefix + snsPost.username() + ".tl");
    std::string sPost;
    while (getline(ifsUserTimeline, sPost)) {
      // Split the line into individual pieces
      std::string sPostUsername;
      std::string sPostMessage;
      std::string sPostTimestamp;
      google::protobuf::int64 tsPostTime;

      std::stringstream ssUserPostParser(sPost);
      getline(ssUserPostParser, sPostUsername, '%');
      getline(ssUserPostParser, sPostMessage, '%');
      getline(ssUserPostParser, sPostTimestamp, '%');
      // Append the newline character so the client prints it
      sPostMessage.append("\n");

      tsPostTime = strtoll(sPostTimestamp.data(), NULL, 10);   

      snsPost.set_username(sPostUsername);
      snsPost.set_msg(sPostMessage);
      snsPost.mutable_timestamp()->set_seconds(google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime).seconds());

      vSNSPosts.push_back(snsPost);
    }
    ifsUserTimeline.close();
    mLock.unlock();

    // Write the most recent posts to the screen
    size_t counter = 0;
    while (!vSNSPosts.empty() && counter < 20) {
      stream->Write(*(vSNSPosts.end() - 1));
      vSNSPosts.pop_back();
      counter++;
    }

    // Open the temp timeline of the master and/or server
    std::ofstream ofsUserTempTimeline;
    ofsUserTempTimeline.open(file_prefix + "u" + std::to_string(iUserID) + "_temp.tl", std::ios::app);
 
    std::string timeline_context_file = file_prefix + "u" + std::to_string(iUserID) + ".tl";
    std::thread tRelayTimeline(&SNSServiceImpl::RelayTimeline, this, timeline_context_file, stream, std::ref(stream_finished));

    // Write the user's post to their own temp file
    while (stream->Read(&snsPost)) {
      ofsUserTempTimeline << snsPost.username() << "%" << snsPost.msg().substr(0, snsPost.msg().size() - 1) << "%" << snsPost.timestamp().seconds() << std::endl;
      if (server_type == snsCoordinator::ServerType::MASTER) {
        grpc::ClientContext grpcMasterClientContext;
        csce438::Message post = snsPost;
        csce438::Reply rep;
        this->slaveStub->ReplicaWriteTimeline(&grpcMasterClientContext, post, &rep);
      }
    }

    // If the client quits, close the context files
    ofsUserTempTimeline.close();

    tRelayTimeline.join();

    return Status::OK;
  }

  // This method checks every thirty seconds and sees if a temporary timeline file has been modified in the period
  // between the last time check and now
  void RelayTimeline(std::string timeline_context_file_name, ServerReaderWriter<Message, Message>* stream, bool& stream_finished) {
    // Initial stat
    struct stat st;
    stat(timeline_context_file_name.c_str(), &st);
    struct timeval current_time;
    struct timespec modified_time = st.st_mtim;
    gettimeofday(&current_time, NULL);

    // Indefinitely check until the readerwriter stream is closed
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(25));
      stat(timeline_context_file_name.c_str(), &st);
      gettimeofday(&current_time, NULL);
      modified_time = st.st_mtim;
      // This value is always positive, just need to check if its less than 30sec
      if ((current_time.tv_sec - modified_time.tv_sec) < 30) {
        // Similar to initial timeline setup
        std::vector<Message> vSNSPosts;
        Message snsPost;
        mLock.lock();
        std::ifstream ifsUserTimeline(timeline_context_file_name);
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

        // If the stream is finished, we cannot write to it, exit the thread and join the parent thread
        if (!stream_finished) {
          while (!vSNSPosts.empty()) {
            stream->Write(*(vSNSPosts.end() - 1));
            vSNSPosts.pop_back();
          }
        } else {
          break;
        }
      }
    }
  }

public:
  std::vector<std::string> vUsers;
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
  
  // Setting identification info
  Service.server_id = server_id;
  Service.port_no = port_no;
  Service.server_type = sv_type;

  // This helps reduce the amount of literals present in code
  if (sv_type == snsCoordinator::MASTER) {
    Service.file_prefix = "master" + std::to_string(server_id) + "_";
  } else {
    Service.file_prefix = "slave" + std::to_string(server_id) + "_";
  }

  // Setting up the stubs
  Service.coordChannel = grpc::CreateChannel(coordinator_ip + ":" + coordinator_port, grpc::InsecureChannelCredentials());
  Service.coordStub = snsCoordinator::SNSCoordinator::NewStub(Service.coordChannel);

  // Send an initial heartbeat to register the service
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

  // Get the slave stub from the coordinator
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

  // Start a thread that sends heartbeats
  std::thread tHeartbeatThread(&SNSServiceImpl::SendHeartbeat, &Service);
  grpcServer->Wait();
}

int main(int argc, char** argv) {
  std::string port = "6610";
  std::string coord_ip = "localhost";
  std::string coord_port = "9000";
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
