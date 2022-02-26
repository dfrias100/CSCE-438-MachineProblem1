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
    for (size_t i = 0; i < vUsers.size(); i++) {
      reply->add_all_users(vUsers[i]);
    }

    std::ifstream ifsUserFollowers(username + ".fl");
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
    std::string username = request->username();

    mLock.lock();

    std::ifstream ifsUserData(username + ".fg");
    std::ofstream ofsUserData;

    if (std::find(vUsers.begin(), vUsers.end(), request->arguments(0)) == vUsers.end()) {
      ifsUserData.close();
      mLock.unlock();
      return Status(grpc::StatusCode::NOT_FOUND, "");
    }

    std::fstream fsTargetUserData(request->arguments(0) + ".fl", std::ios::out | std::ios::app);

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

    ofsUserData.open(username + ".fg");
    sUserFollowers.append(request->arguments(0) + " ");

    fsTargetUserData << username << " ";
    ofsUserData << sUserFollowers << std::endl;

    ofsUserData.close();
    fsTargetUserData.close();

    mLock.unlock();

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // request from a user to unfollow one of his/her existing
    // followers
    // ------------------------------------------------------------
    std::string username = request->username();

    mLock.lock();

    std::ifstream ifsUserData(username + ".fg");
    std::ofstream ofsUserData;

    if (username == request->arguments(0) || std::find(vUsers.begin(), vUsers.end(), request->arguments(0)) == vUsers.end()) {
      ifsUserData.close();
      mLock.unlock();
      return Status(grpc::StatusCode::NOT_FOUND, "");
    }

    // ------------------------------------------------ //

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

    if (std::find(vUserFollowers.begin(), vUserFollowers.end(), request->arguments(0)) == vUserFollowers.end()) {
      mLock.unlock();
      return Status(grpc::StatusCode::NOT_FOUND, "");
    }

    // --------------------------------------------------- //

    std::ifstream ifsTargetUserData(request->arguments(0) + ".fl");
    std::ofstream ofsTargetUserData;

    std::string sTargetUserFollowers;
    std::string sTargetUserFollower;
    std::vector<std::string> vTargetUserFollowers;

    if (!std::getline(ifsTargetUserData, sTargetUserFollowers)) {
      sTargetUserFollowers = "";
    }

    ifsTargetUserData.close();

    ssFollowerParser = std::stringstream(sTargetUserFollowers);

    while (ssFollowerParser >> sTargetUserFollower) {
      vTargetUserFollowers.push_back(sTargetUserFollower);
    }

    std::vector<std::string>::iterator spTargetUserEntry = std::find(vTargetUserFollowers.begin(), vTargetUserFollowers.end(), username);
    vTargetUserFollowers.erase(spTargetUserEntry);

    // ------------------------------------------- //

    std::vector<std::string>::iterator spUserFollowingEntry = std::find(vUserFollowers.begin(), vUserFollowers.end(), request->arguments(0));
    vUserFollowers.erase(spUserFollowingEntry);

    ofsUserData.open(username + ".fg");
    ofsTargetUserData.open(request->arguments(0) + ".fl");

    sUserFollowers = "";
    for (size_t i = 0; i < vUserFollowers.size(); i++) {
      sUserFollowers += vUserFollowers[i] + " ";
    }
    ofsUserData << sUserFollowers;

    sTargetUserFollowers = "";
    for (size_t i = 0; i < vTargetUserFollowers.size(); i++) {
      sTargetUserFollowers += vTargetUserFollowers[i] + " ";
    }
    ofsTargetUserData << sTargetUserFollowers;

    ofsUserData.close();
    ofsTargetUserData.close();

    mLock.unlock();

    return Status::OK;
  }
  
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // a new user and verify if the username is available
    // or already taken
    // ------------------------------------------------------------
    mLock.lock();
    
    std::string username = request->username();
    std::ofstream ofsUserListing("users.ls", std::ios::app);

    std::ifstream ifsUserTimeline;
    std::ifstream ifsUserFollowers;
    std::ifstream ifsUserFollowing;

    std::ofstream ofsUserTimeline;
    std::ofstream ofsUserFollowers;
    std::ofstream ofsUserFollowing;

    ifsUserTimeline.open(username + ".tl");
    ifsUserFollowers.open(username + ".fl");
    ifsUserFollowing.open(username + ".fg");
    
    if (!ifsUserTimeline.is_open()) {
      ofsUserTimeline.open(username + ".tl");
    }   

    if (!ifsUserFollowers.is_open()) {
      ofsUserFollowers.open(username + ".fl");
      ofsUserFollowers << username << " ";
    }   

    if (!ifsUserFollowing.is_open()) {
      ofsUserFollowing.open(username + ".fg");
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
    std::ifstream ifsUserTimeline(usClient.sUsername + ".tl");
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

    size_t counter = 0
    while (!vSNSPosts.empty() && counter < 20) {
      stream->Write(*(vSNSPosts.end() - 1));
      vSNSPosts.pop_back();
      counter++;
    }

    mStreamLock.lock();
    vStreams.push_back(usClient);
    mStreamLock.unlock();

    mLock.lock();
    std::ifstream ifsUserFollowers(usClient.sUsername + ".fl");
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
      std::ifstream ifsUserFollowers(usClient.sUsername + ".fl");
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
        if (sFollower != usClient.sUsername) {
          std::ofstream ofsTimelineWriter(sFollower + ".tl", std::ios::app);
          ofsTimelineWriter << snsPost.username() << "%" << snsPost.msg().substr(0, snsPost.msg().size() - 1) << "%" << snsPost.timestamp().seconds() << std::endl;
          ofsTimelineWriter.close();
        }
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
};

void RunServer(std::string port_no) {
  // ------------------------------------------------------------
  // In this function, you are to write code 
  // which would start the server, make it listen on a particular
  // port number.
  // ------------------------------------------------------------
  std::string sServerAddress("localhost:" + port_no);
  SNSServiceImpl Service;

  std::ifstream ifsUsersListing;
  ifsUsersListing.open("users.ls");
  std::string sUser;

  while(getline(ifsUsersListing, sUser) && ifsUsersListing.is_open()) {
    Service.vUsers.push_back(sUser);
  }

  ifsUsersListing.close();

  ServerBuilder builder;
  builder.AddListeningPort(sServerAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(&Service);
  std::unique_ptr<Server> grpcServer(builder.BuildAndStart());
  grpcServer->Wait();
}

int main(int argc, char** argv) {
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}
