#include <iostream>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fcntl.h>
#include <thread>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <sys/stat.h>

#include "sns.grpc.pb.h"
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

const std::string master_prefix = "master";
const std::string slave_prefix = "slave";

// The statmap stores context file hash_maps to allow for quick lookup of files and has
// methods to simply the addition of users and stat-ing of files
struct StatMap {
    std::unordered_map<int, std::pair<std::string, std::string>> cluster_file_names;
    std::map<int, struct stat> stat_map;
    // This is a map of "current size" and "last recorded size" variables; the cluster id is the key
    std::map<int, std::pair<off_t, off_t>> current_last_map;
    int server_id; 
    std::string file_extension;

    inline void AddClient(int id) {
        std::string file_suffix = std::to_string(server_id) + "_u" + std::to_string(id) + file_extension;
        cluster_file_names[id] = std::pair<std::string, std::string>(master_prefix + file_suffix, slave_prefix + file_suffix);          
    }

    inline void StatFile(int id) {
        stat(cluster_file_names[id].second.c_str(), &stat_map[id]);
        current_last_map[id].first = stat_map[id].st_size;
    }
};

class SNSFollowSyncImpl final : public SNSFollowSync::Service {
    // Adds the user to a listing
    Status SyncUsers(ServerContext* context, const User* user, Reply* reply) override {
        std::string master_users_filename = master_prefix + std::to_string(server_id) + "_users.ls";
        std::string slave_users_filename = slave_prefix + std::to_string(server_id) + "_users.ls";

        std::ofstream users_file;
        users_file.open(master_users_filename, std::ios::app);

        User copy_user = *user;
        for (int id : *copy_user.mutable_src_user_id()) {
            users_file << "u" + std::to_string(id) << std::endl;
        }
        users_file.close();

        users_file.open(slave_users_filename, std::ios::app);

        // Here we'll add the user to the user_id hash map
        copy_user = *user;
        users_lock.lock();
        for (int id : *copy_user.mutable_src_user_id()) {
            users_file << "u" + std::to_string(id) << std::endl;
            user_ids[id] = id;
        }
        users_lock.unlock();

        return Status::OK;
    }

    // SyncFollow and SyncTimeline open and modify the master & slave files of the respective data 
    Status SyncFollow(ServerContext* context, const Relat* relat, Reply* reply) override {        
        std::string master_follower_file_name = master_prefix + std::to_string(server_id) + "_u" + std::to_string(relat->dest_user_id()) + ".fl";
        std::string slave_follower_file_name = slave_prefix + std::to_string(server_id) + "_u" + std::to_string(relat->dest_user_id()) + ".fl";

        user_follower_context_lock.lock();
        std::ofstream master_follower_file_stream;
        master_follower_file_stream.open(master_follower_file_name, std::ios::app);
        master_follower_file_stream << std::string("u" + std::to_string(relat->src_user_id())) << std::endl;
        master_follower_file_stream.close();

        std::ofstream slave_follower_file_stream;
        slave_follower_file_stream.open(slave_follower_file_name, std::ios::app);
        slave_follower_file_stream << std::string("u" + std::to_string(relat->src_user_id())) << std::endl;
        slave_follower_file_stream.close();
        user_follower_context_lock.unlock();
        
        return Status::OK;
    }

    Status SyncTimeline(ServerContext* context, const Post* post, Reply* reply) override {
        int target_id = post->dest_user_id();
        std::string master_timeline_context = master_prefix + std::to_string(server_id) + "_u" + std::to_string(target_id) + ".tl";
        std::string slave_timeline_context = slave_prefix + std::to_string(server_id) + "_u" + std::to_string(target_id) + ".tl";

        user_timeline_context_lock.lock();
        std::ofstream target_user_timeline_out;
        target_user_timeline_out.open(master_timeline_context, std::ios::app);
        target_user_timeline_out << ("u" + std::to_string(post->src_user_id())) << "%" << post->msg().substr(0, post->msg().size() - 1) << "%" << post->timestamp().seconds() << std::endl;
        target_user_timeline_out.close();

        target_user_timeline_out.open(slave_timeline_context, std::ios::app);
        target_user_timeline_out << ("u" + std::to_string(post->src_user_id())) << "%" << post->msg().substr(0, post->msg().size() - 1) << "%" << post->timestamp().seconds() << std::endl;
        target_user_timeline_out.close();
        user_timeline_context_lock.unlock();

        return Status::OK;
    }

    bool LocalTimelineWrite(const Post& post) {
        int target_id = post.dest_user_id();
        std::string master_timeline_context = master_prefix + std::to_string(server_id) + "_u" + std::to_string(target_id) + ".tl";
        std::string slave_timeline_context = slave_prefix + std::to_string(server_id) + "_u" + std::to_string(target_id) + ".tl";

        user_timeline_context_lock.lock();
        std::ofstream target_user_timeline_out;
        target_user_timeline_out.open(master_timeline_context, std::ios::app);
        target_user_timeline_out << ("u" + std::to_string(post.src_user_id())) << "%" << post.msg().substr(0, post.msg().size() - 1) << "%" << post.timestamp().seconds() << std::endl;
        target_user_timeline_out.close();

        target_user_timeline_out.open(slave_timeline_context, std::ios::app);
        target_user_timeline_out << ("u" + std::to_string(post.src_user_id())) << "%" << post.msg().substr(0, post.msg().size() - 1) << "%" << post.timestamp().seconds() << std::endl;
        target_user_timeline_out.close();
        user_timeline_context_lock.unlock();

        return true;
    }

    bool LocalFollowWrite(const Relat& relat) {
        std::string master_follower_file_name = master_prefix + std::to_string(server_id) + "_u" + std::to_string(relat.dest_user_id()) + ".fl";
        std::string slave_follower_file_name = slave_prefix + std::to_string(server_id) + "_u" + std::to_string(relat.dest_user_id()) + ".fl";

        user_follower_context_lock.lock();
        std::ofstream follower_file_stream;
        follower_file_stream.open(master_follower_file_name, std::ios::app);
        follower_file_stream << std::string("u" + std::to_string(relat.src_user_id())) << std::endl;
        follower_file_stream.close();

        follower_file_stream.open(slave_follower_file_name, std::ios::app);
        follower_file_stream << std::string("u" + std::to_string(relat.src_user_id())) << std::endl;
        follower_file_stream.close();
        user_follower_context_lock.unlock();

        return true;
    }

    public:
    std::unique_ptr<grpc::ClientReaderWriter<snsCoordinator::Heartbeat, snsCoordinator::Heartbeat>> cReaderWriter;
    std::shared_ptr<grpc::Channel> coordChannel;
    
    std::unique_ptr<snsCoordinator::SNSCoordinator::Stub> coordStub;
    snsCoordinator::ServerType server_type;

    // Maps cluster IDs to stubs, for fast lookup and RPC calls
    std::unordered_map<int, std::shared_ptr<grpc::Channel>> followSyncChannels;
    std::unordered_map<int, std::shared_ptr<snsFollowSync::SNSFollowSync::Stub>> followSyncStubs;  

    int server_id;
    std::string port_no;

    // This maps user_ids to themselves, it's just used for searching
    std::unordered_map<int, int> user_ids;

    struct StatMap following_stat_map;
    struct StatMap timeline_stat_map;

    // Synchronize writes between the RPC and local methods
    std::mutex users_lock;
    std::mutex user_follower_context_lock;
    std::mutex user_timeline_context_lock;

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

    // This method handles the checking of new users, to update the user listing
    // It checks the user listing file and propagates new users to all other
    // clusters
    void CheckUsers() {
        struct stat sfile;
        std::string users_file = "slave" + std::to_string(server_id) + "_users.ls";
        stat(users_file.c_str(), &sfile);
        off_t current_file_size = sfile.st_size;
        off_t last_file_size = sfile.st_size;

        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            stat(users_file.c_str(), &sfile);
            current_file_size = sfile.st_size;
            
            if (current_file_size > last_file_size) {
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

                users_file_stream.close();

                for (int i : iUsers) {
                    auto u = user_ids.find(i);
                    if (u == user_ids.end()) {
                        users.add_src_user_id(i);
                        user_ids[i] = i;
                        following_stat_map.AddClient(i);
                        timeline_stat_map.AddClient(i);
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

    // This follow handles the checking of following relationships
    // It only stats the .fg file, any new entries are sent to the stub or the local method to modify
    // the .fl file
    void CheckFollowing() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            for (auto file_pair : following_stat_map.cluster_file_names) {
                int user_id = file_pair.first;
                following_stat_map.StatFile(user_id);
                if (following_stat_map.current_last_map[user_id].first > following_stat_map.current_last_map[user_id].second) {
                    std::ifstream user_following_stream;
                    user_following_stream.open(file_pair.second.second);

                    std::vector<Relat> relationships;
                    std::vector<int> vUserFollowings;
                    std::string sUser;

                    user_following_stream.seekg(following_stat_map.current_last_map[user_id].second);

                    while(getline(user_following_stream, sUser) && user_following_stream.is_open()) {
                        vUserFollowings.push_back(std::stoi(sUser.substr(1, sUser.size() - 1)));
                    }

                    user_following_stream.close();

                    for (int i : vUserFollowings) {
                        Relat relat;
                        relat.set_src_user_id(user_id);
                        relat.set_dest_user_id(i);
                        relationships.push_back(relat);
                    }

                    for (Relat r : relationships) {
                        grpc::ClientContext coordContext;
                        snsCoordinator::Search query;
                        snsCoordinator::Result result;

                        query.set_user_id(r.dest_user_id());

                        coordStub->WhoseClient(&coordContext, query, &result);

                        int stub_id = result.cluster_id();

                        if (stub_id == server_id) {
                            LocalFollowWrite(r);
                        } else {
                            grpc::ClientContext grpcFollowSyncContext;
                            snsFollowSync::Reply reply;
                            followSyncStubs[stub_id]->SyncFollow(&grpcFollowSyncContext, r, &reply);
                        }
                    }
                }

                following_stat_map.current_last_map[user_id].second = following_stat_map.current_last_map[user_id].first;
            }
        }
    }

    // This thread method handles the checking of context files
    // It contacts the statmap and iterates over every pair of files
    // It then gets all the recent posts and for every cluster client,
    // for every post, it writes the post to the appropriate cluster
    void CheckTimeline() {
        while (true) {
            // Check every 30 seconds
            std::this_thread::sleep_for(std::chrono::seconds(30));
            for (auto file_pair : timeline_stat_map.cluster_file_names) {
                int user_id = file_pair.first;
                // Stat the file and update the file size
                timeline_stat_map.StatFile(user_id);
                if (timeline_stat_map.current_last_map[user_id].first > timeline_stat_map.current_last_map[user_id].second) {
                    // Open the timeline stream
                    std::ifstream user_timeline_stream;
                    user_timeline_stream.open(file_pair.second.second);

                    std::vector<Post> posts;
                    std::string sPost;
                    Post msg;
                    
                    // Seek to the most recent post
                    user_timeline_stream.seekg(timeline_stat_map.current_last_map[user_id].second);

                    while (getline(user_timeline_stream, sPost)) {
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

                        msg.set_src_user_id(std::stoi(sPostUsername.substr(1, sPostUsername.size() - 1)));
                        msg.set_msg(sPostMessage);
                        msg.mutable_timestamp()->set_seconds(google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime).seconds());

                        posts.push_back(msg);
                    }

                    user_timeline_stream.close();

                    // Get the user's followers
                    std::ifstream user_followers_stream;
                    std::vector<int> user_follower_ids;
                    std::string sUser;
                    user_followers_stream.open(slave_prefix + std::to_string(server_id) + "_u" + std::to_string(user_id) + ".fl");
                    while (user_followers_stream >> sUser) {
                        user_follower_ids.push_back(std::stoi(sUser.substr(1, sUser.size() - 1)));
                    }
                    user_followers_stream.close();

                    // User follows themselves!
                    user_follower_ids.push_back(user_id);

                    // Send posts to all followers
                    for (int i : user_follower_ids) {
                        for (Post p : posts) {
                            grpc::ClientContext coordContext;
                            snsCoordinator::Search query;
                            snsCoordinator::Result result;

                            query.set_user_id(i);

                            coordStub->WhoseClient(&coordContext, query, &result);

                            int stub_id = result.cluster_id();
                            p.set_dest_user_id(i);

                            // If they're a local follower, the synchronizers need to write to the local file
                            if (stub_id == server_id) {
                                LocalTimelineWrite(p);
                            } else {
                                grpc::ClientContext grpcFollowSyncContext;
                                snsFollowSync::Reply reply;
                                followSyncStubs[stub_id]->SyncTimeline(&grpcFollowSyncContext, p, &reply);
                            }
                        }
                    }
                }

                timeline_stat_map.current_last_map[user_id].second = timeline_stat_map.current_last_map[user_id].first;
            }
        }
    }
};

void RunServer(std::string coordinator_ip, std::string coordinator_port, 
              std::string port_no, int server_id, snsCoordinator::ServerType sv_type) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSFollowSyncImpl Service;
  // Setting up identifier information
  Service.server_id = server_id;
  Service.port_no = port_no;
  Service.server_type = sv_type;

  // Getting the user listing from the replica, it's the most up to date
  std::ifstream user_listing;
  user_listing.open("slave" + std::to_string(server_id) + "_users.ls");
  std::string user;

  // Setting up the stub for the coordinator
  Service.coordChannel = grpc::CreateChannel(coordinator_ip + ":" + coordinator_port, grpc::InsecureChannelCredentials());
  Service.coordStub = snsCoordinator::SNSCoordinator::NewStub(Service.coordChannel);

  // This sets up the stat maps
  Service.following_stat_map.file_extension = ".fg";
  Service.following_stat_map.server_id = server_id;

  Service.timeline_stat_map.file_extension = "_temp.tl";
  Service.timeline_stat_map.server_id = server_id;

  // Read in all the users and add them to the local map
  while (user_listing >> user) {
    int user_id = std::stoi(user.substr(1, user.size() - 1));
    Service.user_ids[user_id] = user_id;

    grpc::ClientContext grpcCoordContext;
    snsCoordinator::Search query;
    snsCoordinator::Result result;
    query.set_user_id(user_id);
    Service.coordStub->WhoseClient(&grpcCoordContext, query, &result);
    
    // If we find a user that we're responsible for, add their context files as well
    if (result.cluster_id() == server_id) {
        struct stat st1;
        struct stat st2;

        Service.following_stat_map.stat_map[user_id] = st1;
        Service.timeline_stat_map.stat_map[user_id] = st2;

        Service.following_stat_map.AddClient(user_id);
        Service.timeline_stat_map.AddClient(user_id);

        std::ofstream user_timeline_stream_out;
        user_timeline_stream_out.open(Service.timeline_stat_map.cluster_file_names[user_id].second, std::ios::out | std::ios::trunc);
        user_timeline_stream_out.close();

        user_timeline_stream_out.open(Service.timeline_stat_map.cluster_file_names[user_id].first, std::ios::out | std::ios::trunc);
        user_timeline_stream_out.close();

        stat(Service.following_stat_map.cluster_file_names[user_id].second.c_str(), &Service.following_stat_map.stat_map[user_id]);
        stat(Service.timeline_stat_map.cluster_file_names[user_id].second.c_str(), &Service.timeline_stat_map.stat_map[user_id]);


        Service.following_stat_map.current_last_map[user_id] = std::pair<off_t, off_t>(Service.following_stat_map.stat_map[user_id].st_size, 
                                                                      Service.following_stat_map.stat_map[user_id].st_size);
        Service.timeline_stat_map.current_last_map[user_id] = std::pair<off_t, off_t>(Service.timeline_stat_map.stat_map[user_id].st_size, 
                                                                      Service.timeline_stat_map.stat_map[user_id].st_size);
    }
  }

  // Start checking users early, before system check ends runs on the coordinator
  std::thread tCheckUsersThread(&SNSFollowSyncImpl::CheckUsers, &Service);

  // Register the synchronizer
  grpc::ClientContext grpcHeartbeatContext;
  snsCoordinator::Heartbeat hb;
  hb.set_server_id(server_id);
  hb.set_server_type(sv_type);
  hb.set_server_port(port_no);

  time_t tsPostTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  auto gptPostTime = google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime);
  hb.mutable_timestamp()->set_seconds(gptPostTime.seconds());

  Service.cReaderWriter = Service.coordStub->ServerCommunicate(&grpcHeartbeatContext);
  Service.cReaderWriter->Write(hb);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&Service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  // Get all the available synchronizers from the coordinator
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

  // Start all the needed threads
  std::thread tHeartbeatThread(&SNSFollowSyncImpl::SendHeartbeat, &Service);
  std::thread tCheckFollowThread(&SNSFollowSyncImpl::CheckFollowing, &Service);
  std::thread tCheckTimelineThread(&SNSFollowSyncImpl::CheckTimeline, &Service);
  server->Wait();
}

int main(int argc, char** argv) {
  std::string port = "6710";
  std::string coord_ip = "localhost";
  std::string coord_port = "9000";
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