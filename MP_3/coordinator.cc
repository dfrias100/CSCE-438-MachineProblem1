#include <iostream>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <thread>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

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

using snsCoordinator::SNSCoordinator;
using snsCoordinator::ServerType;
using snsCoordinator::Coordinatee;
using snsCoordinator::Request;
using snsCoordinator::Reply;
using snsCoordinator::Heartbeat;
using snsCoordinator::ConnectionPoints;
using snsCoordinator::Search;
using snsCoordinator::Result;

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
	// This function handles the business logic of retrieving the
	// proper connection points depending on who's asking
	// Clients will get either a master or slave
	// Master servers will get the corresponding slave server
	Status Login(ServerContext* context, const Request* request, Reply* reply) override {
		if (request->coordinatee() == snsCoordinator::CLIENT) {
			int client_id = request->id();
			int server_id = (client_id % 3) + 1;

			if (vTableMaster[server_id].second == "Active") {
				reply->set_server_type(snsCoordinator::MASTER);
				reply->set_msg("localhost:" + vTableMaster[server_id].first);
			} else {
				reply->set_server_type(snsCoordinator::SLAVE);
				reply->set_msg("localhost:" + vTableSlave[server_id].first);
			}
		} else if (request->coordinatee() == snsCoordinator::SERVER) {
			if (request->server_type() == snsCoordinator::MASTER) {
				int cluster_id = request->id();
				reply->set_msg("localhost:" + vTableSlave[cluster_id].first);
			}
		}
		return Status::OK;
	};

	// This method handles the reading of heartbeat messages, each server maintains their own
	// stream. If a master server fails, the routing table is marked inactive.
	Status ServerCommunicate(ServerContext* context, ServerReaderWriter<Heartbeat, Heartbeat>* HRStream) override {
		Heartbeat hb;
		Heartbeat lastHb;
		bool got_first_beat = false;

		while (HRStream->Read(&hb)) {
			got_first_beat = true;
			switch (hb.server_type()) {
				case ServerType::MASTER:
					vTableMaster[hb.server_id()] = std::pair<std::string, std::string>(hb.server_port(), "Active");
					break;
				case ServerType::SLAVE:
					vTableSlave[hb.server_id()] = std::pair<std::string, std::string>(hb.server_port(), "Active");
					break;
				case ServerType::SYNC:
					vTableSync[hb.server_id()] = std::pair<std::string, std::string>(hb.server_port(), "Active");
					break;
			}
			lastHb = hb;
		}

		if (lastHb.server_type() == ServerType::MASTER && got_first_beat) {
			vTableMaster[lastHb.server_id()] = std::pair<std::string, std::string>(lastHb.server_port(), "Inactive");
		}

		return Status::OK;
	}

	// This method specifically handles returning all the available follower synchronizers 
	// and adds them to the reply
	Status ReturnFollowerSync(ServerContext* context, const Request* request, ConnectionPoints* connection_points) override {
		for (auto syncer : vTableSync) {
			if (syncer.first != request->id()) {
				connection_points->add_cluster_ids(syncer.first);
				connection_points->add_connection_points("localhost:" + syncer.second.first);
			}
		}
		return Status::OK;
	}

	// This uses the mod3 algorithm to notify a follower synchronizer of a clients cluster ID
	Status WhoseClient(ServerContext* context, const Search* request, Result* reply) override {
		int cluster_id = (request->user_id() % 3) + 1;

		reply->set_cluster_id(cluster_id);

		return Status::OK;
	}

	// The hash tables help map the client id to server clusters
	std::unordered_map<int, std::pair<std::string, std::string>> vTableMaster;
	std::unordered_map<int, std::pair<std::string, std::string>> vTableSlave;
	std::unordered_map<int, std::pair<std::string, std::string>> vTableSync;

	bool system_ready = false;

	public:

	void SystemCheck() {
		// This is to help the graders know when to begin testing of the servers.
		while (!system_ready) {
			if (vTableMaster.size() > 0 && vTableMaster.size() > 0 && vTableSync.size() > 0) {
				std::cout << "System is ready to accept clients." << std::endl;
				system_ready = true;
			}
		}
	}
};


void RunCoordinator(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSCoordinatorImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  std::thread tSystemCheck(&SNSCoordinatorImpl::SystemCheck, &service);
  server->Wait();
}

int main(int argc, char** argv) {
  std::string port = "9000";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunCoordinator(port);

  return 0;
}