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

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
	Status Login(ServerContext* context, const Request* request, Reply* reply) override {
		if (request->coordinatee() == snsCoordinator::CLIENT) {
			int client_id = request->id();
			int server_id = (client_id % 3) + 1;

			if (vTableMaster[server_id].second == "Active") {
				reply->set_msg("localhost:" + vTableMaster[server_id].first);
			} else {
				reply->set_msg("localhost:" + vTableSlave[server_id].first);
			}
		} else if (request->coordinatee() == snsCoordinator::SERVER) {
			if (request->server_type() == snsCoordinator::MASTER) {
				int cluster_id = request->id();
				reply->set_msg("localhost:" + vTableSlave[cluster_id].first);
			} else if (request->server_type() == snsCoordinator::SYNC) {
				int client_id = request->id();
				int cluster_id = (client_id % 3) + 1;
				reply->set_msg("localhost:" + vTableSync[cluster_id].first);
			}
		}
		return Status::OK;
	};

	Status ServerCommunicate(ServerContext* context, ServerReaderWriter<Heartbeat, Heartbeat>* HRStream) override {
		Heartbeat hb;
		Heartbeat lastHb;
		bool got_first_beat = false;

		while (HRStream->Read(&hb)) {
			got_first_beat = true;
			switch (hb.server_type()) {
				case ServerType::MASTER:
					std::cout << "Got master heartbeat from server ID " << hb.server_id() << "!" << std::endl;
					vTableMaster[hb.server_id()] = std::pair<std::string, std::string>(hb.server_port(), "Active");
					break;
				case ServerType::SLAVE:
					std::cout << "Got slave heartbeat from server ID " << hb.server_id() << "!" << std::endl;
					vTableSlave[hb.server_id()] = std::pair<std::string, std::string>(hb.server_port(), "Active");
					break;
				case ServerType::SYNC:
					std::cout << "Got synchronizer heartbeat from server ID " << hb.server_id() << "!" << std::endl;
					vTableSync[hb.server_id()] = std::pair<std::string, std::string>(hb.server_port(), "Active");
					break;
			}
			lastHb = hb;
		}

		if (lastHb.server_type() == ServerType::MASTER && got_first_beat) {
			std::cout << "Cluster " << lastHb.server_id() << " master heartbeat has failed!" << std::endl;
			vTableMaster[lastHb.server_id()] = std::pair<std::string, std::string>(lastHb.server_port(), "Inactive");
		}
		
		return Status::OK;
	}

	Status ReturnFollowerSync(ServerContext* context, const Request* request, ConnectionPoints* connection_points) override {
		for (auto syncer : vTableSync) {
			if (syncer.first != request->id()) {
				connection_points->add_cluster_ids(syncer.first);
				connection_points->add_connection_points("localhost:" + syncer.second.first);
			}
		}
		return Status::OK;
	}

	std::unordered_map<int, std::pair<std::string, std::string>> vTableMaster;
	std::unordered_map<int, std::pair<std::string, std::string>> vTableSlave;
	std::unordered_map<int, std::pair<std::string, std::string>> vTableSync;
};


void RunCoordinator(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSCoordinatorImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "9090";
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