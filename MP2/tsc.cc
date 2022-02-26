#include <thread>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include <google/protobuf/util/time_util.h>
#include "client.h"

#include "sns.grpc.pb.h"

class Client : public IClient
{
public:
    Client(const std::string &hname,
           const std::string &uname,
           const std::string &p)
        : hostname(hname), username(uname), port(p)
    {
    }
    void processTimelineRead(std::unique_ptr<grpc::ClientReaderWriter<csce438::Message, csce438::Message>>& cReaderWriter);
    void processTimelineWrite(std::unique_ptr<grpc::ClientReaderWriter<csce438::Message, csce438::Message>>& cReaderWriter);
protected:
    virtual int connectTo();
    virtual IReply processCommand(std::string &input);
    virtual void processTimeline();

private:
    std::string hostname;
    std::string username;
    std::string port;

    // You can have an instance of the client stub
    // as a member variable.
    std::shared_ptr<grpc::Channel> StubChannel;
    std::unique_ptr<csce438::SNSService::Stub> stub_;
};

int main(int argc, char **argv)
{

    std::string hostname = "localhost";
    std::string username = "default";
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:p:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            hostname = optarg;
            break;
        case 'u':
            username = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    Client myc(hostname, username, port);
    // You MUST invoke "run_client" function to start business logic
    myc.run_client();

    return 0;
}

int Client::connectTo()
{
    // ------------------------------------------------------------
    // In this function, you are supposed to create a stub so that
    // you call service methods in the processCommand/porcessTimeline
    // functions. That is, the stub should be accessible when you want
    // to call any service methods in those functions.
    // I recommend you to have the stub as
    // a member variable in your own Client class.
    // Please refer to gRpc tutorial how to create a stub.
    // ------------------------------------------------------------
    StubChannel = grpc::CreateChannel(hostname + ":" + port, grpc::InsecureChannelCredentials());
    stub_ = csce438::SNSService::NewStub(StubChannel);

    if (!StubChannel || !stub_)
        return -1;

    grpc::ClientContext grpcCContext;

    csce438::Request cRequest;
    csce438::Reply sReply;

    cRequest.set_username(username);

    grpc::Status status = stub_->Login(&grpcCContext, cRequest, &sReply);

    if (!status.ok())
    {
        return -1;
    }

    return 1; // return 1 if success, otherwise return -1
}

IReply Client::processCommand(std::string &input)
{
    // ------------------------------------------------------------
    // GUIDE 1:
    // In this function, you are supposed to parse the given input
    // command and create your own message so that you call an
    // appropriate service method. The input command will be one
    // of the followings:
    //
    // FOLLOW <username>
    // UNFOLLOW <username>
    // LIST
    // TIMELINE
    //
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // GUIDE 2:
    // Then, you should create a variable of IReply structure
    // provided by the client.h and initialize it according to
    // the result. Finally you can finish this function by returning
    // the IReply.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // HINT: How to set the IReply?
    // Suppose you have "Follow" service method for FOLLOW command,
    // IReply can be set as follow:
    //
    //     // some codes for creating/initializing parameters for
    //     // service method
    //     IReply ire;
    //     grpc::Status status = stub_->Follow(&context, /* some parameters */);
    //     ire.grpc_status = status;
    //     if (status.ok()) {
    //         ire.comm_status = SUCCESS;
    //     } else {
    //         ire.comm_status = FAILURE_NOT_EXISTS;
    //     }
    //
    //      return ire;
    //
    // IMPORTANT:
    // For the command "LIST", you should set both "all_users" and
    // "following_users" member variable of IReply.
    // ------------------------------------------------------------
    grpc::ClientContext grpcCContext;
    csce438::Request cRequest;
    csce438::Reply sReply;
    grpc::Status status;
    IReply ire;

    std::stringstream ssTokenizer(input);
    std::vector<std::string> vCommandTokens;
    std::string sToken;

    while (ssTokenizer >> sToken)
    {
        vCommandTokens.push_back(sToken);
    }

    if (vCommandTokens.empty())
    {
        ire.comm_status = FAILURE_INVALID;
        return ire;
    }
    else
    {
        std::transform(vCommandTokens[0].begin(), vCommandTokens[0].end(), vCommandTokens[0].begin(), ::toupper);
        if (vCommandTokens[0] == "FOLLOW")
        {
            if (vCommandTokens.size() < 2)
            {
                ire.comm_status = FAILURE_INVALID;
                return ire;
            }
            cRequest.set_username(username);
            cRequest.add_arguments(vCommandTokens[1]);
            status = stub_->Follow(&grpcCContext, cRequest, &sReply);
            ire.grpc_status = status;
            if (status.ok())
            {
                ire.comm_status = SUCCESS;
            }
            else
            {
                switch (status.error_code())
                {
                case grpc::StatusCode::NOT_FOUND:
                    ire.grpc_status = grpc::Status::OK;
                    ire.comm_status = FAILURE_INVALID_USERNAME;
                    break;
                case grpc::StatusCode::ALREADY_EXISTS:
                    ire.grpc_status = grpc::Status::OK;
                    ire.comm_status = FAILURE_ALREADY_EXISTS;
                    break;
                default:
                    ire.comm_status = FAILURE_UNKNOWN;
                }
            }
        }
        else if (vCommandTokens[0] == "UNFOLLOW")
        {
            if (vCommandTokens.size() < 2)
            {
                ire.comm_status = FAILURE_INVALID;
                return ire;
            }
            cRequest.set_username(username);
            cRequest.add_arguments(vCommandTokens[1]);
            status = stub_->UnFollow(&grpcCContext, cRequest, &sReply);
            ire.grpc_status = status;
            if (status.ok())
            {
                ire.comm_status = SUCCESS;
            }
            else
            {
                switch (status.error_code())
                {
                case grpc::StatusCode::NOT_FOUND:
                    ire.grpc_status = grpc::Status::OK;
                    ire.comm_status = FAILURE_INVALID_USERNAME;
                    break;
                default:
                    ire.comm_status = FAILURE_UNKNOWN;
                }
            }
        }
        else if (vCommandTokens[0] == "LIST")
        {
            cRequest.set_username(username);
            status = stub_->List(&grpcCContext, cRequest, &sReply);
            ire.grpc_status = status;
            if (status.ok())
            {
                ire.comm_status = SUCCESS;
                size_t stAllUserSize = sReply.mutable_all_users()->size();
                size_t stFollowingUserSize = sReply.mutable_following_users()->size();
                for (size_t i = 0; i < stAllUserSize; i++)
                    ire.all_users.push_back(sReply.all_users(i));

                for (size_t i = 0; i < stFollowingUserSize; i++)
                    ire.following_users.push_back(sReply.following_users(i));
            }
            else
            {
                ire.comm_status = FAILURE_UNKNOWN;
            }
        }
        else if (vCommandTokens[0] == "TIMELINE")
        {
            ire.comm_status = SUCCESS;
        }
        else
        {
            ire.grpc_status = grpc::Status(grpc::StatusCode::ABORTED, "");
            ire.comm_status = FAILURE_INVALID;
            return ire;
        }
    }

    return ire;
}

void Client::processTimeline()
{
    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // for both getting and displaying messages in timeline mode.
    // You should use them as you did in hw1.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
    grpc::ClientContext grpcCContext;
    std::unique_ptr<grpc::ClientReaderWriter<csce438::Message, csce438::Message>> cReaderWriter(stub_->Timeline(&grpcCContext));
    csce438::Message snsInitialMessage;
    snsInitialMessage.set_username(username);
    snsInitialMessage.set_msg("Starting message!");
    cReaderWriter->Write(snsInitialMessage);

    std::thread tClientReadingThread(&Client::processTimelineRead, this, std::ref(cReaderWriter));
    std::thread tClientWritingThread(&Client::processTimelineWrite, this, std::ref(cReaderWriter));

    tClientReadingThread.join();
    tClientWritingThread.join();
}

void Client::processTimelineRead(std::unique_ptr<grpc::ClientReaderWriter<csce438::Message, csce438::Message>>& cReaderWriter) {
    csce438::Message snsPost;
    while (cReaderWriter->Read(&snsPost)) {
        time_t tsPostTime = google::protobuf::util::TimeUtil::TimestampToTimeT(snsPost.timestamp());
        displayPostMessage(snsPost.username(), snsPost.msg(), tsPostTime);
    }
}

void Client::processTimelineWrite(std::unique_ptr<grpc::ClientReaderWriter<csce438::Message, csce438::Message>>& cReaderWriter) {
    std::string sPost;
    csce438::Message snsPost;
    while ((sPost = getPostMessage()).data() != nullptr) {
        snsPost.set_msg(sPost);
        snsPost.set_username(username);

        time_t tsPostTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        auto gptPostTime = google::protobuf::util::TimeUtil::TimeTToTimestamp(tsPostTime);
        snsPost.mutable_timestamp()->set_seconds(gptPostTime.seconds());
        
        cReaderWriter->Write(snsPost);
    }
}