#include "creek.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

struct Options {
    std::string target;
    std::string name{"world"};
    std::string sid{"1"};
    bool sticky{true};
    int count{1};
    int timeout_ms{3000};
};

void usage() {
    std::cerr << "usage: creek_hello_client --target HOST:PORT [--name NAME] [--sid SID] [--sticky [true|false]] [--count N] [--timeout-ms MS]\n";
}

std::string value_after(int& index, int argc, char** argv, std::string_view option) {
    if (++index >= argc) {
        throw std::runtime_error("missing value for " + std::string(option));
    }
    return argv[index];
}

int positive_integer(const std::string& value, std::string_view option) {
    std::size_t consumed = 0;
    long parsed = 0;
    try {
        parsed = std::stol(value, &consumed);
    } catch (const std::exception&) {
        throw std::runtime_error("invalid value for " + std::string(option));
    }
    if (consumed != value.size() || parsed <= 0 || parsed > 2147483647L) {
        throw std::runtime_error("invalid value for " + std::string(option));
    }
    return static_cast<int>(parsed);
}

bool boolean_value(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (value == "true" || value == "1" || value == "yes") {
        return true;
    }
    if (value == "false" || value == "0" || value == "no") {
        return false;
    }
    throw std::runtime_error("invalid value for --sticky");
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--target" || option == "--leaf") {
            options.target = value_after(index, argc, argv, option);
        } else if (option == "--name") {
            options.name = value_after(index, argc, argv, option);
        } else if (option == "--sid") {
            options.sid = value_after(index, argc, argv, option);
        } else if (option == "--sticky") {
            if (index + 1 < argc && std::string_view(argv[index + 1]).rfind("--", 0) != 0) {
                options.sticky = boolean_value(argv[++index]);
            } else {
                options.sticky = true;
            }
        } else if (option == "--no-sticky") {
            options.sticky = false;
        } else if (option == "--count") {
            options.count = positive_integer(value_after(index, argc, argv, option), option);
        } else if (option == "--timeout-ms") {
            options.timeout_ms = positive_integer(value_after(index, argc, argv, option), option);
        } else {
            throw std::runtime_error("unknown option: " + option);
        }
    }
    if (options.target.empty()) {
        throw std::runtime_error("--target is required");
    }
    if (options.sid.empty()) {
        throw std::runtime_error("--sid must not be empty");
    }
    return options;
}

int execute(const Options& options) {
    auto channel = grpc::CreateChannel(options.target, grpc::InsecureChannelCredentials());
    auto greeter = creek::v1::Greeter::NewStub(channel);
    creek::v1::HelloRequest request;
    request.set_name(options.name);
    request.set_sid(options.sid);
    request.set_sticky(options.sticky);
    for (int index = 0; index < options.count; ++index) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(options.timeout_ms));
        context.AddMetadata("sticky", options.sticky ? "true" : "false");
        context.AddMetadata("sid", options.sid);
        creek::v1::HelloReply reply;
        const auto status = greeter->SayHello(&context, request, &reply);
        if (!status.ok()) {
            std::cerr << "SayHello failed: " << status.error_code() << " "
                      << status.error_message() << '\n';
            return 1;
        }
        std::cout << reply.backend_id() << '\t' << reply.message() << '\n';
    }
    return 0;
}

}

int main(int argc, char** argv) {
    if (argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h")) {
        usage();
        return 0;
    }
    try {
        return execute(parse_options(argc, argv));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        usage();
        return 2;
    }
}
