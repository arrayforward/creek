#pragma once

#include "creek/types.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace creek {

class JsonRpcHttpServer {
public:
    using HeaderMap = std::map<std::string, std::string>;
    using Handler = std::function<std::pair<int, std::string>(std::string body, const HeaderMap& headers)>;

    JsonRpcHttpServer(Address bind, Handler handler);
    ~JsonRpcHttpServer();

    JsonRpcHttpServer(const JsonRpcHttpServer&) = delete;
    JsonRpcHttpServer& operator=(const JsonRpcHttpServer&) = delete;

    bool start();
    void stop();
    std::uint16_t local_port() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
