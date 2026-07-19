#pragma once

// Internal IPv4 address resolution helper (numeric hosts, "localhost", and
// DNS names). Not part of the public API.

#include "socket_platform.hpp"

#include <cstdint>
#include <string>

namespace creek::tight_detail {

bool resolve_address(const std::string& host, std::uint16_t port, sockaddr_in& out);

}
