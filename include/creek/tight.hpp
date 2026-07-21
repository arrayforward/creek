#pragma once

// Adapter header: the tight reliable-UDP transport now lives in the
// standalone tight/ library (namespace tight, see tight/include/tight/).
// This header re-exports the pieces the creek runtime uses and provides
// conversions from the creek config types (creek::Address / creek::RemotePeer)
// to their tight counterparts.

#include "creek/types.hpp"
#include "tight/tight.hpp"

namespace creek {

using tight::LinkRole;
using tight::LinkState;
using tight::PeerEvent;
using tight::TightConfig;
using tight::TightTransport;

inline tight::NetAddress to_tight_address(const Address& addr) {
    return tight::NetAddress(addr.host, addr.port);
}

inline tight::RemotePeer to_tight_peer(const RemotePeer& peer) {
    return tight::RemotePeer{peer.id, to_tight_address(peer.address)};
}

}
