#pragma once

#include "creek/types.hpp"

#include <cstddef>
#include <memory>

namespace creek {

class ECDHKeyExchange {
public:
    ECDHKeyExchange();
    ~ECDHKeyExchange();
    ECDHKeyExchange(const ECDHKeyExchange&) = delete;
    ECDHKeyExchange& operator=(const ECDHKeyExchange&) = delete;
    ECDHKeyExchange(ECDHKeyExchange&&) noexcept;
    ECDHKeyExchange& operator=(ECDHKeyExchange&&) noexcept;

    Bytes generate_keypair();
    Bytes compute_shared_secret(const Bytes& peer_pubkey) const;
    Bytes public_key() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class AeadCipher {
public:
    explicit AeadCipher(Bytes shared_secret);
    ~AeadCipher();
    AeadCipher(const AeadCipher&) = delete;
    AeadCipher& operator=(const AeadCipher&) = delete;
    AeadCipher(AeadCipher&&) noexcept;
    AeadCipher& operator=(AeadCipher&&) noexcept;

    Bytes encrypt(const Bytes& plaintext, const Bytes& aad);
    Bytes decrypt(const Bytes& packet);
    Bytes decrypt(const Bytes& packet, const Bytes& aad);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
