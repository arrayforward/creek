#include "creek/crypto.hpp"

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

#include <openssl/evp.h>
#include <openssl/ec.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/err.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace creek {

namespace {

constexpr std::size_t kAesKeySize = 32;
constexpr std::size_t kNonceSize = 12;
constexpr std::size_t kTagSize = 16;
constexpr std::size_t kSec1UncompressedSize = 65;

Bytes hmac_sha256(const Bytes& key, const Bytes& data) {
    Bytes out(32);
    unsigned int out_len = 32;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         data.data(), data.size(), out.data(), &out_len);
    return out;
}

Bytes hkdf_sha256(const Bytes& ikm, const Bytes& salt, const Bytes& info, std::size_t out_len) {
    Bytes prk;
    if (salt.empty()) {
        prk = hmac_sha256(Bytes(32, 0), ikm);
    } else {
        prk = hmac_sha256(salt, ikm);
    }

    Bytes out;
    out.reserve(out_len);
    Bytes prev;
    std::uint8_t counter = 1;
    while (out.size() < out_len) {
        Bytes data;
        data.insert(data.end(), prev.begin(), prev.end());
        data.insert(data.end(), info.begin(), info.end());
        data.push_back(counter);
        prev = hmac_sha256(prk, data);
        out.insert(out.end(), prev.begin(), prev.end());
        ++counter;
    }
    out.resize(out_len);
    return out;
}

Bytes derive_aes_key(const Bytes& shared_secret) {
    const Bytes salt = {};
    const Bytes info = {'a', 'e', 's', '-', '2', '5', '6', '-', 'g', 'c', 'm', '-', 'c', 'r', 'e', 'e', 'k'};
    return hkdf_sha256(shared_secret, salt, info, kAesKeySize);
}

void rand_bytes(std::uint8_t* buf, std::size_t len) {
    if (RAND_bytes(buf, static_cast<int>(len)) != 1) {
        throw std::runtime_error("RAND_bytes failed");
    }
}

class EvpCipherCtx {
public:
    EvpCipherCtx() : ctx_(EVP_CIPHER_CTX_new()) {
        if (!ctx_) throw std::runtime_error("EVP_CIPHER_CTX_new failed");
    }
    ~EvpCipherCtx() {
        if (ctx_) EVP_CIPHER_CTX_free(ctx_);
    }
    EvpCipherCtx(const EvpCipherCtx&) = delete;
    EvpCipherCtx& operator=(const EvpCipherCtx&) = delete;
    EvpCipherCtx(EvpCipherCtx&&) = delete;
    EvpCipherCtx& operator=(EvpCipherCtx&&) = delete;

    EVP_CIPHER_CTX* get() { return ctx_; }
    EVP_CIPHER_CTX* operator*() { return ctx_; }

private:
    EVP_CIPHER_CTX* ctx_;
};

}

class ECDHKeyExchange::Impl {
public:
    Impl() {
        EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
        if (!pctx) throw std::runtime_error("EVP_PKEY_CTX_new_id failed");

        if (EVP_PKEY_keygen_init(pctx) <= 0) {
            EVP_PKEY_CTX_free(pctx);
            throw std::runtime_error("EVP_PKEY_keygen_init failed");
        }

        if (EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1) <= 0) {
            EVP_PKEY_CTX_free(pctx);
            throw std::runtime_error("set curve failed");
        }

        if (EVP_PKEY_keygen(pctx, &key_) <= 0) {
            EVP_PKEY_CTX_free(pctx);
            throw std::runtime_error("keygen failed");
        }

        EVP_PKEY_CTX_free(pctx);

        ec_key_ = EVP_PKEY_get1_EC_KEY(key_);
        if (!ec_key_) throw std::runtime_error("get EC_KEY failed");
    }

    ~Impl() {
        if (key_) EVP_PKEY_free(key_);
        if (ec_key_) EC_KEY_free(ec_key_);
    }

    Bytes public_key_sec1() const {
        const EC_POINT* pub = EC_KEY_get0_public_key(ec_key_);
        const EC_GROUP* group = EC_KEY_get0_group(ec_key_);
        std::size_t len = EC_POINT_point2oct(group, pub, POINT_CONVERSION_UNCOMPRESSED, nullptr, 0, nullptr);
        if (len == 0) throw std::runtime_error("point2oct size failed");
        Bytes out(len);
        if (EC_POINT_point2oct(group, pub, POINT_CONVERSION_UNCOMPRESSED, out.data(), len, nullptr) == 0) {
            throw std::runtime_error("point2oct failed");
        }
        return out;
    }

    Bytes compute_shared_secret(const Bytes& peer_pubkey) const {
        EC_KEY* peer_ec = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
        if (!peer_ec) throw std::runtime_error("EC_KEY_new_by_curve_name failed");

        const EC_GROUP* group = EC_KEY_get0_group(peer_ec);
        EC_POINT* peer_pub = EC_POINT_new(group);
        if (!peer_pub) {
            EC_KEY_free(peer_ec);
            throw std::runtime_error("EC_POINT_new failed");
        }

        if (EC_POINT_oct2point(group, peer_pub, peer_pubkey.data(), peer_pubkey.size(), nullptr) != 1) {
            EC_POINT_free(peer_pub);
            EC_KEY_free(peer_ec);
            throw std::runtime_error("oct2point failed");
        }

        if (EC_KEY_set_public_key(peer_ec, peer_pub) != 1) {
            EC_POINT_free(peer_pub);
            EC_KEY_free(peer_ec);
            throw std::runtime_error("set_public_key failed");
        }

        EVP_PKEY* peer_key = EVP_PKEY_new();
        if (!peer_key) {
            EC_POINT_free(peer_pub);
            EC_KEY_free(peer_ec);
            throw std::runtime_error("EVP_PKEY_new failed");
        }

        if (EVP_PKEY_set1_EC_KEY(peer_key, peer_ec) != 1) {
            EC_POINT_free(peer_pub);
            EC_KEY_free(peer_ec);
            EVP_PKEY_free(peer_key);
            throw std::runtime_error("set1 EC_KEY failed");
        }
        EC_POINT_free(peer_pub);
        EC_KEY_free(peer_ec);

        EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new(key_, nullptr);
        if (!ctx) {
            EVP_PKEY_free(peer_key);
            throw std::runtime_error("EVP_PKEY_CTX_new failed");
        }

        if (EVP_PKEY_derive_init(ctx) <= 0) {
            EVP_PKEY_CTX_free(ctx);
            EVP_PKEY_free(peer_key);
            throw std::runtime_error("derive init failed");
        }

        if (EVP_PKEY_derive_set_peer(ctx, peer_key) <= 0) {
            EVP_PKEY_CTX_free(ctx);
            EVP_PKEY_free(peer_key);
            throw std::runtime_error("set peer failed");
        }

        std::size_t secret_len = 0;
        if (EVP_PKEY_derive(ctx, nullptr, &secret_len) <= 0) {
            EVP_PKEY_CTX_free(ctx);
            EVP_PKEY_free(peer_key);
            throw std::runtime_error("get secret len failed");
        }

        Bytes secret(secret_len);
        if (EVP_PKEY_derive(ctx, secret.data(), &secret_len) <= 0) {
            EVP_PKEY_CTX_free(ctx);
            EVP_PKEY_free(peer_key);
            throw std::runtime_error("derive failed");
        }

        secret.resize(secret_len);

        EVP_PKEY_CTX_free(ctx);
        EVP_PKEY_free(peer_key);
        return secret;
    }

private:
    EVP_PKEY* key_ = nullptr;
    EC_KEY* ec_key_ = nullptr;
};

ECDHKeyExchange::ECDHKeyExchange() : impl_(std::make_unique<Impl>()) {}
ECDHKeyExchange::~ECDHKeyExchange() = default;
ECDHKeyExchange::ECDHKeyExchange(ECDHKeyExchange&&) noexcept = default;
ECDHKeyExchange& ECDHKeyExchange::operator=(ECDHKeyExchange&&) noexcept = default;

Bytes ECDHKeyExchange::generate_keypair() {
    return impl_->public_key_sec1();
}

Bytes ECDHKeyExchange::compute_shared_secret(const Bytes& peer_pubkey) const {
    return impl_->compute_shared_secret(peer_pubkey);
}

Bytes ECDHKeyExchange::public_key() const {
    return impl_->public_key_sec1();
}

class AeadCipher::Impl {
public:
    explicit Impl(Bytes shared_secret)
        : aes_key_(derive_aes_key(shared_secret)) {
    }

    Bytes encrypt(const Bytes& plaintext, const Bytes& aad) {
        EvpCipherCtx ctx;
        Bytes nonce(kNonceSize);
        rand_bytes(nonce.data(), kNonceSize);

        if (EVP_EncryptInit_ex(*ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) {
            throw std::runtime_error("EncryptInit failed");
        }

        if (EVP_CIPHER_CTX_ctrl(*ctx, EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(kNonceSize), nullptr) != 1) {
            throw std::runtime_error("set IV len failed");
        }

        if (EVP_EncryptInit_ex(*ctx, nullptr, nullptr, aes_key_.data(), nonce.data()) != 1) {
            throw std::runtime_error("EncryptInit with key failed");
        }

        int out_len = 0;
        if (!aad.empty()) {
            if (EVP_EncryptUpdate(*ctx, nullptr, &out_len, aad.data(), static_cast<int>(aad.size())) != 1) {
                throw std::runtime_error("AAD update failed");
            }
        }

        Bytes ciphertext(plaintext.size() + kTagSize);
        if (EVP_EncryptUpdate(*ctx, ciphertext.data(), &out_len, plaintext.data(),
                              static_cast<int>(plaintext.size())) != 1) {
            throw std::runtime_error("EncryptUpdate failed");
        }
        int actual_cipher_len = out_len;

        if (EVP_EncryptFinal_ex(*ctx, ciphertext.data() + actual_cipher_len, &out_len) != 1) {
            throw std::runtime_error("EncryptFinal failed");
        }
        actual_cipher_len += out_len;

        Bytes tag(kTagSize);
        if (EVP_CIPHER_CTX_ctrl(*ctx, EVP_CTRL_GCM_GET_TAG, static_cast<int>(kTagSize), tag.data()) != 1) {
            throw std::runtime_error("get tag failed");
        }

        ciphertext.resize(actual_cipher_len);

        Bytes result;
        result.reserve(kNonceSize + ciphertext.size() + kTagSize);
        result.insert(result.end(), nonce.begin(), nonce.end());
        result.insert(result.end(), ciphertext.begin(), ciphertext.end());
        result.insert(result.end(), tag.begin(), tag.end());
        return result;
    }

    Bytes decrypt(const Bytes& packet) {
        return decrypt(packet, {});
    }

    Bytes decrypt(const Bytes& packet, const Bytes& aad) {
        if (packet.size() < kNonceSize + kTagSize) {
            throw std::runtime_error("packet too short");
        }

        EvpCipherCtx ctx;
        const auto* nonce = packet.data();
        const auto* ciphertext = packet.data() + kNonceSize;
        std::size_t ciphertext_len = packet.size() - kNonceSize - kTagSize;
        const auto* tag = packet.data() + packet.size() - kTagSize;

        if (EVP_DecryptInit_ex(*ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) {
            throw std::runtime_error("DecryptInit failed");
        }

        if (EVP_CIPHER_CTX_ctrl(*ctx, EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(kNonceSize), nullptr) != 1) {
            throw std::runtime_error("set IV len failed");
        }

        if (EVP_DecryptInit_ex(*ctx, nullptr, nullptr, aes_key_.data(), nonce) != 1) {
            throw std::runtime_error("DecryptInit with key failed");
        }

        int out_len = 0;
        if (!aad.empty()) {
            if (EVP_DecryptUpdate(*ctx, nullptr, &out_len, aad.data(), static_cast<int>(aad.size())) != 1) {
                throw std::runtime_error("AAD update failed");
            }
        }

        Bytes plaintext(ciphertext_len + kTagSize);
        if (EVP_DecryptUpdate(*ctx, plaintext.data(), &out_len, ciphertext,
                              static_cast<int>(ciphertext_len)) != 1) {
            throw std::runtime_error("DecryptUpdate failed");
        }
        int actual_plain_len = out_len;

        if (EVP_CIPHER_CTX_ctrl(*ctx, EVP_CTRL_GCM_SET_TAG, static_cast<int>(kTagSize),
                                 const_cast<std::uint8_t*>(tag)) != 1) {
            throw std::runtime_error("set tag failed");
        }

        int ret = EVP_DecryptFinal_ex(*ctx, plaintext.data() + actual_plain_len, &out_len);
        if (ret <= 0) {
            throw std::runtime_error("authentication failed");
        }
        actual_plain_len += out_len;

        plaintext.resize(actual_plain_len);
        return plaintext;
    }

private:
    Bytes aes_key_;
};

AeadCipher::AeadCipher(Bytes shared_secret) : impl_(std::make_unique<Impl>(std::move(shared_secret))) {}
AeadCipher::~AeadCipher() = default;
AeadCipher::AeadCipher(AeadCipher&&) noexcept = default;
AeadCipher& AeadCipher::operator=(AeadCipher&&) noexcept = default;

Bytes AeadCipher::encrypt(const Bytes& plaintext, const Bytes& aad) {
    return impl_->encrypt(plaintext, aad);
}

Bytes AeadCipher::decrypt(const Bytes& packet) {
    return impl_->decrypt(packet);
}

Bytes AeadCipher::decrypt(const Bytes& packet, const Bytes& aad) {
    return impl_->decrypt(packet, aad);
}

}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
