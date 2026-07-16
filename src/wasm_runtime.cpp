#include "creek/wasm_runtime.hpp"

#include "wasm3.h"
#include "m3_env.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
#include <thread>

namespace creek {
namespace {

constexpr uint32_t kDefaultStackSize = 65536;

thread_local std::mt19937 rand_engine{std::random_device{}()};

namespace host {

const void* creek_get_metadata(IM3Runtime runtime, IM3ImportContext ctx, uint64_t* wasm_sp, void* mem) {
    auto* self = static_cast<WasmRuntime*>(ctx->userdata);
    m3ApiReturnType(uint32_t);
    m3ApiGetArg(uint32_t, key_ptr);
    m3ApiGetArg(uint32_t, key_len);

    std::string key(reinterpret_cast<const char*>(static_cast<uint8_t*>(mem) + key_ptr), key_len);
    std::string value;
    if (auto* ctx = self->current_ctx()) {
        auto it = ctx->metadata.find(key);
        if (it != ctx->metadata.end()) {
            value = it->second;
        }
    }
    m3ApiReturn(self->scratch_write(value));
}

const void* creek_set_metadata(IM3Runtime runtime, IM3ImportContext ctx, uint64_t* wasm_sp, void* mem) {
    auto* self = static_cast<WasmRuntime*>(ctx->userdata);
    m3ApiGetArg(uint32_t, key_ptr);
    m3ApiGetArg(uint32_t, key_len);
    m3ApiGetArg(uint32_t, value_ptr);
    m3ApiGetArg(uint32_t, value_len);

    std::string key(reinterpret_cast<const char*>(static_cast<uint8_t*>(mem) + key_ptr), key_len);
    std::string value(reinterpret_cast<const char*>(static_cast<uint8_t*>(mem) + value_ptr), value_len);
    if (auto* ctx = self->current_ctx()) {
        ctx->metadata[key] = std::move(value);
    }
    m3ApiSuccess();
}

const void* creek_sleep(IM3Runtime runtime, IM3ImportContext ctx, uint64_t* wasm_sp, void* mem) {
    m3ApiGetArg(uint32_t, ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    m3ApiSuccess();
}

const void* creek_random(IM3Runtime runtime, IM3ImportContext ctx, uint64_t* wasm_sp, void* mem) {
    m3ApiReturnType(uint32_t);
    m3ApiReturn(static_cast<uint32_t>(rand_engine()));
}

const void* creek_log(IM3Runtime runtime, IM3ImportContext ctx, uint64_t* wasm_sp, void* mem) {
    m3ApiGetArg(uint32_t, msg_ptr);
    m3ApiGetArg(uint32_t, msg_len);
    std::string msg(reinterpret_cast<const char*>(static_cast<uint8_t*>(mem) + msg_ptr), msg_len);
    std::cout << "[wasm] " << msg << std::endl;
    m3ApiSuccess();
}

} // namespace host
} // namespace

WasmRuntime::WasmRuntime() {
    env_ = m3_NewEnvironment();
    if (!env_) {
        throw std::runtime_error("WasmRuntime: failed to create m3 environment");
    }
    runtime_ = m3_NewRuntime(env_, kDefaultStackSize, nullptr);
    if (!runtime_) {
        m3_FreeEnvironment(env_);
        throw std::runtime_error("WasmRuntime: failed to create m3 runtime");
    }
}

WasmRuntime::~WasmRuntime() {
    if (runtime_) {
        m3_FreeRuntime(runtime_);
    }
    if (env_) {
        m3_FreeEnvironment(env_);
    }
}

WasmRuntime& WasmRuntime::instance() {
    static WasmRuntime inst;
    return inst;
}

uint32_t WasmRuntime::load_module(const Bytes& wasm_bytes) {
    ModuleInfo mi;
    mi.id = next_id_++;
    mi.wasm_bytes = wasm_bytes;

    M3Result result = m3_ParseModule(env_, &mi.module,
                                      mi.wasm_bytes.data(),
                                      static_cast<uint32_t>(mi.wasm_bytes.size()));
    if (result) {
        throw std::runtime_error(std::string("WasmRuntime: parse failed: ") + result);
    }

    result = m3_LoadModule(runtime_, mi.module);
    if (result) {
        m3_FreeModule(mi.module);
        throw std::runtime_error(std::string("WasmRuntime: load failed: ") + result);
    }

    register_host_imports(mi);

    result = m3_CompileModule(mi.module);
    if (result) {
        throw std::runtime_error(std::string("WasmRuntime: compile failed: ") + result);
    }

    result = m3_RunStart(mi.module);
    if (result && result != m3Err_functionLookupFailed) {
        throw std::runtime_error(std::string("WasmRuntime: start failed: ") + result);
    }

    ensure_memory(2);

    modules_.push_back(std::move(mi));
    return modules_.back().id;
}

void WasmRuntime::link_host_function(uint32_t module_id,
                                      const std::string& module_name,
                                      const std::string& function_name,
                                      const std::string& signature,
                                      M3RawCall fn,
                                      const void* userdata) {
    auto* mi = find_module(module_id);
    if (!mi) {
        throw std::runtime_error("WasmRuntime: module not found: " + std::to_string(module_id));
    }
    M3Result result = m3_LinkRawFunctionEx(mi->module, module_name.c_str(),
                                            function_name.c_str(), signature.c_str(),
                                            fn, userdata);
    if (result) {
        throw std::runtime_error(std::string("WasmRuntime: link failed: ") + result);
    }
}

RpcContext WasmRuntime::call_on_request(uint32_t module_id, const RpcContext& ctx) {
    auto* mi = find_module(module_id);
    if (!mi) {
        throw std::runtime_error("WasmRuntime: module not found: " + std::to_string(module_id));
    }

    IM3Function func = nullptr;
    M3Result result = m3_FindFunction(&func, runtime_, "on_request");
    if (result || !func) {
        return ctx;
    }

    reset_scratch();
    ensure_memory(2);

    RpcContext mutable_ctx = ctx;
    current_ctx_ = &mutable_ctx;

    nlohmann::json meta_json = mutable_ctx.metadata;
    std::string meta_json_str = meta_json.dump();

    uint32_t svc_ptr = write_null_str(mutable_ctx.service);
    uint32_t method_ptr = write_null_str(mutable_ctx.method);
    uint32_t meta_ptr = write_null_str(meta_json_str);
    uint32_t result_ptr = scratch_head_;
    scratch_head_ += kResultBufSize;
    std::memset(mem_base_ + result_ptr, 0, kResultBufSize);

    uint32_t args[4] = { svc_ptr, method_ptr, meta_ptr, result_ptr };
    const void* arg_ptrs[4] = { &args[0], &args[1], &args[2], &args[3] };

    result = m3_Call(func, 4, arg_ptrs);
    current_ctx_ = nullptr;

    if (result) {
        throw std::runtime_error(std::string("WasmRuntime: call on_request failed: ") + result);
    }

    std::string result_json = read_null_str(result_ptr);
    if (!result_json.empty()) {
        try {
            auto parsed = nlohmann::json::parse(result_json);
            if (parsed.is_object()) {
                for (auto& [k, v] : parsed.items()) {
                    mutable_ctx.metadata[k] = v.get<std::string>();
                }
            }
        } catch (...) {
        }
    }

    return mutable_ctx;
}

RpcContext WasmRuntime::call_on_response(uint32_t module_id, const RpcContext& ctx) {
    auto* mi = find_module(module_id);
    if (!mi) {
        throw std::runtime_error("WasmRuntime: module not found: " + std::to_string(module_id));
    }

    IM3Function func = nullptr;
    M3Result result = m3_FindFunction(&func, runtime_, "on_response");
    if (result || !func) {
        return ctx;
    }

    reset_scratch();
    ensure_memory(2);

    RpcContext mutable_ctx = ctx;
    current_ctx_ = &mutable_ctx;

    nlohmann::json meta_json = mutable_ctx.metadata;
    std::string meta_json_str = meta_json.dump();

    uint32_t meta_ptr = write_null_str(meta_json_str);
    uint32_t result_ptr = scratch_head_;
    scratch_head_ += kResultBufSize;
    std::memset(mem_base_ + result_ptr, 0, kResultBufSize);

    uint32_t args[3] = { static_cast<uint32_t>(mutable_ctx.status_code), meta_ptr, result_ptr };
    const void* arg_ptrs[3] = { &args[0], &args[1], &args[2] };

    result = m3_Call(func, 3, arg_ptrs);
    current_ctx_ = nullptr;

    if (result) {
        throw std::runtime_error(std::string("WasmRuntime: call on_response failed: ") + result);
    }

    std::string result_json = read_null_str(result_ptr);
    if (!result_json.empty()) {
        try {
            auto parsed = nlohmann::json::parse(result_json);
            if (parsed.is_object()) {
                for (auto& [k, v] : parsed.items()) {
                    mutable_ctx.metadata[k] = v.get<std::string>();
                }
            }
        } catch (...) {
        }
    }

    return mutable_ctx;
}

WasmRuntime::ModuleInfo* WasmRuntime::find_module(uint32_t module_id) {
    for (auto& mi : modules_) {
        if (mi.id == module_id) {
            return &mi;
        }
    }
    return nullptr;
}

void WasmRuntime::register_host_imports(ModuleInfo& mi) {
    link_host_function(mi.id, "env", "creek_get_metadata", "i(ii)", host::creek_get_metadata, this);
    link_host_function(mi.id, "env", "creek_set_metadata", "v(iiii)", host::creek_set_metadata, this);
    link_host_function(mi.id, "env", "creek_sleep", "v(i)", host::creek_sleep, this);
    link_host_function(mi.id, "env", "creek_random", "i()", host::creek_random, this);
    link_host_function(mi.id, "env", "creek_log", "v(ii)", host::creek_log, this);
}

void WasmRuntime::ensure_memory(uint32_t min_pages) {
    uint32_t sz = 0;
    mem_base_ = m3_GetMemory(runtime_, &sz, 0);
    mem_size_ = sz;
    if (sz < min_pages * 65536) {
        M3Result result = ResizeMemory(runtime_, min_pages);
        if (result) {
            throw std::runtime_error(std::string("WasmRuntime: memory resize failed: ") + result);
        }
        mem_base_ = m3_GetMemory(runtime_, &mem_size_, 0);
    }
}

uint32_t WasmRuntime::write_null_str(const std::string& s) {
    uint32_t offset = scratch_head_;
    uint32_t len = static_cast<uint32_t>(s.size()) + 1;
    if (offset + len > mem_size_) {
        throw std::runtime_error("WasmRuntime: scratch buffer overflow");
    }
    std::memcpy(mem_base_ + offset, s.c_str(), len);
    scratch_head_ = offset + len;
    return offset;
}

std::string WasmRuntime::read_null_str(uint32_t offset) {
    if (offset >= mem_size_) {
        return {};
    }
    const char* start = reinterpret_cast<const char*>(mem_base_ + offset);
    uint32_t max_len = mem_size_ - offset;
    uint32_t len = 0;
    while (len < max_len && start[len] != '\0') {
        ++len;
    }
    return std::string(start, len);
}

void WasmRuntime::reset_scratch() {
    scratch_head_ = kScratchBase;
}

WasmFilter::WasmFilter(const std::string& wasm_path)
    : wasm_path_(wasm_path) {
    std::ifstream file(wasm_path, std::ios::binary | std::ios::ate);
    if (!file) {
        throw std::runtime_error("WasmFilter: cannot open " + wasm_path);
    }
    auto size = file.tellg();
    file.seekg(0, std::ios::beg);
    Bytes wasm_bytes(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(wasm_bytes.data()), size);
    module_id_ = WasmRuntime::instance().load_module(wasm_bytes);
}

RpcContext WasmFilter::on_request(RpcContext ctx) {
    return WasmRuntime::instance().call_on_request(module_id_, std::move(ctx));
}

RpcContext WasmFilter::on_response(RpcContext ctx) {
    return WasmRuntime::instance().call_on_response(module_id_, std::move(ctx));
}

std::string WasmFilter::name() const {
    return "WasmFilter[" + wasm_path_ + "]";
}

}
