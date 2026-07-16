#pragma once

#include "creek/filter_chain.hpp"

#include <cstdint>
#include <string>
#include <vector>

extern "C" {
struct M3Environment;
typedef struct M3Environment* IM3Environment;
struct M3Runtime;
typedef struct M3Runtime* IM3Runtime;
struct M3Module;
typedef struct M3Module* IM3Module;
struct M3Function;
typedef struct M3Function* IM3Function;
struct M3ImportContext;
typedef struct M3ImportContext* IM3ImportContext;
typedef const void* (*M3RawCall)(IM3Runtime, IM3ImportContext, uint64_t*, void*);
}

namespace creek {

class WasmRuntime {
public:
    static WasmRuntime& instance();

    uint32_t load_module(const Bytes& wasm_bytes);
    void link_host_function(uint32_t module_id,
                            const std::string& module_name,
                            const std::string& function_name,
                            const std::string& signature,
                            M3RawCall fn,
                            const void* userdata = nullptr);
    RpcContext call_on_request(uint32_t module_id, const RpcContext& ctx);
    RpcContext call_on_response(uint32_t module_id, const RpcContext& ctx);
    RpcContext* current_ctx() { return current_ctx_; }
    uint32_t scratch_write(const std::string& s) { return write_null_str(s); }

private:
    WasmRuntime();
    ~WasmRuntime();
    WasmRuntime(const WasmRuntime&) = delete;
    WasmRuntime& operator=(const WasmRuntime&) = delete;

    void set_current_ctx(RpcContext* ctx) { current_ctx_ = ctx; }

    struct ModuleInfo {
        uint32_t id;
        Bytes wasm_bytes;
        IM3Module module;
    };

    ModuleInfo* find_module(uint32_t module_id);
    void register_host_imports(ModuleInfo& mi);
    void ensure_memory(uint32_t min_pages);
    uint32_t write_null_str(const std::string& s);
    std::string read_null_str(uint32_t offset);
    void reset_scratch();

    IM3Environment env_ = nullptr;
    IM3Runtime runtime_ = nullptr;
    std::vector<ModuleInfo> modules_;
    uint32_t next_id_ = 1;
    uint8_t* mem_base_ = nullptr;
    uint32_t mem_size_ = 0;
    uint32_t scratch_head_ = 0;
    RpcContext* current_ctx_ = nullptr;

    static constexpr uint32_t kScratchBase = 65536;
    static constexpr uint32_t kResultBufSize = 4096;
};

class WasmFilter : public Filter {
public:
    explicit WasmFilter(const std::string& wasm_path);

    RpcContext on_request(RpcContext ctx) override;
    RpcContext on_response(RpcContext ctx) override;
    std::string name() const override;

private:
    uint32_t module_id_;
    std::string wasm_path_;
};

struct WasmConfig {
    std::string type;
    double probability{1.0};
    int delay_ms{};
    std::string mirror_target;
    std::string canary_endpoint;
    int canary_percentage{};
};

class JsonRuleFilter {
public:
    enum class Action { Passthrough, Delay, Mirror, Reroute, Reject };
    struct Result {
        Action action{Action::Passthrough};
        int delay_ms{};
        std::string mirror_target;
        std::string route_override;
        int status_code{200};
    };

    explicit JsonRuleFilter(WasmConfig config);
    explicit JsonRuleFilter(const std::string& json_config);
    ~JsonRuleFilter();
    JsonRuleFilter(JsonRuleFilter&&);
    JsonRuleFilter& operator=(JsonRuleFilter&&);

    Result on_request(const std::string& service, const std::string& method, Metadata& metadata);
    Result on_response(int status_code, Metadata& metadata);
    std::string name() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class JsonRuleFilterChain {
public:
    void add_json(const std::string& json_config);
    void add(JsonRuleFilter filter);
    JsonRuleFilter::Result process_request(const std::string& service, const std::string& method, Metadata& metadata);
    JsonRuleFilter::Result process_response(int status_code, Metadata& metadata);
    std::size_t size() const;
    void clear();

private:
    std::vector<JsonRuleFilter> filters_;
};

}
