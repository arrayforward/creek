#include "creek/trace_context.hpp"

#include <algorithm>
#include <cctype>
#include <random>
#include <sstream>
#include <string>
#include <string_view>

namespace creek {

namespace {

std::string random_hex(int bytes) {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    static const char hex[] = "0123456789abcdef";
    std::string out(bytes * 2, '0');
    for (int i = 0; i < bytes; ++i) {
        auto v = static_cast<uint8_t>(rng() & 0xFF);
        out[i * 2] = hex[v >> 4];
        out[i * 2 + 1] = hex[v & 0xF];
    }
    return out;
}

bool is_hex(std::string_view s) {
    for (char c : s) {
        if (!std::isxdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

}

std::string TraceSpan::traceparent() const {
    std::ostringstream os;
    os << "00-" << trace_id << '-' << span_id << '-' << (flags < 10 ? "0" : "") << flags;
    return os.str();
}

std::string TraceSpan::traceparent_swapped() const {
    std::ostringstream os;
    os << "00-" << trace_id << '-' << parent_span_id << '-' << (flags < 10 ? "0" : "") << flags;
    return os.str();
}

TraceSpan TraceContext::parse_traceparent(std::string_view header) {
    TraceSpan span;
    if (header.size() < 55) return span;

    if (header[0] != '0' || header[1] != '0' || header[2] != '-') return span;

    span.trace_id = std::string(header.substr(3, 32));
    if (!is_hex(span.trace_id) || header[35] != '-') return span;

    span.span_id = std::string(header.substr(36, 16));
    if (!is_hex(span.span_id) || header[52] != '-') return span;

    auto flags_str = header.substr(53);
    while (!flags_str.empty() && flags_str.back() == ' ') {
        flags_str.remove_suffix(1);
    }
    try {
        span.flags = std::stoi(std::string(flags_str), nullptr, 16);
    } catch (...) {
        span.flags = 0;
    }

    return span;
}

std::string TraceContext::generate_span_id() {
    return random_hex(8);
}

std::string TraceContext::generate_trace_id() {
    return random_hex(16);
}

TraceSpan TraceContext::create_child(const TraceSpan& parent) {
    TraceSpan child;
    child.trace_id = parent.trace_id;
    child.parent_span_id = parent.span_id;
    child.span_id = generate_span_id();
    child.trace_state = parent.trace_state;
    child.flags = parent.flags;
    return child;
}

TraceSpan TraceContext::extract_or_create(const std::string& traceparent_header,
                                           const std::string& tracestate_header) {
    if (!traceparent_header.empty()) {
        auto span = parse_traceparent(traceparent_header);
        if (span.valid()) {
            span.trace_state = tracestate_header;
            return span;
        }
    }
    TraceSpan root;
    root.trace_id = generate_trace_id();
    root.span_id = generate_span_id();
    root.trace_state = tracestate_header;
    return root;
}

}
