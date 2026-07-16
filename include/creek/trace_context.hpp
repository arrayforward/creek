#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace creek {

struct TraceSpan {
    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;
    std::string trace_state;
    int flags{};

    bool valid() const { return trace_id.size() == 32 && span_id.size() == 16; }
    std::string traceparent() const;
    std::string traceparent_swapped() const;
};

class TraceContext {
public:
    static TraceSpan parse_traceparent(std::string_view header);
    static std::string generate_span_id();
    static std::string generate_trace_id();
    static TraceSpan create_child(const TraceSpan& parent);
    static TraceSpan extract_or_create(const std::string& traceparent_header,
                                       const std::string& tracestate_header);
};

}
