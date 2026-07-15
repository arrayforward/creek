#pragma once

#include "creek/metrics.hpp"

#include <memory>
#include <string>

namespace creek {

class OtlpExporter {
public:
    OtlpExporter(std::string collector_addr, std::string service_name, bool use_tls = false);
    ~OtlpExporter();

    OtlpExporter(const OtlpExporter&) = delete;
    OtlpExporter& operator=(const OtlpExporter&) = delete;

    bool export_metrics(MetricsStore& store);
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
