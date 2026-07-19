#include "creek/crash_handler.hpp"

#ifdef _WIN32

#include <windows.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

namespace {

std::string g_log_dir = "logs";
std::atomic<bool> g_crashed{false};

// Convert a runtime virtual address inside the exe image to its link-time VMA
// (what addr2line expects), accounting for ASLR relocation.
struct ImageInfo {
    std::uintptr_t runtime_base = 0;
    std::uintptr_t preferred_base = 0;
    std::uintptr_t image_size = 0;
};

ImageInfo exe_image_info() {
    ImageInfo info{};
    std::uintptr_t base = reinterpret_cast<std::uintptr_t>(::GetModuleHandleA(nullptr));
    info.runtime_base = base;
    info.preferred_base = base;
    auto* dos = reinterpret_cast<IMAGE_DOS_HEADER*>(base);
    if (dos && dos->e_magic == IMAGE_DOS_SIGNATURE) {
        auto* nt = reinterpret_cast<IMAGE_NT_HEADERS*>(
            reinterpret_cast<char*>(base) + dos->e_lfanew);
        if (nt->Signature == IMAGE_NT_SIGNATURE) {
            info.preferred_base = static_cast<std::uintptr_t>(nt->OptionalHeader.ImageBase);
            info.image_size = static_cast<std::uintptr_t>(nt->OptionalHeader.SizeOfImage);
        }
    }
    return info;
}

void write_crash(EXCEPTION_POINTERS* ep, const char* kind) {
    // Only dump the first crash; avoid recursion if the handler itself faults.
    if (g_crashed.exchange(true)) return;

    char path[560];
    std::snprintf(path, sizeof(path), "%s/crash.%lu.txt",
                  g_log_dir.c_str(), static_cast<unsigned long>(::GetCurrentProcessId()));
    // Best-effort: the dir usually exists (Logger created it), but just in case.
    ::CreateDirectoryA(g_log_dir.c_str(), nullptr);
    std::FILE* f = std::fopen(path, "a");
    if (!f) return;

    ImageInfo img = exe_image_info();
    std::fprintf(f, "kind=%s pid=%lu\n", kind, static_cast<unsigned long>(::GetCurrentProcessId()));
    std::fprintf(f, "exe_runtime_base=0x%llx exe_preferred_base=0x%llx image_size=0x%llx\n",
                 (unsigned long long)img.runtime_base,
                 (unsigned long long)img.preferred_base,
                 (unsigned long long)img.image_size);

    auto to_linked = [&](std::uintptr_t a) -> std::uintptr_t {
        if (img.image_size && a >= img.runtime_base && a < img.runtime_base + img.image_size) {
            return a - img.runtime_base + img.preferred_base;
        }
        return a;  // external module (DLL); left as runtime VA
    };

    if (ep && ep->ExceptionRecord) {
        auto* er = ep->ExceptionRecord;
        std::fprintf(f, "exception_code=0x%lx flags=0x%lx fault_pc=0x%llx fault_pc_linked=0x%llx\n",
                     er->ExceptionCode, er->ExceptionFlags,
                     (unsigned long long)reinterpret_cast<std::uintptr_t>(er->ExceptionAddress),
                     (unsigned long long)to_linked(reinterpret_cast<std::uintptr_t>(er->ExceptionAddress)));
        if (er->ExceptionCode == 0xC0000005 && er->NumberParameters >= 2) {
            std::fprintf(f, "access_violation %s addr=0x%llx\n",
                         er->ExceptionInformation[0] == 0 ? "read" :
                         er->ExceptionInformation[0] == 1 ? "write" : "exec",
                         (unsigned long long)er->ExceptionInformation[1]);
        }
#ifdef _WIN64
        if (ep->ContextRecord) {
            std::fprintf(f, "rip=0x%llx rsp=0x%llx rbp=0x%llx\n",
                         (unsigned long long)ep->ContextRecord->Rip,
                         (unsigned long long)ep->ContextRecord->Rsp,
                         (unsigned long long)ep->ContextRecord->Rbp);
        }
#endif
    }

    void* frames[64];
    USHORT n = ::CaptureStackBackTrace(0, 64, frames, nullptr);
    std::fprintf(f, "backtrace_linked %u\n", static_cast<unsigned>(n));
    for (USHORT i = 0; i < n; ++i) {
        std::fprintf(f, "  0x%llx\n",
                     (unsigned long long)to_linked(reinterpret_cast<std::uintptr_t>(frames[i])));
    }
    std::fclose(f);

    std::fprintf(stderr, "[crash_handler] %s crash written to %s\n", kind, path);
    std::fflush(stderr);
}

LONG WINAPI seh_filter(EXCEPTION_POINTERS* ep) {
    write_crash(ep, "seh");
    return EXCEPTION_EXECUTE_HANDLER;
}

void terminate_handler() {
    write_crash(nullptr, "terminate");
    std::abort();
}

} // namespace

namespace creek {

void install_crash_handler(const char* log_dir) {
    if (log_dir && *log_dir) g_log_dir = log_dir;
    // Vectored handler runs first (catches heap corruption that can bypass the
    // unhandled-exception filter); unhandled filter is the fallback.
    ::AddVectoredExceptionHandler(1, seh_filter);
    ::SetUnhandledExceptionFilter(seh_filter);
    std::set_terminate(terminate_handler);
}

} // namespace creek

#else

namespace creek {
void install_crash_handler(const char*) {}
} // namespace creek

#endif
