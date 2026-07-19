#pragma once

namespace creek {

// Installs SEH + C++ terminate handlers that, on a fatal crash (access
// violation, heap corruption, abort), write the exception code, faulting PC
// and a stack backtrace to <log_dir>/crash.<pid>.txt. Addresses are written
// as link-time VMAs so they can be resolved offline with:
//   addr2line -e creek_sidecar.exe -f -C <addr>
void install_crash_handler(const char* log_dir);

} // namespace creek
