#include "realm/fpga/fpga_module.h"
#include "realm/fpga/fpga_utils.h"
#include "realm/logging.h"

namespace Realm {
  namespace FPGA {
    extern Logger log_fpga;
  }
}

extern "C" {
  using namespace Realm;
  using namespace Realm::FPGA;

  REALM_PUBLIC_API xrt::device *FPGAGetCurrentDevice(void) {
    FPGAProcessor *p = FPGAProcessor::get_current_fpga_proc();
    xrt::device *ret = p->fpga_device->device;
    log_fpga.info() << "FPGAGetCurrentDevice()";
    return ret;
  }
}
