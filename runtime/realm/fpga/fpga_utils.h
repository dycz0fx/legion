#ifndef FPGA_UTILS_H
#define FPGA_UTILS_H

#include "experimental/xrt_device.h"
extern "C"
{
  xclDeviceHandle FPGAGetCurrentDevice(void);
  void *FPGAGetBasePtrSys(void);
  void *FPGAGetBasePtrDev(void);
}
#endif // FPGA_UTILS_H