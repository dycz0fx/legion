#ifndef FPGA_UTILS_H
#define FPGA_UTILS_H

#include "experimental/xrt_device.h"
extern "C" {
xclDeviceHandle FPGAGetCurrentDevice(void);
}
#endif // FPGA_UTILS_H