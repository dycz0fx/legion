#ifndef REALM_FPGA_H
#define REALM_FPGA_H

#include "realm/module.h"
#include "realm/proc_impl.h"
#include "realm/mem_impl.h"
#include "realm/runtime_impl.h"

// XCL includes
#include "ert.h"
#include "xrt.h"
#include "xrt_mem.h"

namespace Realm {
  namespace FPGA {
    class FPGADevice {
      public:
        std::string name;
        xclDeviceHandle dev_handle;
        int cur_fpga_mem_bank;
        FPGADevice(xclDeviceHandle dev_handle, std::string name);
        void create_fpga_mem(RuntimeImpl *runtime, size_t size);
        void copy_to_fpga(off_t dst_offset, const void *src, size_t bytes);
        void copy_from_fpga(void *dst, off_t src_offset, size_t bytes);
    };

    class FPGAProcessor : public LocalTaskProcessor {
      public:
        FPGAProcessor(FPGADevice *fpga_device, Processor me, Realm::CoreReservationSet &crs);
        virtual ~FPGAProcessor(void);
        static FPGAProcessor *get_current_fpga_proc(void);
        FPGADevice *fpga_device;
      protected:
        Realm::CoreReservation *core_rsrv_;
    };

    class FPGADeviceMemory : public LocalManagedMemory{
      public:
        FPGADeviceMemory(Memory memory, FPGADevice *device, size_t size);
        virtual ~FPGADeviceMemory(void);
        virtual void get_bytes(off_t offset, void *dst, size_t size);
        virtual void put_bytes(off_t offset, const void *src, size_t size);

        FPGADevice *get_device() const { return device; };
        FPGADevice *device;
    };

    class FPGAModule : public Module {
      protected:
        FPGAModule(void);

      public:
        virtual ~FPGAModule(void);

        static Module *create_module(RuntimeImpl *runtime, std::vector<std::string>& cmdline);

        // do any general initialization - this is called after all configuration is
        //  complete
        virtual void initialize(RuntimeImpl *runtime);

        // create any memories provided by this module (default == do nothing)
        //  (each new MemoryImpl should use a Memory from RuntimeImpl::next_local_memory_id)
        virtual void create_memories(RuntimeImpl *runtime);

        // create any processors provided by the module (default == do nothing)
        //  (each new ProcessorImpl should use a Processor from
        //   RuntimeImpl::next_local_processor_id)
        virtual void create_processors(RuntimeImpl *runtime);

        // create any DMA channels provided by the module (default == do nothing)
        virtual void create_dma_channels(RuntimeImpl *runtime);

        // create any code translators provided by the module (default == do nothing)
        virtual void create_code_translators(RuntimeImpl *runtime);

        // clean up any common resources created by the module - this will be called
        //  after all memories/processors/etc. have been shut down and destroyed
        virtual void cleanup(void);

      public:
        size_t cfg_num_fpgas;
        std::vector<FPGADevice *> fpga_devices;

      protected:
  	    std::vector<FPGAProcessor *> fpga_procs_;
    };

    REGISTER_REALM_MODULE(FPGAModule);

  }; // namespace FPGA
}; // namespace Realm

#endif
