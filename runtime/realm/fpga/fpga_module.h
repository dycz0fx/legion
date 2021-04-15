#ifndef REALM_FPGA_H
#define REALM_FPGA_H

#include "realm/module.h"
#include "realm/proc_impl.h"
#include "realm/mem_impl.h"
#include "realm/runtime_impl.h"

namespace Realm {
  namespace FPGA {
    class FPGADevice {
      public:
        std::string name;
        FPGADevice();
        FPGADevice(std::string name);
    };

    class FPGAProcessor : public LocalTaskProcessor {
      public:
        FPGAProcessor(FPGADevice *fpga_device, Processor me, Realm::CoreReservationSet &crs);
        virtual ~FPGAProcessor(void);
              
      protected:
        FPGADevice *fpga_device;
        Realm::CoreReservation *core_rsrv_;
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
        unsigned cfg_num_fpgas;
        std::string cfg_xclbin_path;
        std::vector<FPGADevice *> fpga_devices;

      protected:
  	    std::vector<FPGAProcessor *> fpga_procs_;
    };

    REGISTER_REALM_MODULE(FPGAModule);

  }; // namespace FPGA
}; // namespace Realm

#endif
