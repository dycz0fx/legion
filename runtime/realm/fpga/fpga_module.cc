#include "realm/fpga/fpga_module.h"

#include "realm/logging.h"
#include "realm/cmdline.h"
#include "realm/utils.h"

// // each task access by include header file where the namespace is declared
// namespace XRTContext {
//   // define extern xrt_device
//   thread_local XRTDevice<HW_EXP_SIZE, HW_MANT_SIZE> *xrt_device = 0;
// }

namespace Realm {
  namespace FPGA {

    Logger log_fpga("fpga");

    FPGADevice::FPGADevice() {
      name = "";
    }

    FPGADevice::FPGADevice(std::string name) : name(name) {
    }

    FPGAModule::FPGAModule() : Module("fpga"), cfg_num_fpgas(0) {
    }

    FPGAModule::~FPGAModule(void) {}

    Module *FPGAModule::create_module(RuntimeImpl *runtime, std::vector<std::string>& cmdline) {
      FPGAModule *m = new FPGAModule;
      log_fpga.info() << "create_module";
      // first order of business - read command line parameters
      {
      Realm::CommandLineParser cp;
      cp.add_option_int("-ll:fpga", m->cfg_num_fpgas);
      cp.add_option_string("-ll:xclbin", m->cfg_xclbin_path);

      bool ok = cp.parse_command_line(cmdline);
      if (!ok) {
        log_fpga.error() << "error reading fpga parameters";
        exit(1);
      }}

      for (int i = 0; i < m->cfg_num_fpgas; i++) {
        // template arguments must be known at compile time and const
        FPGADevice *fpga_device = new FPGADevice("fpga" + std::to_string(i));
        m->fpga_devices.push_back(fpga_device);
      }

      return m;
    }

    // do any general initialization - this is called after all configuration is
    //  complete
    void FPGAModule::initialize(RuntimeImpl *runtime) {
      log_fpga.info() << "initialize";
      Module::initialize(runtime);
    }

    // create any memories provided by this module (default == do nothing)
    //  (each new MemoryImpl should use a Memory from RuntimeImpl::next_local_memory_id)
    void FPGAModule::create_memories(RuntimeImpl *runtime) {
      log_fpga.info() << "create_memories";
      Module::create_memories(runtime);
    }

    // create any processors provided by the module (default == do nothing)
    //  (each new ProcessorImpl should use a Processor from
    //   RuntimeImpl::next_local_processor_id)
    void FPGAModule::create_processors(RuntimeImpl *runtime) {
      Module::create_processors(runtime); 
      for (int i = 0; i < cfg_num_fpgas; i++) {
        Processor p = runtime->next_local_processor_id();
        FPGAProcessor *proc = new FPGAProcessor(fpga_devices[i], p, runtime->core_reservation_set());
        fpga_procs_.push_back(proc);
        runtime->add_processor(proc);
        log_fpga.info() << "create fpga processor " << i;

        // create mem affinities to add a proc to machine model
        // create affinities between this processor and system/reg memories
        // if the memory is one we created, use the kernel-reported distance
        // to adjust the answer
        std::vector<MemoryImpl *>& local_mems = runtime->nodes[Network::my_node_id].memories;
          for(std::vector<MemoryImpl *>::iterator it = local_mems.begin();
              it != local_mems.end();
              ++it) {
            Memory::Kind kind = (*it)->get_kind();
            if((kind != Memory::SYSTEM_MEM) && (kind != Memory::REGDMA_MEM))
              continue;

            Machine::ProcessorMemoryAffinity pma;
            pma.p = p;
            pma.m = (*it)->me;

            // use the same made-up numbers as in
            //  runtime_impl.cc
            if(kind == Memory::SYSTEM_MEM) {
              pma.bandwidth = 100;  // "large"
              pma.latency = 5;      // "small"
            } else {
              pma.bandwidth = 80;   // "large"
              pma.latency = 10;     // "small"
            }

            runtime->add_proc_mem_affinity(pma);
 
          }

      }
    }

    // create any DMA channels provided by the module (default == do nothing)
    void FPGAModule::create_dma_channels(RuntimeImpl *runtime) {
      log_fpga.info() << "create_dma_channels";
      Module::create_dma_channels(runtime);
    }

    // create any code translators provided by the module (default == do nothing)
    void FPGAModule::create_code_translators(RuntimeImpl *runtime) {
      log_fpga.info() << "create_code_translators";
      Module::create_code_translators(runtime);
    }

    // clean up any common resources created by the module - this will be called
    //  after all memories/processors/etc. have been shut down and destroyed
    void FPGAModule::cleanup(void) {
      log_fpga.info() << "cleanup";
      Module::cleanup();
    }

    template <typename T>
    class FPGATaskScheduler : public T {
      public:
        FPGATaskScheduler(Processor proc, Realm::CoreReservation& core_rsrv, FPGAProcessor *fpga_proc);
        virtual ~FPGATaskScheduler(void);
      protected:
        virtual bool execute_task(Task *task);
        virtual void execute_internal_task(InternalTask *task);
        FPGAProcessor *fpga_proc_;
    };

    template <typename T>
    FPGATaskScheduler<T>::FPGATaskScheduler(Processor proc,
                                            Realm::CoreReservation& core_rsrv,
                                            FPGAProcessor *fpga_proc) : T(proc, core_rsrv), fpga_proc_(fpga_proc) {
    }

    template <typename T>
      FPGATaskScheduler<T>::~FPGATaskScheduler(void) {
    }

    template <typename T>
    bool FPGATaskScheduler<T>::execute_task(Task *task) {
      log_fpga.info() << "execute_task " << task;
      // add device to thread's xrt context
      // XRTContext::xrt_device = fpga_proc_->xrt_device_;
      bool ok = T::execute_task(task);
      return ok;
    }

    template <typename T>
    void FPGATaskScheduler<T>::execute_internal_task(InternalTask *task) {
      log_fpga.info() << "execute_internal_task";
      // add device to thread's xrt context
      // XRTContext::xrt_device = fpga_proc_->xrt_device_;
      // T::execute_internal_task(task);
    }

    FPGAProcessor::FPGAProcessor(FPGADevice *fpga_device, Processor me, Realm::CoreReservationSet& crs)
    : LocalTaskProcessor(me, Processor::FPGA_PROC)
    {
      log_fpga.info() << "FPGAProcessor()";
      // xrt_device_ = xrt;
      this->fpga_device = fpga_device;
      Realm::CoreReservationParameters params;
      params.set_num_cores(1);
      params.set_alu_usage(params.CORE_USAGE_SHARED);
      params.set_fpu_usage(params.CORE_USAGE_SHARED);
      params.set_ldst_usage(params.CORE_USAGE_SHARED);
      params.set_max_stack_size(2 << 20);
      std::string name = stringbuilder() << "fpga proc " << me;
      core_rsrv_ = new Realm::CoreReservation(name, crs, params);

      #ifdef REALM_USE_USER_THREADS
      UserThreadTaskScheduler *sched = new FPFATaskScheduler<UserThreadTaskScheduler>(me, *core_rsrv_, this);
      #else
      KernelThreadTaskScheduler *sched = new FPGATaskScheduler<KernelThreadTaskScheduler>(me, *core_rsrv_, this);
      #endif
      set_scheduler(sched);
    }

    FPGAProcessor::~FPGAProcessor(void) {
      delete core_rsrv_;
    }

  }; // namespace FPGA
}; // namespace Realm
