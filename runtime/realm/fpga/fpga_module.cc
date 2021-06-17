#include "realm/fpga/fpga_module.h"

#include "realm/logging.h"
#include "realm/cmdline.h"
#include "realm/utils.h"

namespace Realm
{
  namespace FPGA
  {

    namespace ThreadLocal
    {
      static REALM_THREAD_LOCAL FPGAProcessor *current_fpga_proc = NULL;
    }

    Logger log_fpga("fpga");

    // need types with various powers-of-2 size/alignment - we have up to
    //  uint64_t as builtins, but we need trivially-copyable 16B and 32B things
    struct dummy_16b_t
    {
      uint64_t a, b;
    };
    struct dummy_32b_t
    {
      uint64_t a, b, c, d;
    };
    REALM_ALIGNED_TYPE_CONST(aligned_16b_t, dummy_16b_t, 16);
    REALM_ALIGNED_TYPE_CONST(aligned_32b_t, dummy_32b_t, 32);

    template <typename T>
    static void fpga_memcpy_2d_typed(uintptr_t dst_base, uintptr_t dst_lstride,
                                     uintptr_t src_base, uintptr_t src_lstride,
                                     size_t bytes, size_t lines)
    {
      for (size_t i = 0; i < lines; i++)
      {
        std::copy(reinterpret_cast<const T *>(src_base),
                  reinterpret_cast<const T *>(src_base + bytes),
                  reinterpret_cast<T *>(dst_base));
        // manual strength reduction
        src_base += src_lstride;
        dst_base += dst_lstride;
      }
    }

    // TODO: temp solution
    static void fpga_memcpy_2d(uintptr_t dst_base, uintptr_t dst_lstride,
                               uintptr_t src_base, uintptr_t src_lstride,
                               size_t bytes, size_t lines)
    {
      // by subtracting 1 from bases, strides, and lengths, we get LSBs set
      //  based on the common alignment of every parameter in the copy
      unsigned alignment = ((dst_base - 1) & (dst_lstride - 1) &
                            (src_base - 1) & (src_lstride - 1) &
                            (bytes - 1));
      // TODO: consider jump table approach?
      if ((alignment & 31) == 31)
        fpga_memcpy_2d_typed<aligned_32b_t>(dst_base, dst_lstride,
                                            src_base, src_lstride,
                                            bytes, lines);
      else if ((alignment & 15) == 15)
        fpga_memcpy_2d_typed<aligned_16b_t>(dst_base, dst_lstride,
                                            src_base, src_lstride,
                                            bytes, lines);
      else if ((alignment & 7) == 7)
        fpga_memcpy_2d_typed<uint64_t>(dst_base, dst_lstride, src_base, src_lstride,
                                       bytes, lines);
      else if ((alignment & 3) == 3)
        fpga_memcpy_2d_typed<uint32_t>(dst_base, dst_lstride, src_base, src_lstride,
                                       bytes, lines);
      else if ((alignment & 1) == 1)
        fpga_memcpy_2d_typed<uint16_t>(dst_base, dst_lstride, src_base, src_lstride,
                                       bytes, lines);
      else
        fpga_memcpy_2d_typed<uint8_t>(dst_base, dst_lstride, src_base, src_lstride,
                                      bytes, lines);
    }

    FPGADevice::FPGADevice(xclDeviceHandle dev_handle, std::string name) : name(name), cur_fpga_mem_bank(1), fpga_mem(NULL)
    {
      this->dev_handle = dev_handle;
      this->bo_handle = 0;
    }

    FPGADevice::~FPGADevice()
    {
      xclUnmapBO(this->dev_handle, this->bo_handle, this->fpga_mem->base_ptr_sys);
      xclFreeBO(this->dev_handle, this->bo_handle);
    }

    void FPGADevice::create_dma_channels(RuntimeImpl *runtime)
    {
      if (!fpga_mem)
      {
        return;
      }

      const std::vector<MemoryImpl *> &local_mems = runtime->nodes[Network::my_node_id].memories;
      for (std::vector<Realm::MemoryImpl *>::const_iterator it = local_mems.begin();
           it != local_mems.end();
           it++)
      {
        if ((*it)->kind != MemoryImpl::MKIND_SYSMEM)
        {
          continue;
        }
        this->local_sysmem = *it;
      }

      runtime->add_dma_channel(new FPGAfillChannel(this, &runtime->bgwork));
      runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_IN_DEV, &runtime->bgwork));
      runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_TO_DEV, &runtime->bgwork));
      runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_FROM_DEV, &runtime->bgwork));

      Machine::MemoryMemoryAffinity mma;
      mma.m1 = fpga_mem->me;
      mma.m2 = local_sysmem->me;
      mma.bandwidth = 20; // TODO
      mma.latency = 200;
      runtime->add_mem_mem_affinity(mma);

      // TODO: create p2p channel
      // runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_PEER_DEV, &runtime->bgwork));
    }

    void FPGADevice::create_fpga_mem(RuntimeImpl *runtime, size_t size)
    {
      // the third argument 0 is unused
      bo_handle = xclAllocBO(dev_handle, size, 0, cur_fpga_mem_bank);
      void *base_ptr_sys = (void *)xclMapBO(dev_handle, bo_handle, true);
      struct xclBOProperties bo_prop;
      xclGetBOProperties(dev_handle, bo_handle, &bo_prop);
      uint64_t base_ptr_dev = bo_prop.paddr;
      Memory m = runtime->next_local_memory_id();
      this->fpga_mem = new FPGADeviceMemory(m, this, base_ptr_sys, (void *)base_ptr_dev, size);
      runtime->add_memory(fpga_mem);
      log_fpga.info() << "create_fpga_mem: "
                      << dev_handle
                      << ", size = "
                      << (size >> 20) << " MB"
                      << ", base_ptr_sys = " << base_ptr_sys
                      << ", base_ptr_dev = " << base_ptr_dev;
    }

    void FPGADevice::copy_to_fpga(off_t dst_offset, const void *src, size_t bytes, FPGACompletionNotification *notification)
    {
      size_t dst = reinterpret_cast<size_t>((uint8_t *)(fpga_mem->base_ptr_sys) + dst_offset);
      int *temp = (int *)dst;
      log_fpga.info() << "copy_to_fpga: src = " << src << " dst_offset = " << dst_offset
                      << " bytes = " << bytes << " dst = " << temp << "\n";
      fpga_memcpy_2d(reinterpret_cast<uintptr_t>(dst), 0,
                     reinterpret_cast<uintptr_t>(src), 0,
                     bytes, 1);
      xclSyncBO(this->dev_handle, this->bo_handle, XCL_BO_SYNC_BO_TO_DEVICE, bytes, dst_offset);
      // printf("copy_to_fpga: ");
      // for (int i = 0; i < 6000; i++)
      // {
      //   if (((char *)fpga_mem->base_ptr_sys)[i] == '\0') {
      //     printf("_");
      //   }
      //   else {
      //     printf("%c", ((char *)fpga_mem->base_ptr_sys)[i]);
      //   }
      // }
      // printf("\n");
      notification->request_completed();
    }

    void FPGADevice::copy_from_fpga(void *dst, off_t src_offset, size_t bytes, FPGACompletionNotification *notification)
    {
      size_t src = reinterpret_cast<size_t>((uint8_t *)(fpga_mem->base_ptr_sys) + src_offset);
      int *temp = (int *)src;
      log_fpga.info() << "copy_from_fpga: dst = " << dst << " src_offset = " << src_offset
                      << " bytes = " << bytes << " src = " << temp << "\n";
      // xclSyncBO(this->dev_handle, this->bo_handle, XCL_BO_SYNC_BO_FROM_DEVICE, bytes, src_offset);
      xclSyncBO(this->dev_handle, this->bo_handle, XCL_BO_SYNC_BO_FROM_DEVICE, bytes, src_offset);
      fpga_memcpy_2d(reinterpret_cast<uintptr_t>(dst), 0,
                     reinterpret_cast<uintptr_t>(src), 0,
                     bytes, 1);
      // printf("copy_from_fpga: ");
      // for (int i = 0; i < 6000; i++)
      // {
      //   if (((char *)fpga_mem->base_ptr_sys)[i] == '\0') {
      //     printf("_");
      //   }
      //   else {
      //     printf("%c", ((char *)fpga_mem->base_ptr_sys)[i]);
      //   }
      // }
      // printf("\n");
      notification->request_completed();
    }

    void FPGADevice::copy_within_fpga(off_t dst_offset, off_t src_offset, size_t bytes, FPGACompletionNotification *notification)
    {
      log_fpga.info() << "copy_within_fpga: dst_offset = " << dst_offset << " src_offset = " << src_offset
                      << " bytes = " << bytes << "\n";

      notification->request_completed();
    }

    void FPGADevice::copy_to_peer(FPGADevice *dst, off_t dst_offset, off_t src_offset, size_t bytes, FPGACompletionNotification *notification)
    {
      log_fpga.info() << "copy_to_peer: dst = " << dst << " dst_offset = " << dst_offset
                      << " bytes = " << bytes << " notification = " << notification << "\n";
      notification->request_completed();
    }

    FPGAModule::FPGAModule() : Module("fpga"), cfg_num_fpgas(0), cfg_fpga_mem_size(0)
    {
    }

    FPGAModule::~FPGAModule(void)
    {
      if (!this->fpga_devices.empty())
      {
        for (size_t i = 0; i < fpga_devices.size(); i++)
        {
          xclClose(fpga_devices[i]->dev_handle);
          delete this->fpga_devices[i];
        }
      }
    }

    Module *FPGAModule::create_module(RuntimeImpl *runtime, std::vector<std::string> &cmdline)
    {
      FPGAModule *m = new FPGAModule;
      log_fpga.info() << "create_module";
      // first order of business - read command line parameters
      {
        Realm::CommandLineParser cp;
        cp.add_option_int("-ll:fpga", m->cfg_num_fpgas);
        cp.add_option_int_units("-ll:fpga_size", m->cfg_fpga_mem_size, 'm');

        bool ok = cp.parse_command_line(cmdline);
        if (!ok)
        {
          log_fpga.error() << "error reading fpga parameters";
          exit(1);
        }
      }

      for (size_t i = 0; i < m->cfg_num_fpgas; i++)
      {
        xclDeviceHandle dev_handle = xclOpen((unsigned int)i, NULL, XCL_QUIET);
        FPGADevice *fpga_device = new FPGADevice(dev_handle, "fpga" + std::to_string(i));
        m->fpga_devices.push_back(fpga_device);
      }

      return m;
    }

    // do any general initialization - this is called after all configuration is
    //  complete
    void FPGAModule::initialize(RuntimeImpl *runtime)
    {
      log_fpga.info() << "initialize";
      Module::initialize(runtime);
    }

    // create any memories provided by this module (default == do nothing)
    //  (each new MemoryImpl should use a Memory from RuntimeImpl::next_local_memory_id)
    void FPGAModule::create_memories(RuntimeImpl *runtime)
    {
      log_fpga.info() << "create_memories";
      Module::create_memories(runtime);
      if (cfg_fpga_mem_size > 0)
      {
        for (size_t i = 0; i < cfg_num_fpgas; i++)
        {
          fpga_devices[i]->create_fpga_mem(runtime, cfg_fpga_mem_size);
        }
      }
    }

    // create any processors provided by the module (default == do nothing)
    //  (each new ProcessorImpl should use a Processor from
    //   RuntimeImpl::next_local_processor_id)
    void FPGAModule::create_processors(RuntimeImpl *runtime)
    {
      Module::create_processors(runtime);
      for (size_t i = 0; i < cfg_num_fpgas; i++)
      {
        Processor p = runtime->next_local_processor_id();
        FPGAProcessor *proc = new FPGAProcessor(fpga_devices[i], p, runtime->core_reservation_set());
        fpga_procs_.push_back(proc);
        runtime->add_processor(proc);
        log_fpga.info() << "create fpga processor " << i;

        // create mem affinities to add a proc to machine model
        // create affinities between this processor and system/reg memories
        // if the memory is one we created, use the kernel-reported distance
        // to adjust the answer
        std::vector<MemoryImpl *> &local_mems = runtime->nodes[Network::my_node_id].memories;
        for (std::vector<MemoryImpl *>::iterator it = local_mems.begin();
             it != local_mems.end();
             ++it)
        {
          Memory::Kind kind = (*it)->get_kind();
          if (kind == Memory::SYSTEM_MEM or kind == Memory::FPGA_MEM)
          {
            Machine::ProcessorMemoryAffinity pma;
            pma.p = p;
            pma.m = (*it)->me;

            // use the same made-up numbers as in
            //  runtime_impl.cc
            if (kind == Memory::SYSTEM_MEM)
            {
              pma.bandwidth = 100; // "large"
              pma.latency = 5;     // "small"
            }
            else if (kind == Memory::FPGA_MEM)
            {
              pma.bandwidth = 200; // "large"
              pma.latency = 10;    // "small"
            }
            else
            {
              assert(0 && "wrong memory kind");
            }

            runtime->add_proc_mem_affinity(pma);
          }
        }
      }
    }

    // create any DMA channels provided by the module (default == do nothing)
    void FPGAModule::create_dma_channels(RuntimeImpl *runtime)
    {
      log_fpga.info() << "create_dma_channels";
      for (std::vector<FPGADevice *>::iterator it = fpga_devices.begin(); it != fpga_devices.end(); it++)
      {
        (*it)->create_dma_channels(runtime);
      }

      Module::create_dma_channels(runtime);
    }

    // create any code translators provided by the module (default == do nothing)
    void FPGAModule::create_code_translators(RuntimeImpl *runtime)
    {
      log_fpga.info() << "create_code_translators";
      Module::create_code_translators(runtime);
    }

    // clean up any common resources created by the module - this will be called
    //  after all memories/processors/etc. have been shut down and destroyed
    void FPGAModule::cleanup(void)
    {
      log_fpga.info() << "cleanup";
      Module::cleanup();
    }

    template <typename T>
    class FPGATaskScheduler : public T
    {
    public:
      FPGATaskScheduler(Processor proc, Realm::CoreReservation &core_rsrv, FPGAProcessor *fpga_proc);
      virtual ~FPGATaskScheduler(void);

    protected:
      virtual bool execute_task(Task *task);
      virtual void execute_internal_task(InternalTask *task);
      FPGAProcessor *fpga_proc_;
    };

    template <typename T>
    FPGATaskScheduler<T>::FPGATaskScheduler(Processor proc,
                                            Realm::CoreReservation &core_rsrv,
                                            FPGAProcessor *fpga_proc) : T(proc, core_rsrv), fpga_proc_(fpga_proc)
    {
    }

    template <typename T>
    FPGATaskScheduler<T>::~FPGATaskScheduler(void)
    {
    }

    template <typename T>
    bool FPGATaskScheduler<T>::execute_task(Task *task)
    {
      assert(ThreadLocal::current_fpga_proc == NULL);
      ThreadLocal::current_fpga_proc = fpga_proc_;
      log_fpga.info() << "execute_task " << task;
      bool ok = T::execute_task(task);
      assert(ThreadLocal::current_fpga_proc == fpga_proc_);
      ThreadLocal::current_fpga_proc = NULL;
      return ok;
    }

    template <typename T>
    void FPGATaskScheduler<T>::execute_internal_task(InternalTask *task)
    {
      assert(ThreadLocal::current_fpga_proc == NULL);
      ThreadLocal::current_fpga_proc = fpga_proc_;
      log_fpga.info() << "execute_internal_task";
      T::execute_internal_task(task);
      assert(ThreadLocal::current_fpga_proc == fpga_proc_);
      ThreadLocal::current_fpga_proc = NULL;
    }

    FPGAProcessor::FPGAProcessor(FPGADevice *fpga_device, Processor me, Realm::CoreReservationSet &crs)
        : LocalTaskProcessor(me, Processor::FPGA_PROC)
    {
      log_fpga.info() << "FPGAProcessor()";
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
      UserThreadTaskScheduler *sched = new FPGATaskScheduler<UserThreadTaskScheduler>(me, *core_rsrv_, this);
#else
      KernelThreadTaskScheduler *sched = new FPGATaskScheduler<KernelThreadTaskScheduler>(me, *core_rsrv_, this);
#endif
      set_scheduler(sched);
    }

    FPGAProcessor::~FPGAProcessor(void)
    {
      delete core_rsrv_;
    }

    FPGAProcessor *FPGAProcessor::get_current_fpga_proc(void)
    {
      return ThreadLocal::current_fpga_proc;
    }

    FPGADeviceMemory::FPGADeviceMemory(Memory memory, FPGADevice *device, void *base_ptr_sys, void *base_ptr_dev, size_t size)
        : LocalManagedMemory(memory, size, MKIND_FPGA, 512, Memory::FPGA_MEM, NULL), device(device), base_ptr_sys(base_ptr_sys), base_ptr_dev(base_ptr_dev)
    {
    }

    FPGADeviceMemory::~FPGADeviceMemory(void) {}

    void FPGADeviceMemory::get_bytes(off_t offset, void *dst, size_t size)
    {
      FPGACompletionEvent n; //TODO: fix me
      get_device()->copy_from_fpga(dst, offset, size, &n);
      n.request_completed();
    }

    void FPGADeviceMemory::put_bytes(off_t offset, const void *src, size_t size)
    {
      FPGACompletionEvent n; //TODO: fix me
      get_device()->copy_to_fpga(offset, src, size, &n);
      n.request_completed();
    }

    void *FPGADeviceMemory::get_direct_ptr(off_t offset, size_t size)
    {
      return (void *)((uint8_t *)base_ptr_sys + offset);
    }

    void FPGACompletionEvent::request_completed(void)
    {
      req->xd->notify_request_read_done(req);
      req->xd->notify_request_write_done(req);
    }

    FPGAXferDes::FPGAXferDes(uintptr_t _dma_op, Channel *_channel,
                             NodeID _launch_node, XferDesID _guid,
                             const std::vector<XferDesPortInfo> &inputs_info,
                             const std::vector<XferDesPortInfo> &outputs_info,
                             int _priority)
        : XferDes(_dma_op, _channel, _launch_node, _guid,
                  inputs_info, outputs_info,
                  _priority, 0, 0)
    {
      if ((inputs_info.size() >= 1) &&
          (input_ports[0].mem->kind == MemoryImpl::MKIND_FPGA))
      {
        // all input ports should agree on which fpga they target
        src_fpga = ((FPGADeviceMemory *)(input_ports[0].mem))->device;
        for (size_t i = 1; i < input_ports.size(); i++)
        {
          // exception: control and indirect ports should be readable from cpu
          if ((int(i) == input_control.control_port_idx) ||
              (int(i) == output_control.control_port_idx) ||
              input_ports[i].is_indirect_port)
          {
            assert((input_ports[i].mem->kind == MemoryImpl::MKIND_SYSMEM));
            continue;
          }
          assert(input_ports[i].mem == input_ports[0].mem);
        }
      }
      else
      {
        src_fpga = 0;
      }

      if ((outputs_info.size() >= 1) &&
          (output_ports[0].mem->kind == MemoryImpl::MKIND_FPGA))
      {
        // all output ports should agree on which adev they target
        dst_fpga = ((FPGADeviceMemory *)(output_ports[0].mem))->device;
        for (size_t i = 1; i < output_ports.size(); i++)
          assert(output_ports[i].mem == output_ports[0].mem);
      }
      else
      {
        dst_fpga = 0;
      }

      // if we're doing a multi-hop copy, we'll dial down the request
      //  sizes to improve pipelining
      bool multihop_copy = false;
      for (size_t i = 1; i < input_ports.size(); i++)
        if (input_ports[i].peer_guid != XFERDES_NO_GUID)
          multihop_copy = true;
      for (size_t i = 1; i < output_ports.size(); i++)
        if (output_ports[i].peer_guid != XFERDES_NO_GUID)
          multihop_copy = true;

      if (src_fpga != 0)
      {
        if (dst_fpga != 0)
        {
          if (src_fpga == dst_fpga)
          {
            kind = XFER_FPGA_IN_DEV;
            // ignore max_req_size value passed in - it's probably too small
            max_req_size = 1 << 30;
          }
          else
          {
            kind = XFER_FPGA_PEER_DEV;
            // ignore max_req_size value passed in - it's probably too small
            max_req_size = 256 << 20;
          }
        }
        else
        {
          kind = XFER_FPGA_FROM_DEV;
          if (multihop_copy)
            max_req_size = 4 << 20;
        }
      }
      else
      {
        if (dst_fpga != 0)
        {
          kind = XFER_FPGA_TO_DEV;
          if (multihop_copy)
            max_req_size = 4 << 20;
        }
        else
        {
          assert(0);
        }
      }

      const int max_nr = 10; // TODO:FIXME
      for (int i = 0; i < max_nr; i++)
      {
        FPGARequest *fpga_req = new FPGARequest;
        fpga_req->xd = this;
        fpga_req->event.req = fpga_req;
        available_reqs.push(fpga_req);
      }
    }

    long FPGAXferDes::get_requests(Request **requests, long nr)
    {
      FPGARequest **reqs = (FPGARequest **)requests;
      unsigned flags = (TransferIterator::LINES_OK |
                        TransferIterator::PLANES_OK);
      long new_nr = default_get_requests(requests, nr, flags);
      for (long i = 0; i < new_nr; i++)
      {
        switch (kind)
        {
        case XFER_FPGA_TO_DEV:
        {
          reqs[i]->src_base = input_ports[reqs[i]->src_port_idx].mem->get_direct_ptr(reqs[i]->src_off, reqs[i]->nbytes);
          assert(reqs[i]->src_base != 0);
          break;
        }
        case XFER_FPGA_FROM_DEV:
        {
          reqs[i]->dst_base = output_ports[reqs[i]->dst_port_idx].mem->get_direct_ptr(reqs[i]->dst_off, reqs[i]->nbytes);
          assert(reqs[i]->dst_base != 0);
          break;
        }
        case XFER_FPGA_IN_DEV:
        {
          break;
        }
        case XFER_FPGA_PEER_DEV:
        {
          reqs[i]->dst_fpga = dst_fpga;
          break;
        }
        default:
          assert(0);
        }
      }
      return new_nr;
    }

    bool FPGAXferDes::progress_xd(FPGAChannel *channel, TimeLimit work_until)
    {
      Request *rq;
      bool did_work = false;
      do
      {
        long count = get_requests(&rq, 1);
        if (count > 0)
        {
          channel->submit(&rq, count);
          did_work = true;
        }
        else
          break;
      } while (!work_until.is_expired());
      return did_work;
    }

    void FPGAXferDes::notify_request_read_done(Request *req)
    {
      default_notify_request_read_done(req);
    }

    void FPGAXferDes::notify_request_write_done(Request *req)
    {
      default_notify_request_write_done(req);
    }

    void FPGAXferDes::flush()
    {
    }

    FPGAChannel::FPGAChannel(FPGADevice *_src_fpga, XferDesKind _kind, BackgroundWorkManager *bgwork)
        : SingleXDQChannel<FPGAChannel, FPGAXferDes>(bgwork, _kind, "FPGA channel")
    {
      log_fpga.info() << "FPGAChannel(): " << (int)_kind;
      src_fpga = _src_fpga;

      Memory temp_fpga_mem = src_fpga->fpga_mem->me;
      Memory temp_sys_mem = src_fpga->local_sysmem->me;

      switch (_kind)
      {
      case XFER_FPGA_TO_DEV:
      {
        unsigned bw = 0; // TODO
        unsigned latency = 0;
        add_path(temp_sys_mem, temp_fpga_mem, bw, latency, false, false, XFER_FPGA_TO_DEV);
        break;
      }

      case XFER_FPGA_FROM_DEV:
      {
        unsigned bw = 0; // TODO
        unsigned latency = 0;
        add_path(temp_fpga_mem, temp_sys_mem, bw, latency, false, false, XFER_FPGA_FROM_DEV);
        break;
      }

      case XFER_FPGA_IN_DEV:
      {
        // self-path
        unsigned bw = 0; // TODO
        unsigned latency = 0;
        add_path(temp_fpga_mem, temp_fpga_mem, bw, latency, false, false, XFER_FPGA_IN_DEV);
        break;
      }

      case XFER_FPGA_PEER_DEV:
      {
        // just do paths to peers - they'll do the other side
        assert(0 && "not implemented");
        break;
      }

      default:
        assert(0);
      }
    }

    FPGAChannel::~FPGAChannel()
    {
    }

    XferDes *FPGAChannel::create_xfer_des(uintptr_t dma_op,
                                          NodeID launch_node,
                                          XferDesID guid,
                                          const std::vector<XferDesPortInfo> &inputs_info,
                                          const std::vector<XferDesPortInfo> &outputs_info,
                                          int priority,
                                          XferDesRedopInfo redop_info,
                                          const void *fill_data, size_t fill_size)
    {
      assert(redop_info.id == 0);
      assert(fill_size == 0);
      return new FPGAXferDes(dma_op, this, launch_node, guid,
                             inputs_info, outputs_info,
                             priority);
    }

    long FPGAChannel::submit(Request **requests, long nr)
    {
      for (long i = 0; i < nr; i++)
      {
        FPGARequest *req = (FPGARequest *)requests[i];
        // no serdez support
        assert(req->xd->input_ports[req->src_port_idx].serdez_op == 0);
        assert(req->xd->output_ports[req->dst_port_idx].serdez_op == 0);

        // empty transfers don't need to bounce off the ADevice
        if (req->nbytes == 0)
        {
          req->xd->notify_request_read_done(req);
          req->xd->notify_request_write_done(req);
          continue;
        }

        switch (req->dim)
        {
        case Request::DIM_1D:
        {
          switch (kind)
          {
          case XFER_FPGA_TO_DEV:
            src_fpga->copy_to_fpga(req->dst_off, req->src_base,
                                   req->nbytes, &req->event);
            break;
          case XFER_FPGA_FROM_DEV:
            src_fpga->copy_from_fpga(req->dst_base, req->src_off,
                                     req->nbytes, &req->event);
            break;
          case XFER_FPGA_IN_DEV:
            src_fpga->copy_within_fpga(req->dst_off, req->src_off,
                                       req->nbytes, &req->event);
            break;
          case XFER_FPGA_PEER_DEV:
            src_fpga->copy_to_peer(req->dst_fpga, req->dst_off,
                                   req->src_off, req->nbytes, &req->event);
            break;
          default:
            assert(0);
          }
          break;
        }

        case Request::DIM_2D:
        {
          switch (kind)
          {
          case XFER_FPGA_TO_DEV:
            assert(0 && "not implemented");
            break;
          case XFER_FPGA_FROM_DEV:
            assert(0 && "not implemented");
            break;
          case XFER_FPGA_IN_DEV:
            assert(0 && "not implemented");
            break;
          case XFER_FPGA_PEER_DEV:
            assert(0 && "not implemented");
            break;
          default:
            assert(0);
          }
          break;
        }

        case Request::DIM_3D:
        {
          switch (kind)
          {
          case XFER_FPGA_TO_DEV:
            assert(0 && "not implemented");
            break;
          case XFER_FPGA_FROM_DEV:
            assert(0 && "not implemented");
            break;
          case XFER_FPGA_IN_DEV:
            assert(0 && "not implemented");
            break;
          case XFER_FPGA_PEER_DEV:
            assert(0 && "not implemented");
            break;
          default:
            assert(0);
          }
          break;
        }

        default:
          assert(0);
        }

        //pending_copies.push_back(req);
      }

      return nr;
    }

    FPGAfillXferDes::FPGAfillXferDes(uintptr_t _dma_op, Channel *_channel,
                                     NodeID _launch_node, XferDesID _guid,
                                     const std::vector<XferDesPortInfo> &inputs_info,
                                     const std::vector<XferDesPortInfo> &outputs_info,
                                     int _priority,
                                     const void *_fill_data, size_t _fill_size)
        : XferDes(_dma_op, _channel, _launch_node, _guid,
                  inputs_info, outputs_info,
                  _priority, _fill_data, _fill_size)
    {
      kind = XFER_FPGA_IN_DEV;

      // no direct input data for us
      assert(input_control.control_port_idx == -1);
      input_control.current_io_port = -1;
    }

    long FPGAfillXferDes::get_requests(Request **requests, long nr)
    {
      // unused
      assert(0);
      return 0;
    }

    bool FPGAfillXferDes::progress_xd(FPGAfillChannel *channel,
                                      TimeLimit work_until)
    {
      bool did_work = false;
      ReadSequenceCache rseqcache(this, 2 << 20);
      WriteSequenceCache wseqcache(this, 2 << 20);

      while (true)
      {
        size_t min_xfer_size = 4096; // TODO: make controllable
        size_t max_bytes = get_addresses(min_xfer_size, &rseqcache);
        if (max_bytes == 0)
          break;

        XferPort *out_port = 0;
        size_t out_span_start = 0;
        if (output_control.current_io_port >= 0)
        {
          out_port = &output_ports[output_control.current_io_port];
          out_span_start = out_port->local_bytes_total;
        }

        size_t total_bytes = 0;
        if (out_port != 0)
        {
          // input and output both exist - transfer what we can
          log_fpga.info() << "memfill chunk: min=" << min_xfer_size
                          << " max=" << max_bytes;

          uintptr_t out_base = reinterpret_cast<uintptr_t>(out_port->mem->get_direct_ptr(0, 0));
          uintptr_t initial_out_offset = out_port->addrcursor.get_offset();
          while (total_bytes < max_bytes)
          {
            AddressListCursor &out_alc = out_port->addrcursor;

            uintptr_t out_offset = out_alc.get_offset();

            // the reported dim is reduced for partially consumed address
            //  ranges - whatever we get can be assumed to be regular
            int out_dim = out_alc.get_dim();

            size_t bytes = 0;
            size_t bytes_left = max_bytes - total_bytes;
            // memfills don't need to be particularly big to achieve
            //  peak efficiency, so trim to something that takes
            //  10's of us to be responsive to the time limit
            // NOTE: have to be a little careful and make sure the limit
            //  is a multiple of the fill size - we'll make it a power-of-2
            const size_t TARGET_CHUNK_SIZE = 256 << 10; // 256KB
            if (bytes_left > TARGET_CHUNK_SIZE)
            {
              size_t max_chunk = fill_size;
              while (max_chunk < TARGET_CHUNK_SIZE)
                max_chunk <<= 1;
              bytes_left = std::min(bytes_left, max_chunk);
            }

            if (out_dim > 0)
            {
              size_t ocount = out_alc.remaining(0);

              // contig bytes is always the first dimension
              size_t contig_bytes = std::min(ocount, bytes_left);

              // catch simple 1D case first
              if ((contig_bytes == bytes_left) ||
                  ((contig_bytes == ocount) && (out_dim == 1)))
              {
                bytes = contig_bytes;
                // we only have one element worth of data, so fill
                //  multiple elements by using a "2d" copy with a
                //  source stride of 0
                size_t repeat_count = contig_bytes / fill_size;
#ifdef DEBUG_REALM
                assert((contig_bytes % fill_size) == 0);
#endif
                fpga_memcpy_2d(out_base + out_offset, fill_size,
                               reinterpret_cast<uintptr_t>(fill_data), 0,
                               fill_size, repeat_count);
                out_alc.advance(0, bytes);
              }
              else
              {
                // grow to a 2D fill
                assert(0 && "FPGA 2D fill not implemented");
              }
            }
            else
            {
              // scatter adddress list
              assert(0);
            }

#ifdef DEBUG_REALM
            assert(bytes <= bytes_left);
#endif
            total_bytes += bytes;

            // stop if it's been too long, but make sure we do at least the
            //  minimum number of bytes
            if ((total_bytes >= min_xfer_size) && work_until.is_expired())
              break;
          }
          // TODO: sync bo here, will block, only work for 1D
          xclSyncBO(channel->fpga->dev_handle, channel->fpga->bo_handle, XCL_BO_SYNC_BO_TO_DEVICE, total_bytes, initial_out_offset);
        }
        else
        {
          // fill with no output, so just count the bytes
          total_bytes = max_bytes;
        }

        // mem fill is always immediate, so handle both skip and copy with
        //  the same code
        wseqcache.add_span(output_control.current_io_port,
                           out_span_start, total_bytes);
        out_span_start += total_bytes;

        bool done = record_address_consumption(total_bytes, total_bytes);

        did_work = true;

        if (done || work_until.is_expired())
          break;
      }

      rseqcache.flush();
      wseqcache.flush();

      return did_work;
    }

    FPGAfillChannel::FPGAfillChannel(FPGADevice *_fpga, BackgroundWorkManager *bgwork)
        : SingleXDQChannel<FPGAfillChannel, FPGAfillXferDes>(bgwork,
                                                             XFER_GPU_IN_FB,
                                                             "FPGA fill channel"),
          fpga(_fpga)
    {
      Memory temp_fpga_mem = fpga->fpga_mem->me;

      unsigned bw = 0; // TODO
      unsigned latency = 0;

      add_path(Memory::NO_MEMORY, temp_fpga_mem,
               bw, latency, false, false, XFER_FPGA_IN_DEV);

      xdq.add_to_manager(bgwork);
    }

    XferDes *FPGAfillChannel::create_xfer_des(uintptr_t dma_op,
                                              NodeID launch_node,
                                              XferDesID guid,
                                              const std::vector<XferDesPortInfo> &inputs_info,
                                              const std::vector<XferDesPortInfo> &outputs_info,
                                              int priority,
                                              XferDesRedopInfo redop_info,
                                              const void *fill_data, size_t fill_size)
    {
      assert(redop_info.id == 0);
      return new FPGAfillXferDes(dma_op, this, launch_node, guid,
                                 inputs_info, outputs_info,
                                 priority,
                                 fill_data, fill_size);
    }

    long FPGAfillChannel::submit(Request **requests, long nr)
    {
      // unused
      assert(0);
      return 0;
    }

  }; // namespace FPGA
};   // namespace Realm
