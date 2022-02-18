#include "realm/fpga/fpga_module.h"

#include "realm/logging.h"
#include "realm/cmdline.h"
#include "realm/utils.h"

#include <sys/stat.h>
#include <sys/mman.h>

// #define DEFLATE_XCLBIN "/home/xi/Programs/lz77/deflate.xclbin"
// #define INFLATE_XCLBIN "/home/xi/Programs/lz77/inflate.xclbin"
// #define DEFAULT_COMP_UNITS 4

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

        FPGAWorker::FPGAWorker(void)
            : BackgroundWorkItem("FPGA device worker"),
              condvar(lock),
              core_rsrv(0),
              worker_thread(0),
              thread_sleeping(false),
              worker_shutdown_requested(false)
        {
        }

        FPGAWorker::~FPGAWorker(void)
        {
            // shutdown should have already been called
            assert(worker_thread == 0);
        }

        void FPGAWorker::start_background_thread(
            Realm::CoreReservationSet &crs,
            size_t stack_size)
        {
            assert(manager == 0);
            core_rsrv = new Realm::CoreReservation("A worker thread", crs,
                                                   Realm::CoreReservationParameters());
            Realm::ThreadLaunchParameters tlp;
            worker_thread = Realm::Thread::create_kernel_thread<FPGAWorker, &FPGAWorker::thread_main>(this, tlp, *core_rsrv, 0);
        }

        void FPGAWorker::shutdown_background_thread(void)
        {
            {
                AutoLock<> al(lock);
                worker_shutdown_requested.store(true);
                if (thread_sleeping)
                {
                    thread_sleeping = false;
                    condvar.broadcast();
                }
            }

            worker_thread->join();
            delete worker_thread;
            worker_thread = 0;

            delete core_rsrv;
            core_rsrv = 0;
        }

        void FPGAWorker::add_queue(FPGAQueue *queue)
        {
            bool was_empty = false;
            {
                AutoLock<> al(lock);
#ifdef DEBUG_REALM
                // insist that the caller de-duplicate these
                for (ActiveQueue::iterator it = active_queues.begin();
                     it != active_queues.end();
                     ++it)
                    assert(*it != queue);
#endif
                was_empty = active_queues.empty();
                active_queues.push_back(queue);
                if (thread_sleeping)
                {
                    thread_sleeping = false;
                    condvar.broadcast();
                }
            }
            // if we're a background work item,
            // request attention if needed
            if (was_empty && (manager != 0))
                make_active();
        }

        void FPGAWorker::do_work(TimeLimit work_until)
        {
            // pop the first queue off the
            // list and immediately become re-active
            // if more queues remain
            FPGAQueue *queue = 0;
            bool still_not_empty = false;
            {
                AutoLock<> al(lock);

                assert(!active_queues.empty());
                queue = active_queues.front();
                active_queues.pop_front();
                still_not_empty = !active_queues.empty();
            }
            if (still_not_empty)
                make_active();
            // do work for the queue we popped,
            // paying attention to the cutoff
            // time
            bool requeue_q = false;

            if (queue->reap_events(work_until))
            {
                // still work (e.g. copies) to do
                if (work_until.is_expired())
                {
                    // out of time - save it for later
                    requeue_q = true;
                }
                // else if (queue->issue_copies(work_until))
                //     requeue_q = true;
            }

            bool was_empty = false;
            if (requeue_q)
            {
                AutoLock<> al(lock);
                was_empty = active_queues.empty();
                active_queues.push_back(queue);
            }
            // note that this can happen
            // even if we called make_active above!
            if (was_empty)
                make_active();
        }

        bool FPGAWorker::process_queues(bool sleep_on_empty)
        {
            FPGAQueue *cur_queue = 0;
            FPGAQueue *first_queue = 0;
            bool requeue_queue = false;
            while (true)
            {
                // grab the front queue in the list
                {
                    AutoLock<> al(lock);
                    // if we didn't finish work on the queue from the previous
                    // iteration, add it back to the end
                    if (requeue_queue)
                        active_queues.push_back(cur_queue);

                    while (active_queues.empty())
                    {
                        // sleep only if this was the
                        // first attempt to get a queue
                        if (sleep_on_empty && (first_queue == 0) &&
                            !worker_shutdown_requested.load())
                        {
                            thread_sleeping = true;
                            condvar.wait();
                        }
                        else
                            return false;
                    }
                    cur_queue = active_queues.front();
                    // did we wrap around?  if so, stop for now
                    if (cur_queue == first_queue)
                        return true;

                    active_queues.pop_front();
                    if (!first_queue)
                        first_queue = cur_queue;
                }
                // and do some work for it
                requeue_queue = false;
                // reap_events report whether any kind of work
                if (!cur_queue->reap_events(TimeLimit()))
                    continue;
                // if (!cur_queue->issue_copies(TimeLimit()))
                //     continue;
                // if we fall, the queues never went empty at any time, so it's up to us to requeue
                requeue_queue = true;
            }
        }

        void FPGAWorker::thread_main(void)
        {
            // TODO: consider busy-waiting in some cases to reduce latency?
            while (!worker_shutdown_requested.load())
            {
                bool work_left = process_queues(true);
                // if there was work left, yield our thread
                // for now to avoid a tight spin loop
                if (work_left)
                    Realm::Thread::yield();
            }
        }

        // ---------------------------------------------
        // class FPGAWorkFence - event to determine when a
        // device kernel completes execution
        // ---------------------------------------------
        FPGAWorkFence::FPGAWorkFence(Realm::Operation *op)
            : Realm::Operation::AsyncWorkItem(op)
        {
        }

        void FPGAWorkFence::request_cancellation(void)
        {
            // ignored - no way to shoot down FPGA work
        }

        void FPGAWorkFence::print(std::ostream &os) const
        {
            os << "FPGAWorkFence";
        }

        void FPGAWorkFence::enqueue(FPGAQueue *queue)
        {
            queue->add_fence(this);
        }

        //----------------------------------------------------
        // class FPGAQueue: device command queue
        //----------------------------------------------------
        FPGAQueue::FPGAQueue(FPGADevice *fpga_device, FPGAWorker *fpga_worker, cl::CommandQueue &command_queue)
            : fpga_device(fpga_device), fpga_worker(fpga_device->fpga_worker), command_queue(command_queue)
        {
            log_fpga.info() << "Create FPGAQueue ";
            pending_events.clear();
            pending_copies.clear();
        }

        FPGAQueue::~FPGAQueue(void)
        {
        }

        void
        FPGAQueue::add_fence(FPGAWorkFence *fence)
        {
            cl::Event opencl_event;
            cl_int err = 0;
            OCL_CHECK(err, err = command_queue.enqueueMarkerWithWaitList(nullptr, &opencl_event));
            add_event(opencl_event, fence, 0);
        }

        // add event to worker so it can be progressed
        void FPGAQueue::add_event(cl::Event opencl_event,
                                  FPGAWorkFence *fence,
                                  FPGADeviceCompletionNotification *n)

        {
            bool add_to_worker = false;
            // assert(opencl_event != nullptr);
            {
                AutoLock<> al(mutex);
                // remember to add ourselves
                // to the worker if we didn't already have work
                add_to_worker = pending_events.empty() &&
                                pending_copies.empty();
                PendingEvent e;
                e.opencl_event = opencl_event;
                e.fence = fence;
                e.notification = n;
                pending_events.push_back(e);
            }
            if (add_to_worker)
                fpga_worker->add_queue(this);
        }

        bool FPGAQueue::has_work(void) const
        {
            return (!pending_events.empty() ||
                    !pending_copies.empty());
        }

        bool FPGAQueue::reap_events(TimeLimit work_until)
        {
            // peek at the first event
            cl::Event opencl_event;
            FPGADeviceCompletionNotification *notification = 0;
            bool event_valid = false;
            {
                AutoLock<> al(mutex);

                if (pending_events.empty())
                    // no events left, but command queue
                    // might have other work left
                    return has_work();
                opencl_event = pending_events.front().opencl_event;
                notification = pending_events.front().notification;
                event_valid = true;
            }
            // we'll keep looking at events
            // until we find one that hasn't triggered
            bool work_left = true;
            while (event_valid)
            {
                cl_int err = 0;
                cl_int status;
                OCL_CHECK(err, err = opencl_event.getInfo(CL_EVENT_COMMAND_EXECUTION_STATUS, &status));
                if (status == CL_QUEUED || status == CL_SUBMITTED || status == CL_RUNNING)
                {
                    // event is not finished - check again later
                    return true;
                }
                else if (status != CL_COMPLETE)
                {
                    log_fpga.fatal() << "Error reported on FPGA " << fpga_device->name;
                }

                // this event has triggered
                FPGAWorkFence *fence = 0;
                {
                    AutoLock<> al(mutex);

                    const PendingEvent &e = pending_events.front();
                    assert(e.opencl_event == opencl_event);
                    fence = e.fence;
                    notification = e.notification;
                    pending_events.pop_front();

                    if (pending_events.empty())
                    {
                        event_valid = false;
                        work_left = has_work();
                    }
                    else
                    {
                        opencl_event = pending_events.front().opencl_event;
                    }
                }
                if (fence)
                {
                    fence->mark_finished(true /*successful*/); // set preconditions for next tasks
                }
                if (notification)
                {
                    notification->request_completed();
                }
                if (event_valid && work_until.is_expired())
                    return true;
            }
            // if we get here, we ran out of events, but there might have been
            // other kinds of work that we need to let the caller know about
            return work_left;
        }

        // void FPGAQueue::add_copy(FPGADeviceMemcpy *copy)
        // {
        //     bool add_to_worker = false;
        //     {
        //         AutoLock<> al(mutex);
        //         // add if we haven't been added yet
        //         add_to_worker =
        //             pending_copies.empty() && pending_events.empty();
        //         pending_copies.push_back(copy);
        //     }
        //     if (add_to_worker)
        //         fpga_worker->add_queue(this);
        // }

        // bool FPGAQueue::issue_copies(TimeLimit work_until)
        // {
        //     while (true)
        //     {
        //         // if we cause the list to go empty,
        //         // we stop even if more copies show
        //         // up because we don't want to requeue ourselves twice
        //         bool list_exhausted = false;
        //         FPGADeviceMemcpy *copy = 0;
        //         {
        //             AutoLock<> al(mutex);
        //             if (pending_copies.empty())
        //                 // no copies left,
        //                 // but queue might have other work left
        //                 return has_work();
        //             copy = pending_copies.front();
        //             pending_copies.pop_front();
        //             list_exhausted = !has_work();
        //         }
        //         copy->execute(this);
        //         delete copy;
        //         // if the list was exhausted, let the caller know
        //         if (list_exhausted)
        //             return false;

        //         // if we still have work, but time's up, return also
        //         if (work_until.is_expired())
        //             return true;
        //     }
        //     return false; // should never reach here
        // }

        // ---------------------------------------------
        // class FPGADevice
        // ---------------------------------------------
        FPGADevice::FPGADevice(cl::Device device, std::string name, std::string xclbin, FPGAWorker *fpga_worker) : name(name), fpga_worker(fpga_worker)
        {
            cl_int err;
            this->device = device;
            // Create FPGA context and command queue for each device
            OCL_CHECK(err, context = cl::Context(device, nullptr, nullptr, nullptr, &err));
            OCL_CHECK(err, command_queue = cl::CommandQueue(context, device,
                                                            CL_QUEUE_PROFILING_ENABLE | CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE, &err));

            // Program the device
            auto fileBuf = xcl::read_binary_file(xclbin);
            cl::Program::Binaries bins{{fileBuf.data(), fileBuf.size()}};
            log_fpga.info() << "Trying to program device " << device.getInfo<CL_DEVICE_NAME>();
            OCL_CHECK(err, program = cl::Program(context, {device}, bins, nullptr, &err));

            fpga_mem = nullptr;
            local_sysmem = nullptr;
            local_ibmem = nullptr;
            fpga_queue = nullptr;
            create_fpga_queues();
        }

        FPGADevice::~FPGADevice()
        {
            //   xclUnmapBO(this->dev_handle, this->bo_handle, this->fpga_mem->base_ptr_sys);
            //   xclFreeBO(this->dev_handle, this->bo_handle);
            if (fpga_queue != nullptr)
            {
                delete fpga_queue;
                fpga_queue = nullptr;
            }
            command_queue.finish();
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
                if ((*it)->lowlevel_kind == Memory::SYSTEM_MEM)
                {
                    this->local_sysmem = *it;
                    log_fpga.info() << "local_sysmem " << std::hex << (*it)->me.id << std::dec << " kind: " << (*it)->kind << " low-level kind: " << (*it)->lowlevel_kind;
                    break;
                }
            }

            const std::vector<IBMemory *> &local_ib_mems = runtime->nodes[Network::my_node_id].ib_memories;
            for (std::vector<Realm::IBMemory *>::const_iterator it = local_ib_mems.begin();
                 it != local_ib_mems.end();
                 it++)
            {
                if ((*it)->lowlevel_kind == Memory::REGDMA_MEM)
                {
                    this->local_ibmem = *it;
                    log_fpga.info() << "local_ibmem " << std::hex << (*it)->me.id << std::dec << " kind: " << (*it)->kind << " low-level kind: " << (*it)->lowlevel_kind;
                    break;
                }
            }

            runtime->add_dma_channel(new FPGAfillChannel(this, &runtime->bgwork));
            runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_IN_DEV, &runtime->bgwork));
            runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_TO_DEV, &runtime->bgwork));
            runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_FROM_DEV, &runtime->bgwork));
            runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_TO_DEV_COMP, &runtime->bgwork));
            runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_FROM_DEV_COMP, &runtime->bgwork));

            Machine::MemoryMemoryAffinity mma;
            mma.m1 = fpga_mem->me;
            mma.m2 = local_sysmem->me;
            mma.bandwidth = 20; // TODO
            mma.latency = 200;
            runtime->add_mem_mem_affinity(mma);

            mma.m1 = fpga_mem->me;
            mma.m2 = local_ibmem->me;
            mma.bandwidth = 20; // TODO
            mma.latency = 200;
            runtime->add_mem_mem_affinity(mma);

            // TODO: create p2p channel
            // runtime->add_dma_channel(new FPGAChannel(this, XFER_FPGA_PEER_DEV, &runtime->bgwork));
        }

        void FPGADevice::create_fpga_mem(RuntimeImpl *runtime, size_t size)
        {
            // TODO: only use membank 0 for now
            // buff = xclAllocBO(dev_handle, size, 0, cur_fpga_mem_bank);
            // void *base_ptr_sys = (void *)xclMapBO(dev_handle, bo_handle, true);
            // struct xclBOProperties bo_prop;
            // xclGetBOProperties(dev_handle, bo_handle, &bo_prop);
            // uint64_t base_ptr_dev = bo_prop.paddr;
            cl_int err;
            void *base_ptr_sys = nullptr;
            posix_memalign((void **)&base_ptr_sys, 4096, size);
            OCL_CHECK(err, buff = cl::Buffer(context, CL_MEM_READ_WRITE | CL_MEM_USE_HOST_PTR, size, base_ptr_sys, &err));
            Memory m = runtime->next_local_memory_id();
            this->fpga_mem = new FPGADeviceMemory(m, this, base_ptr_sys, size);
            runtime->add_memory(fpga_mem);
            log_fpga.info() << "create_fpga_mem: "
                            << device.getInfo<CL_DEVICE_NAME>()
                            << ", size = "
                            << (size >> 20) << " MB"
                            << ", base_ptr_sys = " << base_ptr_sys;
        }

        void FPGADevice::create_fpga_queues()
        {
            fpga_queue = new FPGAQueue(this, fpga_worker, command_queue);
        }

        void FPGADevice::copy_to_fpga(off_t dst_offset, const void *src, size_t bytes, FPGACompletionEvent *event)
        {
            size_t dst = reinterpret_cast<size_t>((uint8_t *)(fpga_mem->base_ptr_sys) + dst_offset);
            log_fpga.info() << "copy_to_fpga: src = " << src << " dst_offset = " << dst_offset
                            << " bytes = " << bytes << " dst = " << (int *)dst << "\n";
            // fpga_memcpy_2d(reinterpret_cast<uintptr_t>(dst), 0,
            //                reinterpret_cast<uintptr_t>(src), 0,
            //                bytes, 1);
            cl_int err = 0;
            // cl_buffer_region buff_copy_info = {(size_t)dst_offset, bytes};
            // cl::Buffer temp_copy_buff;
            // OCL_CHECK(err, temp_copy_buff = buff.createSubBuffer(CL_MEM_HOST_WRITE_ONLY, CL_BUFFER_CREATE_TYPE_REGION, &buff_copy_info, nullptr));
            // OCL_CHECK(err, err = command_queue.enqueueMigrateMemObjects({temp_copy_buff}, 0 /* 0 means from host*/));
            // OCL_CHECK(err, err = command_queue.finish());
            OCL_CHECK(err, err = command_queue.enqueueWriteBuffer(buff,        // buffer on the FPGA
                                                                  CL_TRUE,     // blocking call
                                                                  dst_offset,  // buffer offset in bytes
                                                                  bytes,       // Size in bytes
                                                                  (void *)src, // Pointer to the data to copy
                                                                  nullptr, nullptr));

            // xclSyncBO(this->dev_handle, this->bo_handle, XCL_BO_SYNC_BO_TO_DEVICE, bytes, dst_offset);

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
            FPGARequest *req = event->req;
            XferDes::XferPort *in_port = &req->xd->input_ports[req->src_port_idx];
            XferDes::XferPort *out_port = &req->xd->output_ports[req->dst_port_idx];
            in_port->iter->confirm_step();
            out_port->iter->confirm_step();

            event->request_completed();
        }

        void FPGADevice::copy_from_fpga(void *dst, off_t src_offset, size_t bytes, FPGACompletionEvent *event)
        {
            size_t src = reinterpret_cast<size_t>((uint8_t *)(fpga_mem->base_ptr_sys) + src_offset);
            log_fpga.info() << "copy_from_fpga: dst = " << dst << " src_offset = " << src_offset
                            << " bytes = " << bytes << " src = " << (int *)src << "\n";
            // xclSyncBO(this->dev_handle, this->bo_handle, XCL_BO_SYNC_BO_FROM_DEVICE, bytes, src_offset);
            // cl::Event async_event;
            // command_queue.enqueueReadBuffer(buff,                                                              // buffer on the FPGA
            //                                 CL_FALSE,                                                                         // blocking call
            //                                 src_offset,                                                               // buffer offset in bytes
            //                                 bytes,                                                                      // Size in bytes
            //                                 (void *)src, // Pointer to the data to copy
            //                                 nullptr, &async_event);
            // async_event.wait();

            cl_int err = 0;
            // cl_buffer_region buff_copy_info = {(size_t)src_offset, bytes};
            // cl::Buffer temp_copy_buff;
            // OCL_CHECK(err, temp_copy_buff = buff.createSubBuffer(CL_MEM_HOST_READ_ONLY, CL_BUFFER_CREATE_TYPE_REGION, &buff_copy_info, nullptr));
            // OCL_CHECK(err, err = command_queue.enqueueMigrateMemObjects({temp_copy_buff}, CL_MIGRATE_MEM_OBJECT_HOST));
            // OCL_CHECK(err, err = command_queue.finish());
            OCL_CHECK(err, err = command_queue.enqueueReadBuffer(buff,       // buffer on the FPGA
                                                                 CL_TRUE,    // blocking call
                                                                 src_offset, // buffer offset in bytes
                                                                 bytes,      // Size in bytes
                                                                 dst,        // Pointer to the data to copy
                                                                 nullptr, nullptr));

            // fpga_memcpy_2d(reinterpret_cast<uintptr_t>(dst), 0,
            //                reinterpret_cast<uintptr_t>(src), 0,
            //                bytes, 1);
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

            printf("copy_from_fpga: ");
            for (int i = 0; i < 40; i++)
            {
                printf("%d ", ((int *)(fpga_mem->base_ptr_sys))[i]);
            }
            printf("\n");
            for (int i = 0; i < 40; i++)
            {
                printf("%d-", ((int *)dst)[i]);
            }
            printf("\n");
            FPGARequest *req = event->req;
            XferDes::XferPort *in_port = &req->xd->input_ports[req->src_port_idx];
            XferDes::XferPort *out_port = &req->xd->output_ports[req->dst_port_idx];
            in_port->iter->confirm_step();
            out_port->iter->confirm_step();

            event->request_completed();
        }

        void FPGADevice::copy_within_fpga(off_t dst_offset, off_t src_offset, size_t bytes, FPGACompletionEvent *event)
        {
            log_fpga.info() << "copy_within_fpga(not implemented!): dst_offset = " << dst_offset << " src_offset = " << src_offset
                            << " bytes = " << bytes << "\n";
            FPGARequest *req = event->req;
            XferDes::XferPort *in_port = &req->xd->input_ports[req->src_port_idx];
            XferDes::XferPort *out_port = &req->xd->output_ports[req->dst_port_idx];
            in_port->iter->confirm_step();
            out_port->iter->confirm_step();
            event->request_completed();
        }

        void FPGADevice::copy_to_peer(FPGADevice *dst, off_t dst_offset, off_t src_offset, size_t bytes, FPGACompletionEvent *event)
        {
            log_fpga.info() << "copy_to_peer(not implemented!): dst = " << dst << " dst_offset = " << dst_offset
                            << " bytes = " << bytes << " event = " << event << "\n";
            FPGARequest *req = event->req;
            XferDes::XferPort *in_port = &req->xd->input_ports[req->src_port_idx];
            XferDes::XferPort *out_port = &req->xd->output_ports[req->dst_port_idx];
            in_port->iter->confirm_step();
            out_port->iter->confirm_step();
            event->request_completed();
        }

        // decompress data after moving data from ib memory to fpga device memory
        void FPGADevice::copy_to_fpga_comp(off_t dst_offset, const void *src, size_t bytes, FPGACompletionEvent *event)
        {
            // size_t i;
            size_t dst = reinterpret_cast<size_t>((uint8_t *)(fpga_mem->base_ptr_sys) + dst_offset);
            int *temp = (int *)dst;
            log_fpga.info() << "copy_to_fpga_comp: src = " << src << " dst_offset = " << dst_offset
                            << " bytes = " << bytes << " dst = " << temp << "\n";

            FPGARequest *req = event->req;
            XferDes::XferPort *in_port = &req->xd->input_ports[req->src_port_idx];
            XferDes::XferPort *out_port = &req->xd->output_ports[req->dst_port_idx];
            out_port->iter->confirm_step();
            in_port->iter->cancel_step();
            TransferIterator::AddressInfo src_info;
            size_t bytes_avail = in_port->iter->step(bytes,
                                                     src_info,
                                                     0,
                                                     false /*!tentative*/);
            log_fpga.info() << "(src_info) base_offset: " << src_info.base_offset
                            << " bytes_per_chunk: " << src_info.bytes_per_chunk
                            << " num_lines: " << src_info.num_lines
                            << " line_stride: " << src_info.line_stride
                            << " num_planes: " << src_info.num_planes
                            << " plane_stride: " << src_info.plane_stride
                            << " bytes_avail: " << bytes_avail;

            // // create a temp BO to store compressed data
            // xclBufferHandle bo_temp;
            // bo_temp = xclAllocBO(this->dev_handle, bytes, 0, 1);
            // char *bo_temp_host;
            // bo_temp_host = (char *)xclMapBO(this->dev_handle, bo_temp, true);
            // uint64_t bo_temp_dev;
            // struct xclBOProperties bo_prop;
            // xclGetBOProperties(this->dev_handle, bo_temp, &bo_prop);
            // bo_temp_dev = bo_prop.paddr;

            // fpga_memcpy_2d(reinterpret_cast<uintptr_t>(bo_temp_host), 0,
            //                reinterpret_cast<uintptr_t>(src), 0,
            //                bytes, 1);
            // xclSyncBO(this->dev_handle, bo_temp, XCL_BO_SYNC_BO_TO_DEVICE, bytes, 0);

            // printf("copy_to_fpga_comp before decompression: ");
            // for (i = 0; i < bytes; i++)
            // {
            //   if (bo_temp_host[i] == '\0')
            //   {
            //     printf("_");
            //   }
            //   else
            //   {
            //     printf("%c", bo_temp_host[i]);
            //   }
            // }
            // printf("\n");

            // //load xclbin
            // int fd = open(INFLATE_XCLBIN, O_RDONLY);
            // struct stat st;
            // fstat(fd, &st);
            // size_t xclbin_size = st.st_size;
            // struct axlf *xclbin = (struct axlf *)mmap(NULL, xclbin_size, PROT_READ, MAP_PRIVATE, fd, 0);
            // uuid_t xclbin_uuid;
            // memcpy(xclbin_uuid, xclbin->m_header.uuid, sizeof(uuid_t));
            // xclLoadXclBin(this->dev_handle, (const struct axlf *)xclbin);
            // munmap(xclbin, xclbin_size);
            // close(fd);

            // size_t num_compute_units = DEFAULT_COMP_UNITS;
            // for (i = 0; i < num_compute_units; i++)
            // {
            //   xclOpenContext(this->dev_handle, xclbin_uuid, (unsigned int)i, true);
            // }

            // uint64_t p_base_dev = (uint64_t)this->fpga_mem->base_ptr_dev;
            // uint64_t p_inflate_in = bo_temp_dev;
            // uint64_t p_inflate_out = p_base_dev + dst_offset;
            // log_fpga.print("p_inflate_in = %zu p_inflate_out = %zu\n", p_inflate_in, p_inflate_out);

            // xclBufferHandle bo_cmd = xclAllocBO(this->dev_handle, (size_t)CMD_SIZE, 0, XCL_BO_FLAGS_EXECBUF);
            // struct ert_start_kernel_cmd *start_cmd = (struct ert_start_kernel_cmd *)xclMapBO(this->dev_handle, bo_cmd, true);
            // memset(start_cmd, 0, CMD_SIZE);
            // start_cmd->state = ERT_CMD_STATE_NEW;
            // start_cmd->opcode = ERT_START_CU;
            // start_cmd->stat_enabled = 1;
            // start_cmd->count = ARG_INFLATE_OUTPUT_OFFSET / 4 + 3;
            // start_cmd->cu_mask = (1 << num_compute_units) - 1; /* CU 0, 1, 2 */
            // start_cmd->data[ARG_INFLATE_INPUT_OFFSET / 4] = p_inflate_in;
            // start_cmd->data[ARG_INFLATE_INPUT_OFFSET / 4 + 1] = (p_inflate_in >> 32) & 0xFFFFFFFF;
            // start_cmd->data[ARG_INFLATE_OUTPUT_OFFSET / 4] = p_inflate_out;
            // start_cmd->data[ARG_INFLATE_OUTPUT_OFFSET / 4 + 1] = (p_inflate_out >> 32) & 0xFFFFFFFF;

            // xclExecBuf(this->dev_handle, bo_cmd);
            // struct ert_packet *cmd_packet = (struct ert_packet *)start_cmd;
            // while (cmd_packet->state != ERT_CMD_STATE_COMPLETED)
            // {
            //   xclExecWait(this->dev_handle, 1000);
            // }

            // xclUnmapBO(this->dev_handle, bo_cmd, start_cmd);
            // xclFreeBO(this->dev_handle, bo_cmd);
            // for (i = 0; i < num_compute_units; i++)
            // {
            //   xclCloseContext(this->dev_handle, xclbin_uuid, (unsigned int)i);
            // }

            // // copy data from fpga device memory for testing
            // xclSyncBO(this->dev_handle, this->bo_handle, XCL_BO_SYNC_BO_FROM_DEVICE, bytes, dst_offset);

            // printf("copy_to_fpga_comp: ");
            // for (i = 0; i < 6000; i++)
            // {
            //   if (((char *)fpga_mem->base_ptr_sys)[i] == '\0') {
            //     printf("_");
            //   }
            //   else {
            //     printf("%c", ((char *)fpga_mem->base_ptr_sys)[i]);
            //   }
            // }
            // printf("\n");
            event->request_completed();
        }

        // compress data before moving data from fpga device memory to ib memory
        void FPGADevice::copy_from_fpga_comp(void *dst, off_t src_offset, size_t bytes, FPGACompletionEvent *event)
        {
            // size_t i;
            size_t src = reinterpret_cast<size_t>((uint8_t *)(fpga_mem->base_ptr_sys) + src_offset);
            int *temp = (int *)src;
            log_fpga.info() << "copy_from_fpga_comp: dst = " << dst << " src_offset = " << src_offset
                            << " bytes = " << bytes << " src = " << temp << "\n";

            // // create a temp BO to store compressed data
            // xclBufferHandle bo_temp;
            // bo_temp = xclAllocBO(this->dev_handle, bytes, 0, 1);
            // char *bo_temp_host;
            // bo_temp_host = (char *)xclMapBO(this->dev_handle, bo_temp, true);
            // uint64_t bo_temp_dev;
            // struct xclBOProperties bo_prop;
            // xclGetBOProperties(this->dev_handle, bo_temp, &bo_prop);
            // bo_temp_dev = bo_prop.paddr;

            // //load xclbin
            // int fd = open(DEFLATE_XCLBIN, O_RDONLY);
            // struct stat st;
            // fstat(fd, &st);
            // size_t xclbin_size = st.st_size;
            // struct axlf *xclbin = (struct axlf *)mmap(NULL, xclbin_size, PROT_READ, MAP_PRIVATE, fd, 0);
            // uuid_t xclbin_uuid;
            // memcpy(xclbin_uuid, xclbin->m_header.uuid, sizeof(uuid_t));
            // xclLoadXclBin(this->dev_handle, (const struct axlf *)xclbin);
            // munmap(xclbin, xclbin_size);
            // close(fd);

            // size_t num_compute_units = DEFAULT_COMP_UNITS;
            // for (i = 0; i < num_compute_units; i++)
            // {
            //   xclOpenContext(this->dev_handle, xclbin_uuid, (unsigned int)i, true);
            // }

            // uint64_t p_base_dev = (uint64_t)this->fpga_mem->base_ptr_dev;
            // uint64_t p_deflate_in = p_base_dev + src_offset;
            // uint64_t p_deflate_out = bo_temp_dev;
            // log_fpga.print("p_deflate_in = %lu p_deflate_out = %lu\n", p_deflate_in, p_deflate_out);

            // xclBufferHandle bo_cmd = xclAllocBO(this->dev_handle, (size_t)CMD_SIZE, 0, XCL_BO_FLAGS_EXECBUF);
            // struct ert_start_kernel_cmd *start_cmd = (struct ert_start_kernel_cmd *)xclMapBO(this->dev_handle, bo_cmd, true);
            // memset(start_cmd, 0, CMD_SIZE);
            // start_cmd->state = ERT_CMD_STATE_NEW;
            // start_cmd->opcode = ERT_START_CU;
            // start_cmd->stat_enabled = 1;
            // start_cmd->count = ARG_DEFLATE_SIZE_OFFSET / 4 + 3;
            // start_cmd->cu_mask = (1 << num_compute_units) - 1; /* CU 0, 1, 2 */
            // start_cmd->data[ARG_DEFLATE_INPUT_OFFSET / 4] = p_deflate_in;
            // start_cmd->data[ARG_DEFLATE_INPUT_OFFSET / 4 + 1] = (p_deflate_in >> 32) & 0xFFFFFFFF;
            // start_cmd->data[ARG_DEFLATE_OUTPUT_OFFSET / 4] = p_deflate_out;
            // start_cmd->data[ARG_DEFLATE_OUTPUT_OFFSET / 4 + 1] = (p_deflate_out >> 32) & 0xFFFFFFFF;
            // start_cmd->data[ARG_DEFLATE_SIZE_OFFSET / 4] = bytes;
            // start_cmd->data[ARG_DEFLATE_SIZE_OFFSET / 4 + 1] = (bytes >> 32) & 0xFFFFFFFF;

            // xclExecBuf(this->dev_handle, bo_cmd);
            // struct ert_packet *cmd_packet = (struct ert_packet *)start_cmd;
            // while (cmd_packet->state != ERT_CMD_STATE_COMPLETED)
            // {
            //   xclExecWait(this->dev_handle, 1000);
            // }

            // xclUnmapBO(this->dev_handle, bo_cmd, start_cmd);
            // xclFreeBO(this->dev_handle, bo_cmd);
            // for (i = 0; i < num_compute_units; i++)
            // {
            //   xclCloseContext(this->dev_handle, xclbin_uuid, (unsigned int)i);
            // }

            // xclSyncBO(this->dev_handle, bo_temp, XCL_BO_SYNC_BO_FROM_DEVICE, bytes, 0);
            size_t new_bytes = bytes;
            // while (bo_temp_host[new_bytes] != '\0')
            // {
            //   if (bo_temp_host[new_bytes] == '@')
            //   {
            //     new_bytes += 4;
            //   }
            //   else
            //   {
            //     new_bytes++;
            //   }
            // }
            // new_bytes++;
            // printf("new_bytes = %lu\n", new_bytes);

            // fpga_memcpy_2d(reinterpret_cast<uintptr_t>(dst), 0,
            //                reinterpret_cast<uintptr_t>(bo_temp_host), 0,
            //                new_bytes, 1);

            // printf("copy_from_fpga_comp: ");
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
            FPGARequest *req = event->req;
            XferDes::XferPort *in_port = &req->xd->input_ports[req->src_port_idx];
            XferDes::XferPort *out_port = &req->xd->output_ports[req->dst_port_idx];
            in_port->iter->confirm_step();
            if (new_bytes == bytes)
            {
                out_port->iter->confirm_step();
            }
            else
            {
                out_port->iter->cancel_step();
                TransferIterator::AddressInfo dst_info;
                size_t bytes_avail = out_port->iter->step(new_bytes,
                                                          dst_info,
                                                          0,
                                                          false /*!tentative*/);
                assert(bytes_avail == new_bytes);
                out_port->local_bytes_total = new_bytes;
                out_port->local_bytes_cons.store(out_port->local_bytes_total); // completion detection uses this
                req->xd->update_bytes_write(0, req->xd->output_ports[0].local_bytes_total, 0);
                // size_t rewind_dst = bytes - new_bytes;
                // out_port->local_bytes_cons.fetch_sub(rewind_dst);
                // req->xd->update_bytes_write(0, new_bytes, 0);
                req->write_seq_count = out_port->local_bytes_total;
                log_fpga.info() << "(dst_info) base_offset: " << dst_info.base_offset
                                << " bytes_per_chunk: " << dst_info.bytes_per_chunk
                                << " num_lines: " << dst_info.num_lines
                                << " line_stride: " << dst_info.line_stride
                                << " num_planes: " << dst_info.num_planes
                                << " plane_stride: " << dst_info.plane_stride
                                << " local_bytes_total: " << out_port->local_bytes_total
                                << " write_seq_pos: " << req->write_seq_count
                                << " write_seq_count: " << req->write_seq_pos;
            }
            event->request_completed();
        }

        FPGAModule::FPGAModule() : Module("fpga"), cfg_num_fpgas(0), cfg_use_worker_threads(false), cfg_use_shared_worker(true), cfg_fpga_mem_size(4 << 20)
        {
            shared_worker = nullptr;
            cfg_fpga_xclbin = "";
            fpga_devices.clear();
            dedicated_workers.clear();
            fpga_procs_.clear();
        }

        FPGAModule::~FPGAModule(void)
        {
            if (!this->fpga_devices.empty())
            {
                for (size_t i = 0; i < fpga_devices.size(); i++)
                {

                    // xclClose(fpga_devices[i]->dev_handle);
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
                cp.add_option_bool("-ll:fpga_work_thread", m->cfg_use_worker_threads);
                cp.add_option_bool("-ll:fpga_shared_worker", m->cfg_use_shared_worker);
                cp.add_option_int_units("-ll:fpga_size", m->cfg_fpga_mem_size, 'm');
                cp.add_option_string("-ll:fpga_xclbin", m->cfg_fpga_xclbin);

                bool ok = cp.parse_command_line(cmdline);
                if (!ok)
                {
                    log_fpga.error() << "error reading fpga parameters";
                    exit(1);
                }
            }
            return m;
        }

        // do any general initialization - this is called after all configuration is
        //  complete
        void FPGAModule::initialize(RuntimeImpl *runtime)
        {
            log_fpga.info() << "initialize";
            Module::initialize(runtime);

            std::vector<cl::Device> devices = xcl::get_xil_devices();
            if (cfg_num_fpgas > devices.size())
            {
                log_fpga.error() << cfg_num_fpgas << " FPGA Processors requested, but only " << devices.size() << " available!";
                exit(1);
            }

            // if we are using a shared worker, create that next
            if (cfg_use_shared_worker)
            {
                shared_worker = new FPGAWorker;
                if (cfg_use_worker_threads)
                {
                    shared_worker->start_background_thread(
                        runtime->core_reservation_set(),
                        1 << 20); // hardcoded worker stack size
                }
                else
                    shared_worker->add_to_manager(&(runtime->bgwork));
            }

            // set up fpga_devices
            for (unsigned int i = 0; i < cfg_num_fpgas; i++)
            {
                // for each device record the FPGAWorker
                FPGAWorker *worker;
                if (cfg_use_shared_worker)
                {
                    worker = shared_worker;
                }
                else
                {
                    worker = new FPGAWorker;
                    if (cfg_use_worker_threads)
                        worker->start_background_thread(
                            runtime->core_reservation_set(),
                            1 << 20); // hardcoded worker stack size
                    else
                        worker->add_to_manager(&(runtime->bgwork));
                }

                FPGADevice *fpga_device = new FPGADevice(devices[i], "fpga" + std::to_string(i), cfg_fpga_xclbin, worker);
                fpga_devices.push_back(fpga_device);

                if (!cfg_use_shared_worker) 
                {
                    log_fpga.info() << "add to dedicated workers " << worker;
                    dedicated_workers[fpga_device] = worker;
                }
            }
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
        //  (each new ProcessorImpl should use a Processor from RuntimeImpl::next_local_processor_id)
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
            // clean up worker(s)
            if (shared_worker)
            {
#ifdef DEBUG_REALM
                shared_worker->shutdown_work_item();
#endif
                if (cfg_use_worker_threads)
                    shared_worker->shutdown_background_thread();

                delete shared_worker;
                shared_worker = 0;
            }
            for (std::map<FPGADevice *, FPGAWorker *>::iterator it = dedicated_workers.begin();
                 it != dedicated_workers.end();
                 it++)
            {
                log_fpga.info() << "shutdown worker in cleanup";
                FPGAWorker *worker = it->second;
#ifdef DEBUG_REALM
                worker->shutdown_work_item();
#endif
                if (cfg_use_worker_threads)
                    worker->shutdown_background_thread();

                delete worker;
            }
            dedicated_workers.clear();
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
            FPGAQueue *queue = fpga_proc_->fpga_device->fpga_queue;
            log_fpga.info() << "execute_task " << task;

            // we'll use a "work fence" to track when the kernels launched by this task actually
            //  finish - this must be added to the task _BEFORE_ we execute
            FPGAWorkFence *fence = new FPGAWorkFence(task);
            task->add_async_work_item(fence);
            bool ok = T::execute_task(task);
            fence->enqueue(queue);

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

        FPGADeviceMemory::FPGADeviceMemory(Memory memory, FPGADevice *device, void *base_ptr_sys, size_t size)
            : LocalManagedMemory(memory, size, MKIND_FPGA, 512, Memory::FPGA_MEM, NULL), base_ptr_sys(base_ptr_sys)
        {
        }

        FPGADeviceMemory::~FPGADeviceMemory(void) 
        {
            // this function is invoked before ~FPGADevice
            if (base_ptr_sys != nullptr)
            {
                free(base_ptr_sys);
                base_ptr_sys = nullptr;
            }
        }

        void FPGADeviceMemory::get_bytes(off_t offset, void *dst, size_t size)
        {
            FPGACompletionEvent n; // TODO: fix me
            get_device()->copy_from_fpga(dst, offset, size, &n);
            n.request_completed();
        }

        void FPGADeviceMemory::put_bytes(off_t offset, const void *src, size_t size)
        {
            FPGACompletionEvent n; // TODO: fix me
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
                                 int _priority, XferDesKind kind)
            : XferDes(_dma_op, _channel, _launch_node, _guid,
                      inputs_info, outputs_info,
                      _priority, 0, 0)
        {
            // if ((inputs_info.size() >= 1) &&
            //     (input_ports[0].mem->kind == MemoryImpl::MKIND_FPGA))
            // {
            //   // all input ports should agree on which fpga they target
            //   src_fpga = ((FPGADeviceMemory *)(input_ports[0].mem))->device;
            //   for (size_t i = 1; i < input_ports.size(); i++)
            //   {
            //     // exception: control and indirect ports should be readable from cpu
            //     if ((int(i) == input_control.control_port_idx) ||
            //         (int(i) == output_control.control_port_idx) ||
            //         input_ports[i].is_indirect_port)
            //     {
            //       assert((input_ports[i].mem->kind == MemoryImpl::MKIND_SYSMEM));
            //       continue;
            //     }
            //     assert(input_ports[i].mem == input_ports[0].mem);
            //   }
            // }
            // else
            // {
            //   src_fpga = 0;
            // }

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

            // if (src_fpga != 0)
            // {
            //   if (dst_fpga != 0)
            //   {
            //     if (src_fpga == dst_fpga)
            //     {
            //       kind = XFER_FPGA_IN_DEV;
            //       // ignore max_req_size value passed in - it's probably too small
            //       max_req_size = 1 << 30;
            //     }
            //     else
            //     {
            //       kind = XFER_FPGA_PEER_DEV;
            //       // ignore max_req_size value passed in - it's probably too small
            //       max_req_size = 256 << 20;
            //     }
            //   }
            //   else
            //   {
            //     kind = XFER_FPGA_FROM_DEV;
            //     if (multihop_copy)
            //       max_req_size = 4 << 20;
            //   }
            // }
            // else
            // {
            //   if (dst_fpga != 0)
            //   {
            //     kind = XFER_FPGA_TO_DEV;
            //     if (multihop_copy)
            //       max_req_size = 4 << 20;
            //   }
            //   else
            //   {
            //     assert(0);
            //   }
            // }

            log_fpga.info() << "create FPGAXferDes " << kind;
            this->kind = kind;
            switch (kind)
            {
            case XFER_FPGA_TO_DEV:
                if (multihop_copy)
                    max_req_size = 4 << 20;
                break;
            case XFER_FPGA_FROM_DEV:
                if (multihop_copy)
                    max_req_size = 4 << 20;
                break;
            case XFER_FPGA_IN_DEV:
                max_req_size = 1 << 30;
                break;
            case XFER_FPGA_PEER_DEV:
                max_req_size = 256 << 20;
                break;
            case XFER_FPGA_TO_DEV_COMP:
                if (multihop_copy)
                    max_req_size = 4 << 20;
                break;
            case XFER_FPGA_FROM_DEV_COMP:
                if (multihop_copy)
                    max_req_size = 4 << 20;
                break;
            default:
                break;
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

        // copied from long XferDes::default_get_requests
        long FPGAXferDes::default_get_requests_tentative(Request **reqs, long nr,
                                                         unsigned flags)
        {
            long idx = 0;

            while ((idx < nr) && request_available())
            {
                // TODO: we really shouldn't even be trying if the iteration
                //   is already done
                if (iteration_completed.load())
                    break;

                // pull control information if we need it
                if (input_control.remaining_count == 0)
                {
                    XferPort &icp = input_ports[input_control.control_port_idx];
                    size_t avail = icp.seq_remote.span_exists(icp.local_bytes_total,
                                                              4 * sizeof(unsigned));
                    size_t old_lbt = icp.local_bytes_total;

                    // may take a few chunks of data to get a control packet
                    bool got_packet = false;
                    do
                    {
                        if (avail < sizeof(unsigned))
                            break; // no data right now

                        TransferIterator::AddressInfo c_info;
                        size_t amt = icp.iter->step(sizeof(unsigned), c_info, 0,
                                                    false /*!tentative*/);
                        assert(amt == sizeof(unsigned));
                        const void *srcptr = icp.mem->get_direct_ptr(c_info.base_offset, amt);
                        assert(srcptr != 0);
                        unsigned cword;
                        memcpy(&cword, srcptr, sizeof(unsigned));

                        icp.local_bytes_total += sizeof(unsigned);
                        avail -= sizeof(unsigned);

                        got_packet = input_control.decoder.decode(cword,
                                                                  input_control.remaining_count,
                                                                  input_control.current_io_port,
                                                                  input_control.eos_received);
                    } while (!got_packet);

                    // can't make further progress if we didn't get a full packet
                    if (!got_packet)
                        break;

                    update_bytes_read(input_control.control_port_idx,
                                      old_lbt, icp.local_bytes_total - old_lbt);

                    log_fpga.info() << "input control: xd=" << std::hex << guid << std::dec
                                    << " port=" << input_control.current_io_port
                                    << " count=" << input_control.remaining_count
                                    << " done=" << input_control.eos_received;
                    // if count is still zero, we're done
                    if (input_control.remaining_count == 0)
                    {
                        assert(input_control.eos_received);
                        iteration_completed.store_release(true);
                        break;
                    }
                }
                if (output_control.remaining_count == 0)
                {
                    // this looks wrong, but the port that controls the output is
                    //  an input port! vvv
                    XferPort &ocp = input_ports[output_control.control_port_idx];
                    size_t avail = ocp.seq_remote.span_exists(ocp.local_bytes_total,
                                                              4 * sizeof(unsigned));
                    size_t old_lbt = ocp.local_bytes_total;

                    // may take a few chunks of data to get a control packet
                    bool got_packet = false;
                    do
                    {
                        if (avail < sizeof(unsigned))
                            break; // no data right now

                        TransferIterator::AddressInfo c_info;
                        size_t amt = ocp.iter->step(sizeof(unsigned), c_info, 0, false /*!tentative*/);
                        assert(amt == sizeof(unsigned));
                        const void *srcptr = ocp.mem->get_direct_ptr(c_info.base_offset, amt);
                        assert(srcptr != 0);
                        unsigned cword;
                        memcpy(&cword, srcptr, sizeof(unsigned));

                        ocp.local_bytes_total += sizeof(unsigned);
                        avail -= sizeof(unsigned);

                        got_packet = output_control.decoder.decode(cword,
                                                                   output_control.remaining_count,
                                                                   output_control.current_io_port,
                                                                   output_control.eos_received);
                    } while (!got_packet);

                    // can't make further progress if we didn't get a full packet
                    if (!got_packet)
                        break;

                    update_bytes_read(output_control.control_port_idx,
                                      old_lbt, ocp.local_bytes_total - old_lbt);

                    log_fpga.info() << "output control: xd=" << std::hex << guid << std::dec
                                    << " port=" << output_control.current_io_port
                                    << " count=" << output_control.remaining_count
                                    << " done=" << output_control.eos_received;
                    // if count is still zero, we're done
                    if (output_control.remaining_count == 0)
                    {
                        assert(output_control.eos_received);
                        iteration_completed.store_release(true);
                        // give all output channels a chance to indicate completion
                        for (size_t i = 0; i < output_ports.size(); i++)
                            update_bytes_write(i, output_ports[i].local_bytes_total, 0);
                        break;
                    }
                }

                XferPort *in_port = ((input_control.current_io_port >= 0) ? &input_ports[input_control.current_io_port] : 0);
                XferPort *out_port = ((output_control.current_io_port >= 0) ? &output_ports[output_control.current_io_port] : 0);
                if (in_port->mem->kind == Realm::MemoryImpl::MKIND_FPGA)
                {
                    printf("from FPGA\n");
                }
                else if (out_port->mem->kind == Realm::MemoryImpl::MKIND_FPGA)
                {
                    printf("to FPGA\n");
                }
                // special cases for OOR scatter/gather
                if (!in_port)
                {
                    if (!out_port)
                    {
                        // no input or output?  just skip the count?
                        assert(0);
                    }
                    else
                    {
                        // no valid input, so no write to the destination -
                        //  just step the output transfer iterator if it's a real target
                        //  but barf if it's an IB
                        assert((out_port->peer_guid == XferDes::XFERDES_NO_GUID) &&
                               !out_port->serdez_op);
                        TransferIterator::AddressInfo dummy;
                        size_t skip_bytes = out_port->iter->step(std::min(input_control.remaining_count,
                                                                          output_control.remaining_count),
                                                                 dummy,
                                                                 flags & TransferIterator::DST_FLAGMASK,
                                                                 false /*!tentative*/);
                        log_fpga.debug() << "skipping " << skip_bytes << " bytes of output";
                        assert(skip_bytes > 0);
                        input_control.remaining_count -= skip_bytes;
                        output_control.remaining_count -= skip_bytes;
                        // TODO: pull this code out to a common place?
                        if (((input_control.remaining_count == 0) && input_control.eos_received) ||
                            ((output_control.remaining_count == 0) && output_control.eos_received))
                        {
                            log_fpga.info() << "iteration completed via control port: xd=" << std::hex << guid << std::dec;
                            iteration_completed.store_release(true);
                            // give all output channels a chance to indicate completion
                            for (size_t i = 0; i < output_ports.size(); i++)
                                update_bytes_write(i, output_ports[i].local_bytes_total, 0);
                            break;
                        }
                        continue; // try again
                    }
                }
                else if (!out_port)
                {
                    // valid input that we need to throw away
                    assert(!in_port->serdez_op);
                    TransferIterator::AddressInfo dummy;
                    // although we're not reading the IB input data ourselves, we need
                    //  to wait until it's ready before not-reading it to avoid WAW
                    //  races on the producer side
                    size_t skip_bytes = std::min(input_control.remaining_count,
                                                 output_control.remaining_count);
                    if (in_port->peer_guid != XferDes::XFERDES_NO_GUID)
                    {
                        skip_bytes = in_port->seq_remote.span_exists(in_port->local_bytes_total,
                                                                     skip_bytes);
                        if (skip_bytes == 0)
                            break;
                    }
                    skip_bytes = in_port->iter->step(skip_bytes,
                                                     dummy,
                                                     flags & TransferIterator::SRC_FLAGMASK,
                                                     false /*!tentative*/);
                    log_fpga.debug() << "skipping " << skip_bytes << " bytes of input";
                    assert(skip_bytes > 0);
                    update_bytes_read(input_control.current_io_port,
                                      in_port->local_bytes_total,
                                      skip_bytes);
                    in_port->local_bytes_total += skip_bytes;
                    input_control.remaining_count -= skip_bytes;
                    output_control.remaining_count -= skip_bytes;
                    // TODO: pull this code out to a common place?
                    if (((input_control.remaining_count == 0) && input_control.eos_received) ||
                        ((output_control.remaining_count == 0) && output_control.eos_received))
                    {
                        log_fpga.info() << "iteration completed via control port: xd=" << std::hex << guid << std::dec;
                        iteration_completed.store_release(true);
                        // give all output channels a chance to indicate completion
                        for (size_t i = 0; i < output_ports.size(); i++)
                            update_bytes_write(i, output_ports[i].local_bytes_total, 0);
                        break;
                    }
                    continue; // try again
                }

                // there are several variables that can change asynchronously to
                //  the logic here:
                //   pre_bytes_total - the max bytes we'll ever see from the input IB
                //   read_bytes_cons - conservative estimate of bytes we've read
                //   write_bytes_cons - conservative estimate of bytes we've written
                //
                // to avoid all sorts of weird race conditions, sample all three here
                //  and only use them in the code below (exception: atomic increments
                //  of rbc or wbc, for which we adjust the snapshot by the same)
                size_t pbt_snapshot = in_port->remote_bytes_total.load_acquire();
                size_t rbc_snapshot = in_port->local_bytes_cons.load_acquire();
                size_t wbc_snapshot = out_port->local_bytes_cons.load_acquire();

                // normally we detect the end of a transfer after initiating a
                //  request, but empty iterators and filtered streams can cause us
                //  to not realize the transfer is done until we are asking for
                //  the next request (i.e. now)
                if ((in_port->peer_guid == XFERDES_NO_GUID) ? in_port->iter->done() : (in_port->local_bytes_total == pbt_snapshot))
                {
                    if (in_port->local_bytes_total == 0)
                        log_fpga.info() << "empty xferdes: " << guid;
                        // TODO: figure out how to eliminate false positives from these
                        //  checks with indirection and/or multiple remote inputs
#if 0
	    assert((out_port->peer_guid != XFERDES_NO_GUID) ||
		   out_port->iter->done());
#endif

                    iteration_completed.store_release(true);

                    // give all output channels a chance to indicate completion
                    for (size_t i = 0; i < output_ports.size(); i++)
                        update_bytes_write(i, output_ports[i].local_bytes_total, 0);
                    break;
                }

                TransferIterator::AddressInfo src_info, dst_info;
                size_t read_bytes, write_bytes, read_seq, write_seq;
                size_t write_pad_bytes = 0;
                size_t read_pad_bytes = 0;

                // handle serialization-only and deserialization-only cases
                //  specially, because they have uncertainty in how much data
                //  they write or read
                if (in_port->serdez_op && !out_port->serdez_op)
                {
                    // serialization only - must be into an IB
                    assert(in_port->peer_guid == XFERDES_NO_GUID);
                    assert(out_port->peer_guid != XFERDES_NO_GUID);

                    // when serializing, we don't know how much output space we're
                    //  going to consume, so do not step the dst_iter here
                    // instead, see what we can get from the source and conservatively
                    //  check flow control on the destination and let the stepping
                    //  of dst_iter happen in the actual execution of the request

                    // if we don't have space to write a single worst-case
                    //  element, try again later
                    if (out_port->seq_remote.span_exists(wbc_snapshot,
                                                         in_port->serdez_op->max_serialized_size) <
                        in_port->serdez_op->max_serialized_size)
                        break;

                    size_t max_bytes = max_req_size;

                    size_t src_bytes = in_port->iter->step(max_bytes, src_info,
                                                           flags & TransferIterator::SRC_FLAGMASK,
                                                           true /*tentative*/);

                    size_t num_elems = src_bytes / in_port->serdez_op->sizeof_field_type;
                    // no input data?  try again later
                    if (num_elems == 0)
                        break;
                    assert((num_elems * in_port->serdez_op->sizeof_field_type) == src_bytes);
                    size_t max_dst_bytes = num_elems * in_port->serdez_op->max_serialized_size;

                    // if we have an output control, restrict the max number of
                    //  elements
                    if (output_control.control_port_idx >= 0)
                    {
                        if (num_elems > output_control.remaining_count)
                        {
                            log_fpga.info() << "scatter/serialize clamp: " << num_elems << " -> " << output_control.remaining_count;
                            num_elems = output_control.remaining_count;
                        }
                    }

                    size_t clamp_dst_bytes = num_elems * in_port->serdez_op->max_serialized_size;
                    // test for space using our conserative bytes written count
                    size_t dst_bytes_avail = out_port->seq_remote.span_exists(wbc_snapshot,
                                                                              clamp_dst_bytes);

                    if (dst_bytes_avail == max_dst_bytes)
                    {
                        // enough space - confirm the source step
                        in_port->iter->confirm_step();
                    }
                    else
                    {
                        // not enough space - figure out how many elements we can
                        //  actually take and adjust the source step
                        size_t act_elems = dst_bytes_avail / in_port->serdez_op->max_serialized_size;
                        // if there was a remainder in the division, get rid of it
                        dst_bytes_avail = act_elems * in_port->serdez_op->max_serialized_size;
                        size_t new_src_bytes = act_elems * in_port->serdez_op->sizeof_field_type;
                        in_port->iter->cancel_step();
                        src_bytes = in_port->iter->step(new_src_bytes, src_info,
                                                        flags & TransferIterator::SRC_FLAGMASK,
                                                        false /*!tentative*/);
                        // this can come up shorter than we expect if the source
                        //  iterator is 2-D or 3-D - if that happens, re-adjust the
                        //  dest bytes again
                        if (src_bytes < new_src_bytes)
                        {
                            if (src_bytes == 0)
                                break;

                            num_elems = src_bytes / in_port->serdez_op->sizeof_field_type;
                            assert((num_elems * in_port->serdez_op->sizeof_field_type) == src_bytes);

                            // no need to recheck seq_next_read
                            dst_bytes_avail = num_elems * in_port->serdez_op->max_serialized_size;
                        }
                    }

                    // since the dst_iter will be stepped later, the dst_info is a
                    //  don't care, so copy the source so that lines/planes/etc match
                    //  up
                    dst_info = src_info;

                    read_seq = in_port->local_bytes_total;
                    read_bytes = src_bytes;
                    in_port->local_bytes_total += src_bytes;

                    write_seq = 0; // filled in later
                    write_bytes = dst_bytes_avail;
                    out_port->local_bytes_cons.fetch_add(dst_bytes_avail);
                    wbc_snapshot += dst_bytes_avail;
                }
                else if (!in_port->serdez_op && out_port->serdez_op)
                {
                    // deserialization only - must be from an IB
                    assert(in_port->peer_guid != XFERDES_NO_GUID);
                    assert(out_port->peer_guid == XFERDES_NO_GUID);

                    // when deserializing, we don't know how much input data we need
                    //  for each element, so do not step the src_iter here
                    //  instead, see what the destination wants
                    // if the transfer is still in progress (i.e. pre_bytes_total
                    //  hasn't been set), we have to be conservative about how many
                    //  elements we can get from partial data

                    // input data is done only if we know the limit AND we have all
                    //  the remaining bytes (if any) up to that limit
                    bool input_data_done = ((pbt_snapshot != size_t(-1)) &&
                                            ((rbc_snapshot >= pbt_snapshot) ||
                                             (in_port->seq_remote.span_exists(rbc_snapshot,
                                                                              pbt_snapshot - rbc_snapshot) ==
                                              (pbt_snapshot - rbc_snapshot))));
                    // if we're using an input control and it's not at the end of the
                    //  stream, the above checks may not be precise
                    if ((input_control.control_port_idx >= 0) &&
                        !input_control.eos_received)
                        input_data_done = false;

                    // this done-ness overrides many checks based on the conservative
                    //  out_port->serdez_op->max_serialized_size
                    if (!input_data_done)
                    {
                        // if we don't have enough input data for a single worst-case
                        //  element, try again later
                        if ((in_port->seq_remote.span_exists(rbc_snapshot,
                                                             out_port->serdez_op->max_serialized_size) <
                             out_port->serdez_op->max_serialized_size))
                        {
                            break;
                        }
                    }

                    size_t max_bytes = max_req_size;

                    size_t dst_bytes = out_port->iter->step(max_bytes, dst_info,
                                                            flags & TransferIterator::DST_FLAGMASK,
                                                            !input_data_done);

                    size_t num_elems = dst_bytes / out_port->serdez_op->sizeof_field_type;
                    if (num_elems == 0)
                        break;
                    assert((num_elems * out_port->serdez_op->sizeof_field_type) == dst_bytes);
                    size_t max_src_bytes = num_elems * out_port->serdez_op->max_serialized_size;
                    // if we have an input control, restrict the max number of
                    //  elements
                    if (input_control.control_port_idx >= 0)
                    {
                        if (num_elems > input_control.remaining_count)
                        {
                            log_fpga.info() << "gather/deserialize clamp: " << num_elems << " -> " << input_control.remaining_count;
                            num_elems = input_control.remaining_count;
                        }
                    }

                    size_t clamp_src_bytes = num_elems * out_port->serdez_op->max_serialized_size;
                    size_t src_bytes_avail;
                    if (input_data_done)
                    {
                        // we're certainty to have all the remaining data, so keep
                        //  the limit at max_src_bytes - we won't actually overshoot
                        //  (unless the serialized data is corrupted)
                        src_bytes_avail = max_src_bytes;
                    }
                    else
                    {
                        // test for space using our conserative bytes read count
                        src_bytes_avail = in_port->seq_remote.span_exists(rbc_snapshot,
                                                                          clamp_src_bytes);

                        if (src_bytes_avail == max_src_bytes)
                        {
                            // enough space - confirm the dest step
                            out_port->iter->confirm_step();
                        }
                        else
                        {
                            log_fpga.info() << "pred limits deserialize: " << max_src_bytes << " -> " << src_bytes_avail;
                            // not enough space - figure out how many elements we can
                            //  actually read and adjust the dest step
                            size_t act_elems = src_bytes_avail / out_port->serdez_op->max_serialized_size;
                            // if there was a remainder in the division, get rid of it
                            src_bytes_avail = act_elems * out_port->serdez_op->max_serialized_size;
                            size_t new_dst_bytes = act_elems * out_port->serdez_op->sizeof_field_type;
                            out_port->iter->cancel_step();
                            dst_bytes = out_port->iter->step(new_dst_bytes, dst_info,
                                                             flags & TransferIterator::SRC_FLAGMASK,
                                                             false /*!tentative*/);
                            // this can come up shorter than we expect if the destination
                            //  iterator is 2-D or 3-D - if that happens, re-adjust the
                            //  source bytes again
                            if (dst_bytes < new_dst_bytes)
                            {
                                if (dst_bytes == 0)
                                    break;

                                num_elems = dst_bytes / out_port->serdez_op->sizeof_field_type;
                                assert((num_elems * out_port->serdez_op->sizeof_field_type) == dst_bytes);

                                // no need to recheck seq_pre_write
                                src_bytes_avail = num_elems * out_port->serdez_op->max_serialized_size;
                            }
                        }
                    }

                    // since the src_iter will be stepped later, the src_info is a
                    //  don't care, so copy the source so that lines/planes/etc match
                    //  up
                    src_info = dst_info;

                    read_seq = 0; // filled in later
                    read_bytes = src_bytes_avail;
                    in_port->local_bytes_cons.fetch_add(src_bytes_avail);
                    rbc_snapshot += src_bytes_avail;

                    write_seq = out_port->local_bytes_total;
                    write_bytes = dst_bytes;
                    out_port->local_bytes_total += dst_bytes;
                    out_port->local_bytes_cons.store(out_port->local_bytes_total); // completion detection uses this
                }
                else
                {
                    // either no serialization or simultaneous serdez

                    // limit transfer based on the max request size, or the largest
                    //  amount of data allowed by the control port(s)
                    size_t max_bytes = std::min(size_t(max_req_size),
                                                std::min(input_control.remaining_count,
                                                         output_control.remaining_count));

                    // if we're not the first in the chain, and we know the total bytes
                    //  written by the predecessor, don't exceed that
                    if (in_port->peer_guid != XFERDES_NO_GUID)
                    {
                        size_t pre_max = pbt_snapshot - in_port->local_bytes_total;
                        if (pre_max == 0)
                        {
                            // should not happen with snapshots
                            assert(0);
                            // due to unsynchronized updates to pre_bytes_total, this path
                            //  can happen for an empty transfer reading from an intermediate
                            //  buffer - handle it by looping around and letting the check
                            //  at the top of the loop notice it the second time around
                            if (in_port->local_bytes_total == 0)
                                continue;
                            // otherwise, this shouldn't happen - we should detect this case
                            //  on the the transfer of those last bytes
                            assert(0);
                            iteration_completed.store_release(true);
                            break;
                        }
                        log_fpga.info() << "!!!!!!!pred limits xfer: " << max_bytes << " -> " << pre_max;
                        if (pre_max < max_bytes)
                        {
                            max_bytes = pre_max;
                        }

                        // further limit by bytes we've actually received
                        log_fpga.info() << "!!!!!!!before max_bytes: " << max_bytes << " in_port->local_bytes_total: " << in_port->local_bytes_total;
                        max_bytes = in_port->seq_remote.span_exists(in_port->local_bytes_total, max_bytes);
                        log_fpga.info() << "!!!!!!!after max_bytes: " << max_bytes << " in_port->local_bytes_total: " << in_port->local_bytes_total;
                        if (max_bytes == 0)
                        {
                            // TODO: put this XD to sleep until we do have data
                            break;
                        }
                    }

                    if (out_port->peer_guid != XFERDES_NO_GUID)
                    {
                        // if we're writing to an intermediate buffer, make sure to not
                        //  overwrite previously written data that has not been read yet
                        max_bytes = out_port->seq_remote.span_exists(out_port->local_bytes_total, max_bytes);
                        if (max_bytes == 0)
                        {
                            // TODO: put this XD to sleep until we do have data
                            break;
                        }
                    }

                    // tentatively get as much as we can from the source iterator
                    size_t src_bytes = in_port->iter->step(max_bytes, src_info,
                                                           flags & TransferIterator::SRC_FLAGMASK,
                                                           true /*tentative*/);
                    if (src_bytes == 0)
                    {
                        // not enough space for even one element
                        // TODO: put this XD to sleep until we do have data
                        break;
                    }

                    // destination step must be tentative for an non-IB source or
                    //  target that might collapse dimensions differently
                    bool dimension_mismatch_possible = (((in_port->peer_guid == XFERDES_NO_GUID) ||
                                                         (out_port->peer_guid == XFERDES_NO_GUID)) &&
                                                        ((flags & TransferIterator::LINES_OK) != 0));

                    size_t dst_bytes = out_port->iter->step(src_bytes, dst_info,
                                                            flags & TransferIterator::DST_FLAGMASK,
                                                            true);
                    if (dst_bytes == 0)
                    {
                        // not enough space for even one element

                        // if this happens when the input is an IB, the output is not,
                        //  and the input doesn't seem to be limited by max_bytes, this
                        //  is (probably?) the case that requires padding on the input
                        //  side
                        if ((in_port->peer_guid != XFERDES_NO_GUID) &&
                            (out_port->peer_guid == XFERDES_NO_GUID) &&
                            (src_bytes < max_bytes))
                        {
                            log_fpga.info() << "padding input buffer by " << src_bytes << " bytes";
                            src_info.bytes_per_chunk = 0;
                            src_info.num_lines = 1;
                            src_info.num_planes = 1;
                            dst_info.bytes_per_chunk = 0;
                            dst_info.num_lines = 1;
                            dst_info.num_planes = 1;
                            read_pad_bytes = src_bytes;
                            src_bytes = 0;
                            dimension_mismatch_possible = false;
                            // src iterator will be confirmed below
                            // in_port->iter->confirm_step();
                            // dst didn't actually take a step, so we don't need to cancel it
                        }
                        else
                        {
                            in_port->iter->cancel_step();
                            // TODO: put this XD to sleep until we do have data
                            break;
                        }
                    }

                    // does source now need to be shrunk?
                    if (dst_bytes < src_bytes)
                    {
                        // cancel the src step and try to just step by dst_bytes
                        in_port->iter->cancel_step();
                        // this step must still be tentative if a dimension mismatch is
                        //  posisble
                        src_bytes = in_port->iter->step(dst_bytes, src_info,
                                                        flags & TransferIterator::SRC_FLAGMASK,
                                                        dimension_mismatch_possible);
                        if (src_bytes == 0)
                        {
                            // corner case that should occur only with a destination
                            //  intermediate buffer - no transfer, but pad to boundary
                            //  destination wants as long as we're not being limited by
                            //  max_bytes
                            assert((in_port->peer_guid == XFERDES_NO_GUID) &&
                                   (out_port->peer_guid != XFERDES_NO_GUID));
                            if (dst_bytes < max_bytes)
                            {
                                log_fpga.info() << "padding output buffer by " << dst_bytes << " bytes";
                                src_info.bytes_per_chunk = 0;
                                src_info.num_lines = 1;
                                src_info.num_planes = 1;
                                dst_info.bytes_per_chunk = 0;
                                dst_info.num_lines = 1;
                                dst_info.num_planes = 1;
                                write_pad_bytes = dst_bytes;
                                dst_bytes = 0;
                                dimension_mismatch_possible = false;
                                // src didn't actually take a step, so we don't need to cancel it
                                // out_port->iter->confirm_step();
                            }
                            else
                            {
                                // retry later
                                // src didn't actually take a step, so we don't need to cancel it
                                out_port->iter->cancel_step();
                                break;
                            }
                        }
                        // a mismatch is still possible if the source is 2+D and the
                        //  destination wants to stop mid-span
                        if (src_bytes < dst_bytes)
                        {
                            assert(dimension_mismatch_possible);
                            out_port->iter->cancel_step();
                            dst_bytes = out_port->iter->step(src_bytes, dst_info,
                                                             flags & TransferIterator::DST_FLAGMASK,
                                                             true /*tentative*/);
                        }
                        // byte counts now must match
                        assert(src_bytes == dst_bytes);
                    }
                    else
                    {
                        // in the absense of dimension mismatches, it's safe now to confirm
                        //  the source step
                        if (!dimension_mismatch_possible)
                        {
                            // in_port->iter->confirm_step();
                            ;
                        }
                    }

                    // when 2D transfers are allowed, it is possible that the
                    // bytes_per_chunk don't match, and we need to add an extra
                    //  dimension to one side or the other
                    // NOTE: this transformation can cause the dimensionality of the
                    //  transfer to grow.  Allow this to happen and detect it at the
                    //  end.
                    if (!dimension_mismatch_possible)
                    {
                        assert(src_info.bytes_per_chunk == dst_info.bytes_per_chunk);
                        assert(src_info.num_lines == 1);
                        assert(src_info.num_planes == 1);
                        assert(dst_info.num_lines == 1);
                        assert(dst_info.num_planes == 1);
                    }
                    else
                    {
                        // track how much of src and/or dst is "lost" into a 4th
                        //  dimension
                        size_t src_4d_factor = 1;
                        size_t dst_4d_factor = 1;
                        if (src_info.bytes_per_chunk < dst_info.bytes_per_chunk)
                        {
                            size_t ratio = dst_info.bytes_per_chunk / src_info.bytes_per_chunk;
                            assert((src_info.bytes_per_chunk * ratio) == dst_info.bytes_per_chunk);
                            dst_4d_factor *= dst_info.num_planes; // existing planes lost
                            dst_info.num_planes = dst_info.num_lines;
                            dst_info.plane_stride = dst_info.line_stride;
                            dst_info.num_lines = ratio;
                            dst_info.line_stride = src_info.bytes_per_chunk;
                            dst_info.bytes_per_chunk = src_info.bytes_per_chunk;
                        }
                        if (dst_info.bytes_per_chunk < src_info.bytes_per_chunk)
                        {
                            size_t ratio = src_info.bytes_per_chunk / dst_info.bytes_per_chunk;
                            assert((dst_info.bytes_per_chunk * ratio) == src_info.bytes_per_chunk);
                            src_4d_factor *= src_info.num_planes; // existing planes lost
                            src_info.num_planes = src_info.num_lines;
                            src_info.plane_stride = src_info.line_stride;
                            src_info.num_lines = ratio;
                            src_info.line_stride = dst_info.bytes_per_chunk;
                            src_info.bytes_per_chunk = dst_info.bytes_per_chunk;
                        }

                        // similarly, if the number of lines doesn't match, we need to promote
                        //  one of the requests from 2D to 3D
                        if (src_info.num_lines < dst_info.num_lines)
                        {
                            size_t ratio = dst_info.num_lines / src_info.num_lines;
                            assert((src_info.num_lines * ratio) == dst_info.num_lines);
                            dst_4d_factor *= dst_info.num_planes; // existing planes lost
                            dst_info.num_planes = ratio;
                            dst_info.plane_stride = dst_info.line_stride * src_info.num_lines;
                            dst_info.num_lines = src_info.num_lines;
                        }
                        if (dst_info.num_lines < src_info.num_lines)
                        {
                            size_t ratio = src_info.num_lines / dst_info.num_lines;
                            assert((dst_info.num_lines * ratio) == src_info.num_lines);
                            src_4d_factor *= src_info.num_planes; // existing planes lost
                            src_info.num_planes = ratio;
                            src_info.plane_stride = src_info.line_stride * dst_info.num_lines;
                            src_info.num_lines = dst_info.num_lines;
                        }

                        // sanity-checks: src/dst should match on lines/planes and we
                        //  shouldn't have multiple planes if we don't have multiple lines
                        assert(src_info.num_lines == dst_info.num_lines);
                        assert((src_info.num_planes * src_4d_factor) ==
                               (dst_info.num_planes * dst_4d_factor));
                        assert((src_info.num_lines > 1) || (src_info.num_planes == 1));
                        assert((dst_info.num_lines > 1) || (dst_info.num_planes == 1));

                        // only do as many planes as both src and dst can manage
                        if (src_info.num_planes > dst_info.num_planes)
                            src_info.num_planes = dst_info.num_planes;
                        else
                            dst_info.num_planes = src_info.num_planes;

                        // if 3D isn't allowed, set num_planes back to 1
                        if ((flags & TransferIterator::PLANES_OK) == 0)
                        {
                            src_info.num_planes = 1;
                            dst_info.num_planes = 1;
                        }

                        // now figure out how many bytes we're actually able to move and
                        //  if it's less than what we got from the iterators, try again
                        size_t act_bytes = (src_info.bytes_per_chunk *
                                            src_info.num_lines *
                                            src_info.num_planes);
                        if (act_bytes == src_bytes)
                        {
                            // things match up - confirm the steps
                            // in_port->iter->confirm_step();
                            // out_port->iter->confirm_step();
                        }
                        else
                        {
                            // log_fpga.info() << "dimension mismatch! " << act_bytes << " < " << src_bytes << " (" << bytes_total << ")";
                            TransferIterator::AddressInfo dummy_info;
                            in_port->iter->cancel_step();
                            src_bytes = in_port->iter->step(act_bytes, dummy_info,
                                                            flags & TransferIterator::SRC_FLAGMASK,
                                                            true /*!tentative*/);
                            assert(src_bytes == act_bytes);
                            out_port->iter->cancel_step();
                            dst_bytes = out_port->iter->step(act_bytes, dummy_info,
                                                             flags & TransferIterator::DST_FLAGMASK,
                                                             true /*!tentative*/);
                            assert(dst_bytes == act_bytes);
                        }
                    }

                    size_t act_bytes = (src_info.bytes_per_chunk *
                                        src_info.num_lines *
                                        src_info.num_planes);
                    read_seq = in_port->local_bytes_total;
                    read_bytes = act_bytes + read_pad_bytes;

                    // update bytes read unless we're using indirection
                    if (in_port->indirect_port_idx < 0)
                        in_port->local_bytes_total += read_bytes;

                    write_seq = out_port->local_bytes_total;
                    write_bytes = act_bytes + write_pad_bytes;
                    out_port->local_bytes_total += write_bytes;
                    out_port->local_bytes_cons.store(out_port->local_bytes_total); // completion detection uses this
                }

                Request *new_req = dequeue_request();
                new_req->src_port_idx = input_control.current_io_port;
                new_req->dst_port_idx = output_control.current_io_port;
                new_req->read_seq_pos = read_seq;
                new_req->read_seq_count = read_bytes;
                new_req->write_seq_pos = write_seq;
                new_req->write_seq_count = write_bytes;
                new_req->dim = ((src_info.num_planes == 1) ? ((src_info.num_lines == 1) ? Request::DIM_1D : Request::DIM_2D) : Request::DIM_3D);
                new_req->src_off = src_info.base_offset;
                new_req->dst_off = dst_info.base_offset;
                new_req->nbytes = src_info.bytes_per_chunk;
                new_req->nlines = src_info.num_lines;
                new_req->src_str = src_info.line_stride;
                new_req->dst_str = dst_info.line_stride;
                new_req->nplanes = src_info.num_planes;
                new_req->src_pstr = src_info.plane_stride;
                new_req->dst_pstr = dst_info.plane_stride;

                // we can actually hit the end of an intermediate buffer input
                //  even if our initial pbt_snapshot was (size_t)-1 because
                //  we use the asynchronously-updated seq_pre_write, so if
                //  we think we might be done, go ahead and resample here if
                //  we still have -1
                if ((in_port->peer_guid != XFERDES_NO_GUID) &&
                    (pbt_snapshot == (size_t)-1))
                    pbt_snapshot = in_port->remote_bytes_total.load_acquire();

                // if we have control ports, they tell us when we're done
                if ((input_control.control_port_idx >= 0) ||
                    (output_control.control_port_idx >= 0))
                {
                    // update control port counts, which may also flag a completed iteration
                    size_t input_count = read_bytes - read_pad_bytes;
                    size_t output_count = write_bytes - write_pad_bytes;
                    // if we're serializing or deserializing, we count in elements,
                    //  not bytes
                    if (in_port->serdez_op != 0)
                    {
                        // serializing impacts output size
                        assert((output_count % in_port->serdez_op->max_serialized_size) == 0);
                        output_count /= in_port->serdez_op->max_serialized_size;
                    }
                    if (out_port->serdez_op != 0)
                    {
                        // and deserializing impacts input size
                        assert((input_count % out_port->serdez_op->max_serialized_size) == 0);
                        input_count /= out_port->serdez_op->max_serialized_size;
                    }
                    assert(input_control.remaining_count >= input_count);
                    assert(output_control.remaining_count >= output_count);
                    input_control.remaining_count -= input_count;
                    output_control.remaining_count -= output_count;
                    if (((input_control.remaining_count == 0) && input_control.eos_received) ||
                        ((output_control.remaining_count == 0) && output_control.eos_received))
                    {
                        log_fpga.info() << "iteration completed via control port: xd=" << std::hex << guid << std::dec;
                        iteration_completed.store_release(true);

                        // give all output channels a chance to indicate completion
                        for (size_t i = 0; i < output_ports.size(); i++)
                            if (int(i) != output_control.current_io_port)
                                update_bytes_write(i, output_ports[i].local_bytes_total, 0);
#if 0
	      // non-ib iterators should end at the same time?
	      for(size_t i = 0; i < input_ports.size(); i++)
		assert((input_ports[i].peer_guid != XFERDES_NO_GUID) ||
		       input_ports[i].iter->done());
	      for(size_t i = 0; i < output_ports.size(); i++)
		assert((output_ports[i].peer_guid != XFERDES_NO_GUID) ||
		       output_ports[i].iter->done());
#endif
                    }
                }
                else
                {
                    // otherwise, we go by our iterators
                    if (in_port->iter->done() || out_port->iter->done() ||
                        (in_port->local_bytes_total == pbt_snapshot))
                    {
                        assert(!iteration_completed.load());
                        iteration_completed.store_release(true);

                        // give all output channels a chance to indicate completion
                        for (size_t i = 0; i < output_ports.size(); i++)
                            if (int(i) != output_control.current_io_port)
                                update_bytes_write(i, output_ports[i].local_bytes_total, 0);

                                // TODO: figure out how to eliminate false positives from these
                                //  checks with indirection and/or multiple remote inputs
#if 0
	      // non-ib iterators should end at the same time
	      assert((in_port->peer_guid != XFERDES_NO_GUID) || in_port->iter->done());
	      assert((out_port->peer_guid != XFERDES_NO_GUID) || out_port->iter->done());
#endif

                        if (!in_port->serdez_op && out_port->serdez_op)
                        {
                            // ok to be over, due to the conservative nature of
                            //  deserialization reads
                            assert((rbc_snapshot >= pbt_snapshot) ||
                                   (pbt_snapshot == size_t(-1)));
                        }
                        else
                        {
                            // TODO: this check is now too aggressive because the previous
                            //  xd doesn't necessarily know when it's emitting its last
                            //  data, which means the update of local_bytes_total might
                            //  be delayed
#if 0
		assert((in_port->peer_guid == XFERDES_NO_GUID) ||
		       (pbt_snapshot == in_port->local_bytes_total));
#endif
                        }
                    }
                }

                switch (new_req->dim)
                {
                case Request::DIM_1D:
                {
                    log_fpga.info() << "request: guid=" << std::hex << guid << std::dec
                                    << " ofs=" << new_req->src_off << "->" << new_req->dst_off
                                    << " len=" << new_req->nbytes;
                    break;
                }
                case Request::DIM_2D:
                {
                    log_fpga.info() << "request: guid=" << std::hex << guid << std::dec
                                    << " ofs=" << new_req->src_off << "->" << new_req->dst_off
                                    << " len=" << new_req->nbytes
                                    << " lines=" << new_req->nlines << "(" << new_req->src_str << "," << new_req->dst_str << ")";
                    break;
                }
                case Request::DIM_3D:
                {
                    log_fpga.info() << "request: guid=" << std::hex << guid << std::dec
                                    << " ofs=" << new_req->src_off << "->" << new_req->dst_off
                                    << " len=" << new_req->nbytes
                                    << " lines=" << new_req->nlines << "(" << new_req->src_str << "," << new_req->dst_str << ")"
                                    << " planes=" << new_req->nplanes << "(" << new_req->src_pstr << "," << new_req->dst_pstr << ")";
                    break;
                }
                }
                reqs[idx++] = new_req;
            }
            return idx;
        }

        long FPGAXferDes::get_requests(Request **requests, long nr)
        {
            FPGARequest **reqs = (FPGARequest **)requests;
            // no do allow 2D and 3D copies
            // unsigned flags = (TransferIterator::LINES_OK |
            //                   TransferIterator::PLANES_OK);
            unsigned flags = 0;
            long new_nr = default_get_requests_tentative(requests, nr, flags);
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
                case XFER_FPGA_TO_DEV_COMP:
                {
                    reqs[i]->src_base = input_ports[reqs[i]->src_port_idx].mem->get_direct_ptr(reqs[i]->src_off, reqs[i]->nbytes);
                    assert(reqs[i]->src_base != 0);
                    break;
                }
                case XFER_FPGA_FROM_DEV_COMP:
                {
                    reqs[i]->dst_base = output_ports[reqs[i]->dst_port_idx].mem->get_direct_ptr(reqs[i]->dst_off, reqs[i]->nbytes);
                    assert(reqs[i]->dst_base != 0);
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
            Memory temp_rdma_mem = src_fpga->local_ibmem->me;

            switch (_kind)
            {
            case XFER_FPGA_TO_DEV:
            {
                unsigned bw = 0; // TODO
                unsigned latency = 0;
                add_path(temp_sys_mem, temp_fpga_mem, bw, latency, false, false, XFER_FPGA_TO_DEV);
                add_path(temp_rdma_mem, temp_fpga_mem, bw, latency, false, false, XFER_FPGA_TO_DEV);
                break;
            }

            case XFER_FPGA_FROM_DEV:
            {
                unsigned bw = 0; // TODO
                unsigned latency = 0;
                add_path(temp_fpga_mem, temp_sys_mem, bw, latency, false, false, XFER_FPGA_FROM_DEV);
                add_path(temp_fpga_mem, temp_rdma_mem, bw, latency, false, false, XFER_FPGA_FROM_DEV);
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

            case XFER_FPGA_TO_DEV_COMP:
            {
                unsigned bw = 0; // TODO
                unsigned latency = 0;
                add_path(temp_sys_mem, temp_fpga_mem, bw, latency, false, false, XFER_FPGA_TO_DEV_COMP);
                add_path(temp_rdma_mem, temp_fpga_mem, bw, latency, false, false, XFER_FPGA_TO_DEV_COMP);
                break;
            }

            case XFER_FPGA_FROM_DEV_COMP:
            {
                unsigned bw = 0; // TODO
                unsigned latency = 0;
                add_path(temp_fpga_mem, temp_sys_mem, bw, latency, false, false, XFER_FPGA_FROM_DEV_COMP);
                add_path(temp_fpga_mem, temp_rdma_mem, bw, latency, false, false, XFER_FPGA_FROM_DEV_COMP);
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
                                   priority, kind);
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
                    case XFER_FPGA_TO_DEV_COMP:
                        src_fpga->copy_to_fpga_comp(req->dst_off, req->src_base,
                                                    req->nbytes, &req->event);
                        break;
                    case XFER_FPGA_FROM_DEV_COMP:
                        src_fpga->copy_from_fpga_comp(req->dst_base, req->src_off,
                                                      req->nbytes, &req->event);
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
                    case XFER_FPGA_TO_DEV_COMP:
                        assert(0 && "not implemented");
                        break;
                    case XFER_FPGA_FROM_DEV_COMP:
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
                    case XFER_FPGA_TO_DEV_COMP:
                        assert(0 && "not implemented");
                        break;
                    case XFER_FPGA_FROM_DEV_COMP:
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

                // pending_copies.push_back(req);
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
                    // xclSyncBO(channel->fpga->dev_handle, channel->fpga->bo_handle, XCL_BO_SYNC_BO_TO_DEVICE, total_bytes, initial_out_offset);
                    for (size_t i = 0; i < 50; i++)
                    {
                        printf("%d ", ((int *)(channel->fpga->fpga_mem->base_ptr_sys))[i]);
                    }
                    printf("\n");

                    log_fpga.info() << "before write buffer, initial_out_offset " << initial_out_offset << " total_bytes " << total_bytes;
                    // cl_buffer_region buff_info = {initial_out_offset, total_bytes};
                    // cl::Buffer temp_buff;
                    // OCL_CHECK(err, temp_buff = channel->fpga->buff.createSubBuffer(CL_MEM_HOST_WRITE_ONLY, CL_BUFFER_CREATE_TYPE_REGION, &buff_info, &err));
                    // OCL_CHECK(err, err = channel->fpga->command_queue.enqueueMigrateMemObjects({temp_buff}, 0 /* 0 means from host*/));
                    // OCL_CHECK(err, err = channel->fpga->command_queue.finish());
                    cl_int err = 0;
                    OCL_CHECK(err, err = channel->fpga->command_queue.enqueueWriteBuffer(channel->fpga->buff,                                                              // buffer on the FPGA
                                                                                         CL_TRUE,                                                                          // blocking call
                                                                                         initial_out_offset,                                                               // buffer offset in bytes
                                                                                         total_bytes,                                                                      // Size in bytes
                                                                                         (void *)((uint64_t)(channel->fpga->fpga_mem->base_ptr_sys) + initial_out_offset), // Pointer to the data to copy
                                                                                         nullptr, nullptr));
                    log_fpga.info() << "after write buffer";
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
};     // namespace Realm
