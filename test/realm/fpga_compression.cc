#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "realm.h"
#include "realm/fpga/fpga_utils.h"

// XRT includes
#include "ert.h"
#include "xrt.h"
#include "xrt_mem.h"

using namespace Realm;

#define ARG_BANK 1
#define CMD_SIZE 4096

#define ARG_DEFLATE_INPUT_OFFSET 0x10
#define ARG_DEFLATE_OUTPUT_OFFSET 0x1c
#define ARG_DEFLATE_SIZE_OFFSET 0x28
#define ARG_INFLATE_INPUT_OFFSET 0x10
#define ARG_INFLATE_OUTPUT_OFFSET 0x1c

#define DEFLATE_XCLBIN "/home/xi/Programs/lz77/deflate.xclbin"
#define INFLATE_XCLBIN "/home/xi/Programs/lz77/inflate.xclbin"
#define MAX_SIZE 3000  // in bytes

#define DEFAULT_COMP_UNITS 4
enum
{
  FID_DEFLATE_IN = 101,
  FID_DEFLATE_OUT = 102,
  FID_INFLATE_IN = 103,
  FID_INFLATE_OUT = 104,
};

// execute a task on FPGA Processor
Logger log_app("app");

enum
{
  TOP_LEVEL_TASK = Processor::TASK_ID_FIRST_AVAILABLE + 0,
  DEFLATE_TASK,
  INFLATE_TASK,
};

struct DEFLATE_Args
{
  RegionInstance deflate_in_inst, deflate_out_inst;
  Rect<1> bounds;
  size_t size;
};

struct INFLATE_Args
{
  RegionInstance inflate_in_inst, inflate_out_inst;
  Rect<1> bounds;
};

void deflate_task(const void *args, size_t arglen,
                  const void *userdata, size_t userlen, Processor p)
{
  size_t i;

  const DEFLATE_Args &local_args = *(const DEFLATE_Args *)args;

  // get affine accessors for each of our three instances
  AffineAccessor<char, 1> ra_deflate_in = AffineAccessor<char, 1>(local_args.deflate_in_inst,
                                                       FID_DEFLATE_IN);
  AffineAccessor<char, 1> ra_deflate_out = AffineAccessor<char, 1>(local_args.deflate_out_inst,
                                                       FID_DEFLATE_OUT);

  //load xclbin
  int fd = open(DEFLATE_XCLBIN, O_RDONLY);
  struct stat st;
  fstat(fd, &st);
  size_t xclbin_size = st.st_size;
  struct axlf *xclbin = (struct axlf *)mmap(NULL, xclbin_size, PROT_READ, MAP_PRIVATE, fd, 0);
  uuid_t xclbin_uuid;
  memcpy(xclbin_uuid, xclbin->m_header.uuid, sizeof(uuid_t));
  xclDeviceHandle dev_handle = FPGAGetCurrentDevice();
  xclLoadXclBin(dev_handle, (const struct axlf *)xclbin);
  munmap(xclbin, xclbin_size);
  close(fd);

  size_t num_compute_units = DEFAULT_COMP_UNITS;
  for (i = 0; i < num_compute_units; i++)
  {
    xclOpenContext(dev_handle, xclbin_uuid, (unsigned int)i, true);
  }

  uint64_t p_base_dev = (uint64_t)FPGAGetBasePtrDev();
  uint64_t p_base_sys = (uint64_t)FPGAGetBasePtrSys();
  uint64_t p_deflate_in = p_base_dev + (uint64_t)ra_deflate_in.ptr(0) - p_base_sys;
  uint64_t p_deflate_out = p_base_dev + (uint64_t)ra_deflate_out.ptr(0) - p_base_sys;
  log_app.print("ra_deflate_in = %p ra_deflate_out = %p\n", ra_deflate_in.ptr(0), ra_deflate_out.ptr(0));
  log_app.print("p_deflate_in = %lu p_deflate_out = %lu\n", p_deflate_in, p_deflate_out);

  xclBufferHandle bo_cmd = xclAllocBO(dev_handle, (size_t)CMD_SIZE, 0, XCL_BO_FLAGS_EXECBUF);
  struct ert_start_kernel_cmd *start_cmd = (struct ert_start_kernel_cmd *)xclMapBO(dev_handle, bo_cmd, true);
	memset(start_cmd, 0, CMD_SIZE);
	start_cmd->state = ERT_CMD_STATE_NEW;
	start_cmd->opcode = ERT_START_CU;
	start_cmd->stat_enabled = 1;
	start_cmd->count = ARG_DEFLATE_SIZE_OFFSET/4 + 3;
	start_cmd->cu_mask = (1 << num_compute_units )-1;  /* CU 0, 1, 2 */
  start_cmd->data[ARG_DEFLATE_INPUT_OFFSET/4] = p_deflate_in; 
	start_cmd->data[ARG_DEFLATE_INPUT_OFFSET/4 + 1] = (p_deflate_in >> 32) & 0xFFFFFFFF;
	start_cmd->data[ARG_DEFLATE_OUTPUT_OFFSET/4] = p_deflate_out; 
	start_cmd->data[ARG_DEFLATE_OUTPUT_OFFSET/4 + 1] = (p_deflate_out >> 32) & 0xFFFFFFFF;
	start_cmd->data[ARG_DEFLATE_SIZE_OFFSET/4] = local_args.size;
	start_cmd->data[ARG_DEFLATE_SIZE_OFFSET/4 + 1] = (local_args.size >> 32) & 0xFFFFFFFF;

  xclExecBuf(dev_handle, bo_cmd);
  struct ert_packet *cmd_packet = (struct ert_packet *)start_cmd;
  while (cmd_packet->state != ERT_CMD_STATE_COMPLETED)
  {
    xclExecWait(dev_handle, 1000);
  }

  xclUnmapBO(dev_handle, bo_cmd, start_cmd);
  xclFreeBO(dev_handle, bo_cmd);
  for (i = 0; i < num_compute_units; i++)
  {
    xclCloseContext(dev_handle, xclbin_uuid, (unsigned int)i);
  }
}

void inflate_task(const void *args, size_t arglen,
                  const void *userdata, size_t userlen, Processor p)
{
  size_t i;

  const INFLATE_Args &local_args = *(const INFLATE_Args *)args;

  // get affine accessors for each of our three instances
  AffineAccessor<char, 1> ra_inflate_in = AffineAccessor<char, 1>(local_args.inflate_in_inst,
                                                       FID_INFLATE_IN);
  AffineAccessor<char, 1> ra_inflate_out = AffineAccessor<char, 1>(local_args.inflate_out_inst,
                                                       FID_INFLATE_OUT);
  
  //load xclbin
  int fd = open(INFLATE_XCLBIN, O_RDONLY);
  struct stat st;
  fstat(fd, &st);
  size_t xclbin_size = st.st_size;
  struct axlf *xclbin = (struct axlf *)mmap(NULL, xclbin_size, PROT_READ, MAP_PRIVATE, fd, 0);
  uuid_t xclbin_uuid;
  memcpy(xclbin_uuid, xclbin->m_header.uuid, sizeof(uuid_t));
  xclDeviceHandle dev_handle = FPGAGetCurrentDevice();
  xclLoadXclBin(dev_handle, (const struct axlf *)xclbin);
  munmap(xclbin, xclbin_size);
  close(fd);

  size_t num_compute_units = DEFAULT_COMP_UNITS;
  for (i = 0; i < num_compute_units; i++)
  {
    xclOpenContext(dev_handle, xclbin_uuid, (unsigned int)i, true);
  }

  uint64_t p_base_dev = (uint64_t)FPGAGetBasePtrDev();
  uint64_t p_base_sys = (uint64_t)FPGAGetBasePtrSys();
  uint64_t p_inflate_in = p_base_dev + (uint64_t)ra_inflate_in.ptr(0) - p_base_sys;
  uint64_t p_inflate_out = p_base_dev + (uint64_t)ra_inflate_out.ptr(0) - p_base_sys;
  log_app.print("ra_inflate_in = %p ra_inflate_out = %p\n", ra_inflate_in.ptr(0), ra_inflate_out.ptr(0));
  log_app.print("p_inflate_in = %lu p_inflate_out = %lu\n", p_inflate_in, p_inflate_out);

  xclBufferHandle bo_cmd = xclAllocBO(dev_handle, (size_t)CMD_SIZE, 0, XCL_BO_FLAGS_EXECBUF);
  struct ert_start_kernel_cmd *start_cmd = (struct ert_start_kernel_cmd *)xclMapBO(dev_handle, bo_cmd, true);
	memset(start_cmd, 0, CMD_SIZE);
	start_cmd->state = ERT_CMD_STATE_NEW;
	start_cmd->opcode = ERT_START_CU;
	start_cmd->stat_enabled = 1;
	start_cmd->count = ARG_INFLATE_OUTPUT_OFFSET/4 + 3;
	start_cmd->cu_mask = (1 << num_compute_units )-1;  /* CU 0, 1, 2 */
  start_cmd->data[ARG_INFLATE_INPUT_OFFSET/4] = p_inflate_in; 
	start_cmd->data[ARG_INFLATE_INPUT_OFFSET/4 + 1] = (p_inflate_in >> 32) & 0xFFFFFFFF;
	start_cmd->data[ARG_INFLATE_OUTPUT_OFFSET/4] = p_inflate_out; 
	start_cmd->data[ARG_INFLATE_OUTPUT_OFFSET/4 + 1] = (p_inflate_out >> 32) & 0xFFFFFFFF;

  xclExecBuf(dev_handle, bo_cmd);
  struct ert_packet *cmd_packet = (struct ert_packet *)start_cmd;
  while (cmd_packet->state != ERT_CMD_STATE_COMPLETED)
  {
    xclExecWait(dev_handle, 1000);
  }

  xclUnmapBO(dev_handle, bo_cmd, start_cmd);
  xclFreeBO(dev_handle, bo_cmd);
  for (i = 0; i < num_compute_units; i++)
  {
    xclCloseContext(dev_handle, xclbin_uuid, (unsigned int)i);
  }
}
void top_level_task(const void *args, size_t arglen,
                    const void *userdata, size_t userlen, Processor p)
{
  const char *text = (const char *)args;
  size_t size = arglen;
  log_app.print() << "top task running on " << p;
  Machine machine = Machine::get_machine();
  std::set<Processor> all_processors;
  machine.get_all_processors(all_processors);
  std::set<Processor> local_processors;
  machine.get_local_processors(local_processors);
  std::vector<Processor> remote_processors;
  std::vector<Memory> remote_fpga_mems;
  std::vector<Memory> remote_sys_mems;
  for (std::set<Processor>::const_iterator it = all_processors.begin();
       it != all_processors.end();
       it++)
  {
    Processor pp = (*it);
    log_app.print() << "[all] processor " << pp << " kind " << pp.kind() << " address_space " << pp.address_space();
    if (pp.kind() == Processor::FPGA_PROC and local_processors.find(pp) == local_processors.end())
    {
      remote_processors.push_back(pp);
    }
  }
  for (std::vector<Processor>::const_iterator it = remote_processors.begin();
      it != remote_processors.end();
      it++)
  {
    Processor pp = (*it);
    log_app.print() << "[remote] processor " << pp << " kind " << pp.kind() << " address_space " << pp.address_space();
    std::set<Memory> visible_mems;
    machine.get_visible_memories(pp, visible_mems);
    for (std::set<Memory>::const_iterator it = visible_mems.begin();
          it != visible_mems.end(); it++)
    {
      if (it->kind() == Memory::FPGA_MEM)
      {
        remote_fpga_mems.push_back(*it);
        log_app.print() << "[remote] fpga memory: " << *it << " capacity="
                        << (it->capacity() >> 20) << " MB";
      }
      if (it->kind() == Memory::SYSTEM_MEM)
      {
        remote_sys_mems.push_back(*it);
        log_app.print() << "[remote] sys memory: " << *it << " capacity="
                        << (it->capacity() >> 20) << " MB";
      }
    }

  }
  for (std::set<Processor>::const_iterator it = local_processors.begin();
       it != local_processors.end();
       it++)
  {
    Processor pp = (*it);
    log_app.print() << "[local] processor " << pp << " kind " << pp.kind() << " address_space " << pp.address_space();
    if (pp.kind() == Processor::FPGA_PROC)
    {
      Memory cpu_mem = Memory::NO_MEMORY;
      Memory fpga_mem = Memory::NO_MEMORY;
      std::set<Memory> visible_mems;
      machine.get_visible_memories(pp, visible_mems);
      for (std::set<Memory>::const_iterator it = visible_mems.begin();
           it != visible_mems.end(); it++)
      {
        if (it->kind() == Memory::FPGA_MEM)
        {
          fpga_mem = *it;
          log_app.print() << "fpga memory: " << *it << " capacity="
                          << (it->capacity() >> 20) << " MB";
        }
        if (it->kind() == Memory::SYSTEM_MEM)
        {
          cpu_mem = *it;
          log_app.print() << "sys memory: " << *it << " capacity="
                          << (it->capacity() >> 20) << " MB";
        }
      }

      Rect<1> bounds(0, MAX_SIZE - 1);

      std::map<FieldID, size_t> deflate_field_sizes;
      deflate_field_sizes[FID_DEFLATE_IN] = sizeof(char);
      deflate_field_sizes[FID_DEFLATE_OUT] = sizeof(char);

      RegionInstance deflate_cpu_inst;
      RegionInstance::create_instance(deflate_cpu_inst, cpu_mem,
                                      bounds, deflate_field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created deflate cpu memory instance: " << deflate_cpu_inst;

      CopySrcDstField cpu_deflate_in_field, cpu_deflate_out_field;
      cpu_deflate_in_field.inst = deflate_cpu_inst;
      cpu_deflate_in_field.field_id = FID_DEFLATE_IN;
      cpu_deflate_in_field.size = sizeof(char);

      cpu_deflate_out_field.inst = deflate_cpu_inst;
      cpu_deflate_out_field.field_id = FID_DEFLATE_OUT;
      cpu_deflate_out_field.size = sizeof(char);

      RegionInstance deflate_fpga_inst;
      RegionInstance::create_instance(deflate_fpga_inst, fpga_mem,
                                      bounds, deflate_field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created deflate fpga memory instance: " << deflate_fpga_inst;

      CopySrcDstField fpga_deflate_in_field, fpga_deflate_out_field;
      fpga_deflate_in_field.inst = deflate_fpga_inst;
      fpga_deflate_in_field.field_id = FID_DEFLATE_IN;
      fpga_deflate_in_field.size = sizeof(char);

      fpga_deflate_out_field.inst = deflate_fpga_inst;
      fpga_deflate_out_field.field_id = FID_DEFLATE_OUT;
      fpga_deflate_out_field.size = sizeof(char);

      AffineAccessor<char, 1> cpu_ra_deflate_in = AffineAccessor<char, 1>(deflate_cpu_inst, FID_DEFLATE_IN);
      // log_app.print("cpu_ra_x = %p fpga_ra_x = %p fpga_ra_x_2 = %p fpga_ra_z_2 = %p\n", cpu_ra_x.ptr(0), fpga_ra_x.ptr(0), fpga_ra_x_2.ptr(0), fpga_ra_z_2.ptr(0));

      // Set up cpu inst and copy to fpga inst
      size_t i;
      for (i = 0; i < size + 1; i++)
      {
        cpu_ra_deflate_in[bounds.lo + i] = text[i];
      }
      Event copy_deflate_in;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(cpu_deflate_in_field);
        dsts.push_back(fpga_deflate_in_field);
        copy_deflate_in = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_deflate_in.wait();

      DEFLATE_Args deflate_args;
      deflate_args.deflate_in_inst = deflate_fpga_inst;
      deflate_args.deflate_out_inst = deflate_fpga_inst;
      deflate_args.bounds = bounds;
      deflate_args.size = size;
      Event e = pp.spawn(DEFLATE_TASK, &deflate_args, sizeof(deflate_args));

      // Copy back
      Event copy_deflate_out;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(fpga_deflate_out_field);
        dsts.push_back(cpu_deflate_out_field);
        copy_deflate_out = bounds.copy(srcs, dsts, ProfilingRequestSet(), e);
      }
      copy_deflate_out.wait();

      AffineAccessor<char, 1> cpu_ra_deflate_out = AffineAccessor<char, 1>(deflate_cpu_inst, FID_DEFLATE_OUT);
      i = bounds.lo;
      int offset, length;
      std::stringstream output_after_lz77;
      while (cpu_ra_deflate_out[i] != '\0') {
        if (cpu_ra_deflate_out[i] == '@') {
            offset = (cpu_ra_deflate_out[i + 1] * 128) + cpu_ra_deflate_out[i + 2];
            length = cpu_ra_deflate_out[i + 3];
            output_after_lz77 << "@(" << offset << "," << length << ")";
            i += 4;
        } else {
            output_after_lz77 << cpu_ra_deflate_out[i];
            i++;
        }
      }
      std::cout << output_after_lz77.str() << std::endl;
      
      /* INFLATE */
      std::map<FieldID, size_t> inflate_field_sizes;
      inflate_field_sizes[FID_INFLATE_IN] = sizeof(char);
      inflate_field_sizes[FID_INFLATE_OUT] = sizeof(char);

      RegionInstance inflate_cpu_inst;
      RegionInstance::create_instance(inflate_cpu_inst, remote_sys_mems[0],
                                      bounds, inflate_field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created inflate cpu memory instance: " << inflate_cpu_inst;

      CopySrcDstField cpu_inflate_in_field, cpu_inflate_out_field;
      cpu_inflate_in_field.inst = inflate_cpu_inst;
      cpu_inflate_in_field.field_id = FID_INFLATE_IN;
      cpu_inflate_in_field.size = sizeof(char);

      cpu_inflate_out_field.inst = inflate_cpu_inst;
      cpu_inflate_out_field.field_id = FID_INFLATE_OUT;
      cpu_inflate_out_field.size = sizeof(char);

      RegionInstance inflate_fpga_inst;
      RegionInstance::create_instance(inflate_fpga_inst, remote_fpga_mems[0],
                                      bounds, inflate_field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created inflate fpga memory instance: " << inflate_fpga_inst;

      CopySrcDstField fpga_inflate_in_field, fpga_inflate_out_field;
      fpga_inflate_in_field.inst = inflate_fpga_inst;
      fpga_inflate_in_field.field_id = FID_INFLATE_IN;
      fpga_inflate_in_field.size = sizeof(char);

      fpga_inflate_out_field.inst = inflate_fpga_inst;
      fpga_inflate_out_field.field_id = FID_INFLATE_OUT;
      fpga_inflate_out_field.size = sizeof(char);

      Event copy_deflate_to_inflate;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(cpu_deflate_out_field);
        dsts.push_back(cpu_inflate_in_field);
        copy_deflate_to_inflate = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_deflate_to_inflate.wait();

      Event copy_inflate_in;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(cpu_inflate_in_field);
        dsts.push_back(fpga_inflate_in_field);
        copy_inflate_in = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_inflate_in.wait();

      INFLATE_Args inflate_args;
      inflate_args.inflate_in_inst = inflate_fpga_inst;
      inflate_args.inflate_out_inst = inflate_fpga_inst;
      inflate_args.bounds = bounds;
      e = remote_processors[0].spawn(INFLATE_TASK, &inflate_args, sizeof(inflate_args));

      // Copy back
      Event copy_inflate_out;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(fpga_inflate_out_field);
        dsts.push_back(cpu_inflate_out_field);
        copy_inflate_out = bounds.copy(srcs, dsts, ProfilingRequestSet(), e);
      }
      copy_inflate_out.wait();
      Event copy_inflate_to_deflate;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(cpu_inflate_out_field);
        dsts.push_back(cpu_deflate_out_field);
        copy_inflate_to_deflate = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_inflate_to_deflate.wait();
      
      AffineAccessor<char, 1> cpu_ra_inflate_out = AffineAccessor<char, 1>(deflate_cpu_inst, FID_DEFLATE_OUT);
      i = bounds.lo;
      while (cpu_ra_inflate_out[i] != '\0') {
        printf("%c", cpu_ra_inflate_out[i]);
        i++;
      }
      printf("\n");
    }
  }

  log_app.print() << "all done!";
}

int main(int argc, char **argv)
{
  sleep(20);
  // the text need to be compressed
  const char *text =
    "To evaluate our prefetcher we modelled the system using the gem5 simulator [4] in full system mode with the setup "
    "given in table 2 and the ARMv8 64-bit instruction set. Our applications are derived from existing benchmarks and "
    "libraries for graph traversal, using a range of graph sizes and characteristics. We simulate the core breadth-first search "
    "based kernels of each benchmark, skipping the graph construction phase. Our first benchmark is from the Graph 500 community [32]. "
    "We used their Kronecker graph generator for both the standard Graph 500 search benchmark and a connected components "
    "calculation. The Graph 500 benchmark is designed to represent data analytics workloads, such as 3D physics "
    "simulation. Standard inputs are too long to simulate, so we create smaller graphs with scales from 16 to 21 and edge "
    "factors from 5 to 15 (for comparison, the Graph 500 toy input has scale 26 and edge factor 16). "
    "Our prefetcher is most easily incorporated into libraries that implement graph traversal for CSR graphs. To this "
    "end, we use the Boost Graph Library (BGL) [41], a C++ templated library supporting many graph-based algorithms "
    "and graph data structures. To support our prefetcher, we added configuration instructions on constructors for CSR "
    "data structures, circular buffer queues (serving as the work list) and colour vectors (serving as the visited list). This "
    "means that any algorithm incorporating breadth-first searches on CSR graphs gains the benefits of our prefetcher without "
    "further modification. We evaluate breadth-first search, betweenness centrality and ST connectivity which all traverse "
    "graphs in this manner. To evaluate our extensions for sequential access prefetching (section 3.5) we use PageRank "
    "and sequential colouring. Inputs to the BGL algorithms are a set of real world "
    "graphs obtained from the SNAP dataset [25] chosen to represent a variety of sizes and disciplines, as shown in table 4. "
    "All are smaller than what we might expect to be processing in a real system, to enable complete simulation in a realistic "
    "time-frame, but as figure 2(a) shows, since stall rates go up for larger data structures, we expect the improvements we "
    "attain in simulation to be conservative when compared with real-world use cases.";
  
  // the size of the text
  size_t size = strlen(text);
  if (size >= MAX_SIZE) {
    perror("text too large");
  }
  Runtime rt;

  rt.init(&argc, &argv);

  rt.register_task(TOP_LEVEL_TASK, top_level_task);

  Processor::register_task_by_kind(Processor::FPGA_PROC, false /*!global*/,
                                   DEFLATE_TASK,
                                   CodeDescriptor(deflate_task),
                                   ProfilingRequestSet())
      .wait();

  Processor::register_task_by_kind(Processor::FPGA_PROC, false /*!global*/,
                                   INFLATE_TASK,
                                   CodeDescriptor(inflate_task),
                                   ProfilingRequestSet())
      .wait();

  // select a processor to run the top level task on
  Processor p = Machine::ProcessorQuery(Machine::get_machine())
                    .only_kind(Processor::LOC_PROC)
                    .first();
  assert(p.exists());

  // collective launch of a single task - everybody gets the same finish event
  Event e = rt.collective_spawn(p, TOP_LEVEL_TASK, text, size);

  // request shutdown once that task is complete
  rt.shutdown(e);

  // now sleep this thread until that shutdown actually happens
  rt.wait_for_shutdown();

  return 0;
}