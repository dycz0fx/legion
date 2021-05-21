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

#define DATA_SIZE 10
#define ARG_BANK 1
#define CMD_SIZE 4096

#define ARG_IN1_OFFSET 0x10
#define ARG_IN2_OFFSET 0x1c
#define ARG_OUT_OFFSET 0x28
#define ARG_SIZE_OFFSET 0x34

enum
{
  FID_X = 101,
  FID_Y = 102,
  FID_Z = 103,
};

// execute a task on FPGA Processor
Logger log_app("app");

enum
{
  TOP_LEVEL_TASK = Processor::TASK_ID_FIRST_AVAILABLE + 0,
  FPGA_TASK,
};

struct FPGAArgs
{
  const char *xclbin;
  RegionInstance x_inst, y_inst, z_inst;
  Rect<1> bounds;
};

void fpga_task(const void *args, size_t arglen,
               const void *userdata, size_t userlen, Processor p)
{
  size_t i;

  const FPGAArgs &local_args = *(const FPGAArgs *)args;

  // get affine accessors for each of our three instances
  AffineAccessor<int, 1> ra_x = AffineAccessor<int, 1>(local_args.x_inst,
                                                       FID_X);
  AffineAccessor<int, 1> ra_y = AffineAccessor<int, 1>(local_args.y_inst,
                                                       FID_Y);
  AffineAccessor<int, 1> ra_z = AffineAccessor<int, 1>(local_args.z_inst,
                                                       FID_Z);

  //load xclbin
  int fd = open(local_args.xclbin, O_RDONLY);
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

  size_t num_compute_units = 4;
  for (i = 0; i < num_compute_units; i++)
  {
    xclOpenContext(dev_handle, xclbin_uuid, (unsigned int)i, true);
  }

  uint64_t p_base_dev = (uint64_t)FPGAGetBasePtrDev();
  uint64_t p_base_sys = (uint64_t)FPGAGetBasePtrSys();
  uint64_t p_in1 = p_base_dev + (uint64_t)ra_x.ptr(0) - p_base_sys;
  uint64_t p_in2 = p_base_dev + (uint64_t)ra_y.ptr(0) - p_base_sys;
  uint64_t p_out = p_base_dev + (uint64_t)ra_z.ptr(0) - p_base_sys;
  log_app.print("ra_x = %p ra_y = %p ra_z = %p\n", ra_x.ptr(0), ra_y.ptr(0), ra_z.ptr(0));
  log_app.print("p_in1 = %lu p_in2 = %lu p_out = %lu\n", p_in1, p_in2, p_out);

  xclBufferHandle bo_cmd = xclAllocBO(dev_handle, (size_t)CMD_SIZE, 0, XCL_BO_FLAGS_EXECBUF);
  struct ert_start_kernel_cmd *start_cmd = (struct ert_start_kernel_cmd *)xclMapBO(dev_handle, bo_cmd, true);
  memset(start_cmd, 0, CMD_SIZE);
  start_cmd->state = ERT_CMD_STATE_NEW;
  start_cmd->opcode = ERT_START_CU;
  start_cmd->stat_enabled = 1;
  start_cmd->count = 1 + (ARG_SIZE_OFFSET / 4 + 1) + 1;
  start_cmd->cu_mask = (1 << num_compute_units) - 1;
  start_cmd->data[ARG_IN1_OFFSET / 4] = p_in1;
  start_cmd->data[ARG_IN1_OFFSET / 4 + 1] = (p_in1 >> 32) & 0xFFFFFFFF;
  start_cmd->data[ARG_IN2_OFFSET / 4] = p_in2;
  start_cmd->data[ARG_IN2_OFFSET / 4 + 1] = (p_in2 >> 32) & 0xFFFFFFFF;
  start_cmd->data[ARG_OUT_OFFSET / 4] = p_out;
  start_cmd->data[ARG_OUT_OFFSET / 4 + 1] = (p_out >> 32) & 0xFFFFFFFF;
  start_cmd->data[ARG_SIZE_OFFSET / 4] = DATA_SIZE;

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
  log_app.print() << "top task running on " << p;
  Machine machine = Machine::get_machine();
  std::set<Processor> all_processors;
  machine.get_all_processors(all_processors);
  for (std::set<Processor>::const_iterator it = all_processors.begin();
       it != all_processors.end();
       it++)
  {
    Processor pp = (*it);
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

      int init_x_value = 1;
      int init_y_value = 2;
      int init_z_value = 9;

      Rect<1> bounds(0, DATA_SIZE - 1);

      std::map<FieldID, size_t> field_sizes;
      field_sizes[FID_X] = sizeof(int);
      field_sizes[FID_Y] = sizeof(int);
      field_sizes[FID_Z] = sizeof(int);

      RegionInstance cpu_inst;
      RegionInstance::create_instance(cpu_inst, cpu_mem,
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created cpu memory instance: " << cpu_inst;

      CopySrcDstField cpu_x_field, cpu_y_field, cpu_z_field;
      cpu_x_field.inst = cpu_inst;
      cpu_x_field.field_id = FID_X;
      cpu_x_field.size = sizeof(int);

      cpu_y_field.inst = cpu_inst;
      cpu_y_field.field_id = FID_Y;
      cpu_y_field.size = sizeof(int);

      cpu_z_field.inst = cpu_inst;
      cpu_z_field.field_id = FID_Z;
      cpu_z_field.size = sizeof(int);

      RegionInstance fpga_inst;
      RegionInstance::create_instance(fpga_inst, fpga_mem,
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created fpga memory instance: " << fpga_inst;

      CopySrcDstField fpga_x_field, fpga_y_field, fpga_z_field;
      fpga_x_field.inst = fpga_inst;
      fpga_x_field.field_id = FID_X;
      fpga_x_field.size = sizeof(int);

      fpga_y_field.inst = fpga_inst;
      fpga_y_field.field_id = FID_Y;
      fpga_y_field.size = sizeof(int);

      fpga_z_field.inst = fpga_inst;
      fpga_z_field.field_id = FID_Z;
      fpga_z_field.size = sizeof(int);

      RegionInstance fpga_inst_2;
      RegionInstance::create_instance(fpga_inst_2, fpga_mem,
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created fpga memory instance: " << fpga_inst_2;

      CopySrcDstField fpga_x_field_2, fpga_y_field_2, fpga_z_field_2;
      fpga_x_field_2.inst = fpga_inst_2;
      fpga_x_field_2.field_id = FID_X;
      fpga_x_field_2.size = sizeof(int);

      fpga_y_field_2.inst = fpga_inst_2;
      fpga_y_field_2.field_id = FID_Y;
      fpga_y_field_2.size = sizeof(int);

      fpga_z_field_2.inst = fpga_inst_2;
      fpga_z_field_2.field_id = FID_Z;
      fpga_z_field_2.size = sizeof(int);

      AffineAccessor<int, 1> cpu_ra_x = AffineAccessor<int, 1>(cpu_inst, FID_X);
      AffineAccessor<int, 1> fpga_ra_x = AffineAccessor<int, 1>(fpga_inst, FID_X);
      AffineAccessor<int, 1> fpga_ra_x_2 = AffineAccessor<int, 1>(fpga_inst_2, FID_X);
      AffineAccessor<int, 1> fpga_ra_z_2 = AffineAccessor<int, 1>(fpga_inst_2, FID_Z);
      log_app.print("cpu_ra_x = %p fpga_ra_x = %p fpga_ra_x_2 = %p fpga_ra_z_2 = %p\n", cpu_ra_x.ptr(0), fpga_ra_x.ptr(0), fpga_ra_x_2.ptr(0), fpga_ra_z_2.ptr(0));

      //Test fill: fill fpga memory directly
      Event fill_x;
      {
        std::vector<CopySrcDstField> fill_vec;
        fill_vec.push_back(fpga_x_field);
        fill_x = bounds.fill(fill_vec, ProfilingRequestSet(),
                             &init_x_value, sizeof(init_x_value));
      }
      fill_x.wait();
      {
        std::vector<CopySrcDstField> fill_vec;
        fill_vec.push_back(fpga_x_field_2);
        fill_x = bounds.fill(fill_vec, ProfilingRequestSet(),
                             &init_x_value, sizeof(init_x_value));
      }
      fill_x.wait();


      // Event fill_y;
      // {
      //   std::vector<CopySrcDstField> fill_vec;
      //   fill_vec.push_back(fpga_y_field);
      //   fill_y = bounds.fill(fill_vec, ProfilingRequestSet(),
      //     &init_y_value, sizeof(init_y_value));
      // }
      // fill_y.wait();

      Event fill_z;
      {
        std::vector<CopySrcDstField> fill_vec;
        fill_vec.push_back(fpga_z_field);
        fill_z = bounds.fill(fill_vec, ProfilingRequestSet(),
                             &init_z_value, sizeof(init_z_value));
      }
      fill_z.wait();
      {
        std::vector<CopySrcDstField> fill_vec;
        fill_vec.push_back(fpga_z_field_2);
        fill_z = bounds.fill(fill_vec, ProfilingRequestSet(),
                             &init_z_value, sizeof(init_z_value));
      }
      fill_z.wait();

      // fill cpu mem and copy to fpga mem
          Event fill_y_cpu;
      {
        std::vector<CopySrcDstField> fill_vec;
        fill_vec.push_back(cpu_y_field);
        fill_y_cpu = bounds.fill(fill_vec, ProfilingRequestSet(),
                                 &init_y_value, sizeof(init_y_value));
      }
      fill_y_cpu.wait();

      Event copy_y;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(cpu_y_field);
        dsts.push_back(fpga_y_field);
        copy_y = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_y.wait();
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(cpu_y_field);
        dsts.push_back(fpga_y_field_2);
        copy_y = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_y.wait();

      FPGAArgs fpga_args;
      fpga_args.xclbin = "/home/xi/Programs/FPGA/xilinx/vadd/src/kernel/vadd.xclbin";
      fpga_args.x_inst = fpga_inst;
      fpga_args.y_inst = fpga_inst;
      fpga_args.z_inst = fpga_inst_2;
      fpga_args.bounds = bounds;
      Event e = pp.spawn(FPGA_TASK, &fpga_args, sizeof(fpga_args));

      // Copy back
      Event z_ready;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(fpga_z_field_2);
        dsts.push_back(cpu_z_field);
        z_ready = bounds.copy(srcs, dsts, ProfilingRequestSet(), e);
      }
      z_ready.wait();

      AffineAccessor<int, 1> ra_z = AffineAccessor<int, 1>(cpu_inst, FID_Z);
      int i;
      for (i = bounds.lo; i <= bounds.hi; i++)
      {
        printf("%d ", ra_z[i]);
      }
      printf("\n");
      if (i == bounds.hi + 1)
      {
        printf("=======OK=========\n");
      }
    }
  }

  log_app.print() << "all done!";
}

int main(int argc, char **argv)
{
  Runtime rt;

  rt.init(&argc, &argv);

  rt.register_task(TOP_LEVEL_TASK, top_level_task);

  Processor::register_task_by_kind(Processor::FPGA_PROC, false /*!global*/,
                                   FPGA_TASK,
                                   CodeDescriptor(fpga_task),
                                   ProfilingRequestSet())
      .wait();

  // select a processor to run the top level task on
  Processor p = Machine::ProcessorQuery(Machine::get_machine())
                    .only_kind(Processor::LOC_PROC)
                    .first();
  assert(p.exists());

  // collective launch of a single task - everybody gets the same finish event
  Event e = rt.collective_spawn(p, TOP_LEVEL_TASK, 0, 0);

  // request shutdown once that task is complete
  rt.shutdown(e);

  // now sleep this thread until that shutdown actually happens
  rt.wait_for_shutdown();

  return 0;
}