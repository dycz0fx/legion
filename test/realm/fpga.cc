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

#define DATA_SIZE 1024
#define ARG_BANK 1
#define CMD_SIZE 4096

#define ARG_IN1_OFFSET 0x10
#define ARG_IN2_OFFSET 0x1c
#define ARG_OUT_OFFSET 0x28
#define ARG_SIZE_OFFSET 0x34

// execute a task on FPGA Processor
Logger log_app("app");

enum {
  TOP_LEVEL_TASK = Processor::TASK_ID_FIRST_AVAILABLE+0,
  FPGA_TASK,
};

struct FPGAArgs {
  const char* xclbin;
  int a;
  int b;
  int c;
};

void fpga_task(const void *args, size_t arglen,
                const void *userdata, size_t userlen, Processor p)
{
  size_t i;
  const FPGAArgs& local_args = *(const FPGAArgs *)args;

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
  for (i = 0; i < num_compute_units; i++) {
    xclOpenContext(dev_handle, xclbin_uuid, (unsigned int)i, true);
  }
  size_t data_len = sizeof(int) * DATA_SIZE;
  xclBufferHandle bo_data = xclAllocBO(dev_handle, data_len * 3, 0, ARG_BANK);
  struct xclBOProperties bo_prop;
  xclGetBOProperties(dev_handle, bo_data, &bo_prop);
  uint64_t p_base = bo_prop.paddr;
  uint64_t p_in1 = p_base;
  uint64_t p_in2 = p_base + data_len;
  uint64_t p_out = p_base + 2 * data_len;
  int *data = (int *)xclMapBO(dev_handle, bo_data, true);
  int *expected = (int *)malloc(sizeof(int) * DATA_SIZE);
  // srand((unsigned int)time(NULL));
	for (i = 0; i < DATA_SIZE; i++) {
		data[i] = 1; //rand();
		data[DATA_SIZE + i] = 2; //rand();
		expected[i] = data[i] + data[DATA_SIZE + i];
    data[2 * DATA_SIZE + i] = 0;
	}
  xclSyncBO(dev_handle, bo_data, XCL_BO_SYNC_BO_TO_DEVICE, data_len * 3, 0);
  
  xclBufferHandle bo_cmd = xclAllocBO(dev_handle, (size_t)CMD_SIZE, 0, XCL_BO_FLAGS_EXECBUF);
  struct ert_start_kernel_cmd * start_cmd = (struct ert_start_kernel_cmd *)xclMapBO(dev_handle, bo_cmd, true);
  memset(start_cmd, 0, CMD_SIZE);
  start_cmd->state = ERT_CMD_STATE_NEW;
	start_cmd->opcode = ERT_START_CU;
	start_cmd->stat_enabled = 1;
	start_cmd->count = 1 + (ARG_SIZE_OFFSET/4 + 1) + 1;
	start_cmd->cu_mask = (1 << num_compute_units )-1; 
  start_cmd->data[ARG_IN1_OFFSET/4] = p_in1; 
	start_cmd->data[ARG_IN1_OFFSET/4 + 1] = (p_in1 >> 32) & 0xFFFFFFFF;
	start_cmd->data[ARG_IN2_OFFSET/4] = p_in2; 
	start_cmd->data[ARG_IN2_OFFSET/4 + 1] = (p_in2 >> 32) & 0xFFFFFFFF;
	start_cmd->data[ARG_OUT_OFFSET/4] = p_out; 
	start_cmd->data[ARG_OUT_OFFSET/4 + 1] = (p_out >> 32) & 0xFFFFFFFF;
	start_cmd->data[ARG_SIZE_OFFSET/4] = DATA_SIZE;

  xclExecBuf(dev_handle, bo_cmd);
  struct ert_packet * cmd_packet = (struct ert_packet *)start_cmd;
  while (cmd_packet->state != ERT_CMD_STATE_COMPLETED) {
    xclExecWait(dev_handle, 1000);
  }
  printf("cmd_state = %d\n", cmd_packet->state);

  xclSyncBO(dev_handle, bo_data, XCL_BO_SYNC_BO_FROM_DEVICE, data_len, data_len * 2);
	for (i = 0; i < DATA_SIZE; i++) {
		if (expected[i] != data[2 * DATA_SIZE + i]) {    
      printf("expected=%d data_out=%d\n", expected[i], data[2 * DATA_SIZE + i]);
			printf("data different !! (%lu)\n", i);
			break;
		}
	}
	if (i == DATA_SIZE) {
		printf("### OK ###\n");
	}

  xclUnmapBO(dev_handle, bo_data, data);
  xclUnmapBO(dev_handle, bo_cmd, start_cmd);
  xclFreeBO(dev_handle, bo_data);
  xclFreeBO(dev_handle, bo_cmd);
  for (i = 0; i < num_compute_units; i++) {
		xclCloseContext(dev_handle, xclbin_uuid, (unsigned int)i);
  }
	// xclClose(dev_handle);
}

void top_level_task(const void *args, size_t arglen,
		            const void *userdata, size_t userlen, Processor p)
{
  log_app.print() << "top task running on " << p;
  std::set<Event> finish_events;
  Machine machine = Machine::get_machine();
  std::set<Processor> all_processors;
  machine.get_all_processors(all_processors);
  int id = 0;
  for(std::set<Processor>::const_iterator it = all_processors.begin();
      it != all_processors.end();
	    it++) {
    Processor pp = (*it);
    if(pp.kind() != Processor::FPGA_PROC)
      continue;
    FPGAArgs fpga_args;
    fpga_args.xclbin = "/home/xi/Programs/FPGA/xilinx/vadd/src/kernel/vadd.xclbin";
    fpga_args.a = id;
    fpga_args.b = id+1;
    fpga_args.c = id+2;
    Event e = pp.spawn(FPGA_TASK, &fpga_args, sizeof(fpga_args));
    log_app.print() << "spawn fpga task " << id;
    id++;

    finish_events.insert(e);
  }

  Event merged = Event::merge_events(finish_events);

  merged.wait();

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
                                   ProfilingRequestSet()).wait();

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