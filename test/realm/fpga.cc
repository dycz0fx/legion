#include "realm.h"
#include "realm/fpga/fpga_utils.h"

// XRT includes
#include "experimental/xrt_device.h"
#include "experimental/xrt_kernel.h"
#include "experimental/xrt_bo.h"

using namespace Realm;

// // create device
// auto device = xrt::device(device_index);
// // load xclbin
//  auto uuid = device.load_xclbin(xclbin_fnm);
// // create kernel
//  auto simple = xrt::kernel(device, uuid.get(), "simple");
// // lauch kernel
// auto run = simple(bo0, bo1, 0x10);
//  run.wait();

static const int COUNT = 1024;

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
  const FPGAArgs& local_args = *(const FPGAArgs *)args;
  xrt::device *cur_device = FPGAGetCurrentDevice();
  log_app.print() << "before loading xclbin " << local_args.xclbin;
  std::cout << local_args.xclbin << std::endl;
  xrt::uuid uuid = cur_device->load_xclbin(local_args.xclbin);
  xrt::kernel simple = xrt::kernel(*cur_device, uuid.get(), "simple");
  log_app.print() << "child task on " << p << " " << cur_device;

  const size_t DATA_SIZE = COUNT * sizeof(int);
  xrt::bo bo0 = xrt::bo(*cur_device, DATA_SIZE, simple.group_id(0));
  xrt::bo bo1 = xrt::bo(*cur_device, DATA_SIZE, simple.group_id(1));
  int bo0_map[COUNT];
  int bo1_map[COUNT];
  int expected_results[COUNT];
  for (int i = 0; i < COUNT; i++) {
    bo0_map[i] = local_args.a;
    bo1_map[i] = local_args.b;
    expected_results[i] = local_args.a * local_args.b + local_args.c;
  }
  bo0.write(bo0_map);
  bo1.write(bo1_map);
  bo0.sync(XCL_BO_SYNC_BO_TO_DEVICE, DATA_SIZE, 0);
  bo1.sync(XCL_BO_SYNC_BO_TO_DEVICE, DATA_SIZE, 0);
  xrt::run run = simple(bo0, bo1, local_args.c);
  run.wait();
  bo0.sync(XCL_BO_SYNC_BO_FROM_DEVICE, DATA_SIZE, 0);
  bo0.read(bo0_map);
  if (std::memcmp(bo0_map, expected_results, DATA_SIZE)) {
    throw std::runtime_error("Value read back does not match reference");
  }
  log_app.print() << "child task success";
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
    fpga_args.xclbin = "/home/xi/Programs/XRT/tests/xrt/build/opt/02_simple/kernel.xclbin";
    fpga_args.a = id;
    fpga_args.b = id+1;
    fpga_args.c = id+2;
    Event e = pp.spawn(FPGA_TASK, &fpga_args, sizeof(fpga_args));
    // Event e = pp.spawn(FPGA_TASK, &id, sizeof(id));
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