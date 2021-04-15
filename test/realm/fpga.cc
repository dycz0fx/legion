#include "realm.h"

using namespace Realm;

// execute a task on FPGA Processor

Logger log_app("app");

enum {
  TOP_LEVEL_TASK = Processor::TASK_ID_FIRST_AVAILABLE+0,
  FPGA_TASK,
};

void fpga_task(const void *args, size_t arglen,
                const void *userdata, size_t userlen, Processor p)
{
  log_app.print() << "child task on " << p << ": arglen=" << arglen << ", userlen=" << userlen;
}

void top_level_task(const void *args, size_t arglen,
		            const void *userdata, size_t userlen, Processor p)
{
  log_app.print() << "top task running on " << p;
  std::set<Event> finish_events;
  Machine machine = Machine::get_machine();
  std::set<Processor> all_processors;
  machine.get_all_processors(all_processors);
  int argu = 0;
  for(std::set<Processor>::const_iterator it = all_processors.begin();
      it != all_processors.end();
	    it++) {
    Processor pp = (*it);
    if(pp.kind() != Processor::FPGA_PROC)
      continue;

    Event e = pp.spawn(FPGA_TASK, &argu, sizeof(argu));
    log_app.print() << "spawn fpga task " << argu;
    argu++;

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