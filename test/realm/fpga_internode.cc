#include "realm.h"
#include "realm/fpga/fpga_utils.h"

using namespace Realm;

enum
{
  FID_DATA = 101,
};

// execute a task on FPGA Processor
Logger log_app("app");

enum
{
  TOP_LEVEL_TASK = Processor::TASK_ID_FIRST_AVAILABLE + 0,
};

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

      Rect<1> bounds(0, size);

      std::map<FieldID, size_t> field_sizes;
      field_sizes[FID_DATA] = sizeof(char);

      RegionInstance local_cpu_inst;
      RegionInstance::create_instance(local_cpu_inst, cpu_mem,
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created local cpu memory instance: " << local_cpu_inst;

      CopySrcDstField local_cpu_field;
      local_cpu_field.inst = local_cpu_inst;
      local_cpu_field.field_id = FID_DATA;
      local_cpu_field.size = sizeof(char);

      RegionInstance local_fpga_inst;
      RegionInstance::create_instance(local_fpga_inst, fpga_mem,
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created local fpga memory instance: " << local_fpga_inst;

      CopySrcDstField local_fpga_field;
      local_fpga_field.inst = local_fpga_inst;
      local_fpga_field.field_id = FID_DATA;
      local_fpga_field.size = sizeof(char);

      AffineAccessor<char, 1> local_cpu_ra = AffineAccessor<char, 1>(local_cpu_inst, FID_DATA);

      // Set up cpu inst and copy to fpga inst
      size_t i;
      for (i = 0; i < size + 1; i++)
      {
        local_cpu_ra[bounds.lo + i] = text[i];
      }
      Event copy_in;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(local_cpu_field);
        dsts.push_back(local_fpga_field);
        copy_in = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_in.wait();
      
      RegionInstance remote_fpga_inst;
      RegionInstance::create_instance(remote_fpga_inst, remote_fpga_mems[0],
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created remote fpga memory instance: " << remote_fpga_inst;

      CopySrcDstField remote_fpga_field;
      remote_fpga_field.inst = remote_fpga_inst;
      remote_fpga_field.field_id = FID_DATA;
      remote_fpga_field.size = sizeof(char);

      Event copy_internode;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(local_fpga_field);
        dsts.push_back(remote_fpga_field);
        copy_internode = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_internode.wait();

      RegionInstance temp_cpu_inst;
      RegionInstance::create_instance(temp_cpu_inst, cpu_mem,
                                      bounds, field_sizes,
                                      0 /*SOA*/, ProfilingRequestSet())
          .wait();
      log_app.print() << "created temp cpu memory instance: " << temp_cpu_inst;

      CopySrcDstField temp_cpu_field;
      temp_cpu_field.inst = temp_cpu_inst;
      temp_cpu_field.field_id = FID_DATA;
      temp_cpu_field.size = sizeof(char);

      // Copy back
      Event copy_out;
      {
        std::vector<CopySrcDstField> srcs, dsts;
        srcs.push_back(remote_fpga_field);
        dsts.push_back(temp_cpu_field);
        copy_out = bounds.copy(srcs, dsts, ProfilingRequestSet());
      }
      copy_out.wait();

      AffineAccessor<char, 1> temp_cpu_ra = AffineAccessor<char, 1>(temp_cpu_inst, FID_DATA);
      i = bounds.lo;
      while (temp_cpu_ra[i] != '\0') {
        printf("%c", temp_cpu_ra[i]);
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
  Runtime rt;

  rt.init(&argc, &argv);

  rt.register_task(TOP_LEVEL_TASK, top_level_task);

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