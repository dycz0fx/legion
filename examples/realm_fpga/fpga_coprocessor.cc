#include "realm.h"
#include "realm/fpga/fpga_utils.h"
#include <unistd.h>  //sleep

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
    const int *message = (const int *)args;
    size_t size = arglen / sizeof(int);
    log_app.print() << "top task running on " << p;
    Machine machine = Machine::get_machine();
    std::set<Processor> local_processors;
    machine.get_local_processors(local_processors);

    for (std::set<Processor>::const_iterator it = local_processors.begin();
         it != local_processors.end();
         it++)
    {
        Processor pp = (*it);
        log_app.print() << "[local] processor " << pp << " kind " << pp.kind() << " address_space " << pp.address_space();
        if (pp.kind() == Processor::FPGA_PROC)
        {
            Memory cpu_mem = Memory::NO_MEMORY;
            std::set<Memory> visible_mems;
            machine.get_visible_memories(pp, visible_mems);
            for (std::set<Memory>::const_iterator it = visible_mems.begin();
                 it != visible_mems.end(); it++)
            {
                if (it->kind() == Memory::SYSTEM_MEM)
                {
                    cpu_mem = *it;
                    log_app.print() << "sys memory: " << *it << " capacity="
                                    << (it->capacity() >> 20) << " MB";
                }
            }

            Rect<1> bounds(0, size - 1);

            std::map<FieldID, size_t> field_sizes;
            field_sizes[FID_DATA] = sizeof(int);

            RegionInstance local_cpu_inst_0;
            RegionInstance::create_instance(local_cpu_inst_0, cpu_mem,
                                            bounds, field_sizes,
                                            0 /*SOA*/, ProfilingRequestSet())
                .wait();
            log_app.print() << "created local cpu memory 0 instance: " << local_cpu_inst_0;

            CopySrcDstField local_cpu_field_0;
            local_cpu_field_0.inst = local_cpu_inst_0;
            local_cpu_field_0.field_id = FID_DATA;
            local_cpu_field_0.size = sizeof(int);

            RegionInstance local_cpu_inst_1;
            RegionInstance::create_instance(local_cpu_inst_1, cpu_mem,
                                            bounds, field_sizes,
                                            0 /*SOA*/, ProfilingRequestSet())
                .wait();
            log_app.print() << "created local cpu memory 1 instance: " << local_cpu_inst_1;

            CopySrcDstField local_cpu_field_1;
            local_cpu_field_1.inst = local_cpu_inst_1;
            local_cpu_field_1.field_id = FID_DATA;
            local_cpu_field_1.size = sizeof(int);

            AffineAccessor<int, 1> input_ra = AffineAccessor<int, 1>(local_cpu_inst_0, FID_DATA);

            // Set up cpu inst and copy to fpga inst
            printf("init data: ");
            for (size_t i = 0; i < size; i++)
            {
                input_ra[bounds.lo + i] = message[i];
                printf("%d ", input_ra[bounds.lo + i]);
            }
            printf("\n");
            Event copy;
            {
                std::vector<CopySrcDstField> srcs, dsts;
                srcs.push_back(local_cpu_field_0);
                dsts.push_back(local_cpu_field_1);
                copy = bounds.copy(srcs, dsts, ProfilingRequestSet());
            }
            copy.wait();

            printf("final data: ");
            AffineAccessor<int, 1> output_ra = AffineAccessor<int, 1>(local_cpu_inst_1, FID_DATA);
            for (size_t i = 0; i < size; i++)
            {
                printf("%d ", output_ra[bounds.lo + i]);
            }
            printf("\n");
        }
    }

    log_app.print() << "all done!";
}

int main(int argc, char **argv)
{
    // sleep(20);
    size_t size = 12;
    int message[size];
    for (size_t i = 0; i < size; i++)
    {
        message[i] = i;
    }

    Runtime rt;

    rt.init(&argc, &argv);

    rt.register_task(TOP_LEVEL_TASK, top_level_task);

    // select a processor to run the top level task on
    Processor p = Machine::ProcessorQuery(Machine::get_machine())
                      .only_kind(Processor::LOC_PROC)
                      .first();
    assert(p.exists());

    // collective launch of a single task - everybody gets the same finish event
    Event e = rt.collective_spawn(p, TOP_LEVEL_TASK, message, size * sizeof(int));

    // request shutdown once that task is complete
    rt.shutdown(e);

    // now sleep this thread until that shutdown actually happens
    rt.wait_for_shutdown();

    return 0;
}