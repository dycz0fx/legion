#include "simulator.h"
#include <chrono>
#include <algorithm>

using std::cout;
using std::endl;
using std::max;
using std::pair;
using std::string;
using std::to_string;
using std::unordered_map;
using std::vector;

// class Device
Device::Device(string name, DeviceType type, int node_id, int socket_id, int device_id, int max_sub_device = 1)
    : name(name), type(type), node_id(node_id), socket_id(socket_id), device_id(device_id), max_sub_device(max_sub_device)
{
    cur_sub_deivce = 0;
    sub_devices.reserve(max_sub_device);
    for (int i = 0; i < max_sub_device; i++)
    {
        sub_devices.emplace_back(new SubDevice(this, i));
    }
}

SubDevice *Device::get_avail_sub_device()
{
    if (max_sub_device == 1)
    {
        return sub_devices[0];
    }
    else
    {
        SubDevice *ret = sub_devices[cur_sub_deivce++];
        if (cur_sub_deivce == max_sub_device)
        {
            cur_sub_deivce = 0;
        }
        return ret;
    }
}

// class SubDevice
SubDevice::SubDevice(Device *main_device, int sub_device_id)
    : main_device(main_device), sub_device_id(sub_device_id)
{
}

// class CompDevice
CompDevice::CompDevice(std::string name, CompDevType comp_type, int node_id, int socket_id, int device_id, int max_sub_device)
    : Device(name, Device::DEVICE_COMP, node_id, socket_id, device_id, max_sub_device), comp_type(comp_type)
{
}

// class MemDevice
MemDevice::MemDevice(std::string name, MemDevType mem_type, int node_id, int socket_id, int device_id)
    : Device(name, Device::DEVICE_MEM, node_id, socket_id, device_id), mem_type(mem_type)
{
}

// class CommDevice
CommDevice::CommDevice(std::string name, CommDevType comm_type, int node_id, int socket_id, int device_id, float latency, float bandwidth)
    : Device(name, Device::DEVICE_COMM, node_id, socket_id, device_id), comm_type(comm_type), latency(latency), bandwidth(bandwidth)
{
}

// class Task
size_t Task::cur_id{0};

Task::Task(string name, Device *device)
    : name(name), device(device), ready_time(0.0f), counter(0), is_main(false)
{
    next_tasks.clear();
    id = cur_id++;
}

void Task::add_next_task(Task *task)
{
    next_tasks.push_back(task);
    task->counter++;
}

// class CompTask
CompTask::CompTask(std::string name, CompDevice *comp_deivce, float run_time, MemDevice *mem_device)
    : Task(name, comp_deivce), run_time(run_time), mem(mem_device)
{
}

string CompTask::to_string() const
{
    return name + "(" + device->name + ',' + std::to_string(counter) + ',' + std::to_string(run_time) + "ms," + mem->name + ")";
}

float CompTask::cost() const
{
    return run_time;
}

// class CommTask
CommTask::CommTask(string name, CommDevice *comm_device, size_t message_size)
    : Task(name, comm_device), message_size(message_size)
{
}

string CommTask::to_string() const
{
    return name + "(" + device->name + ',' + std::to_string(counter) + ',' + std::to_string(message_size) + "B)";
}

float CommTask::cost() const
{
    CommDevice *comm = (CommDevice *)device;
    return comm->latency + message_size / comm->bandwidth;
}

// class Simulator
Simulator::Simulator(MachineModel *machine) : machine(machine)
{
}

Task *Simulator::new_comp_task(string name, CompDevice *comp_device, float run_time, MemDevice *mem_device)
{
    Task *cur_task = (Task *)new CompTask(name, comp_device, run_time, mem_device);
    return cur_task;
}

void Simulator::new_comm_task(Task *src_task, Task *tar_task, size_t message_size)
{
    vector<CommDevice *> path = machine->get_comm_path(((CompTask *)src_task)->mem, ((CompTask *)tar_task)->mem);
    if (path.empty() or message_size == 0)
    {
        add_dependency(src_task, tar_task);
        return;
    }
    assert(message_size > 0);
    vector<vector<Task *> > all_tasks;
    // Limit the max number of segments per message
    size_t seg_size = machine->default_seg_size;
    int num_segment = message_size / seg_size;
    if (message_size % seg_size != 0)
    {
        num_segment += 1;
    }
    if (num_segment > machine->max_num_segs)
    {
        num_segment = machine->max_num_segs;
        seg_size = message_size / num_segment;
    }
    if (path.size() == 1)
    {
        num_segment = 1;
        seg_size = message_size;
    }
    // Create all the comm tasks
    // Divide messages into segments
    for (int i = 0; i < path.size(); i++)
    {
        all_tasks.push_back({});
        for (int j = 0; j < num_segment; j++)
        {
            size_t cur_seg_size = seg_size;
            if (j == num_segment - 1)
            {
                cur_seg_size = message_size - (num_segment - 1) * seg_size;
            }
            string name = "seg " + to_string(j) + " from " + src_task->name + " to " + tar_task->name;
            if (path[i] == (void *)0x31) {
                printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                printf("%s\n", name.c_str());
            }
            Task *cur_task = (Task *)new CommTask(name, path[i], cur_seg_size);
            all_tasks[i].push_back(cur_task);
        }
    }

    // Add dependencies among the comm tasks
    for (int i = 0; i < path.size(); i++)
    {
        for (int j = 0; j < num_segment; j++)
        {
            if (i == 0)
            {
                add_dependency(src_task, all_tasks[i][j]);
            }
            if (i == path.size() - 1)
            {
                add_dependency(all_tasks[i][j], tar_task);
            }
            if (i > 0)
            {
                add_dependency(all_tasks[i - 1][j], all_tasks[i][j]);
            }
        }
    }

    // Add special dependencies for upi_ins, upi_outs, nic_ins, and nic_outs to prevent communication
    // overlap between upi_ins and upi_outs, and between nic_ins and nic_outs.
    if (num_segment > 1 and path.size() >= 2)
    {
        for (int i = 0; i < path.size(); i++)
        {
            for (int j = 0; j < num_segment - 1; j++)
            {
                if (((CommDevice *)all_tasks[i][j]->device)->comm_type == CommDevice::NIC_IN_COMM or
                    ((CommDevice *)all_tasks[i][j]->device)->comm_type == CommDevice::UPI_IN_COMM)
                {
                    add_dependency(all_tasks[i][j], all_tasks[i - 1][j + 1]);
                }
            }
        }
    }
}

void Simulator::enter_ready_queue(Task *task)
{
    ready_queue.push(task);
}

void Simulator::add_dependency(vector<Task *> prev_tasks, Task *cur_task)
{
    for (int i = 0; i < prev_tasks.size(); i++)
    {
        prev_tasks[i]->add_next_task(cur_task);
    }
}

void Simulator::add_dependency(Task *prev_task, Task *cur_task)
{
    prev_task->add_next_task(cur_task);
}

void Simulator::simulate()
{
    srand(time(NULL));
    bool measure_main_loop = false;
    float main_loop_start = std::numeric_limits<float>::max();
    float main_loop_stop = 0.0f;
    float sim_time = 0.0f;
    float comp_time = 0.0f;
    int comp_count = 0;
    float comm_time = 0.0f;
    unordered_map<SubDevice *, float> device_times;
    while (!ready_queue.empty())
    {
        // Find the task with the earliest start time
        Task *cur_task = ready_queue.top();
        ready_queue.pop();
        float ready_time = 0;
        SubDevice *cur_sub_device = cur_task->device->get_avail_sub_device();
        if (device_times.find(cur_sub_device) != device_times.end())
        {
            ready_time = device_times[cur_sub_device];
        }
        float start_time = max(ready_time, cur_task->ready_time);
        float run_time = 0;
        if (cur_task->device->type == Device::DEVICE_COMP)
        {
            run_time = ((CompTask *)cur_task)->cost();
            comp_time += run_time;
            comp_count++;
        }
        else
        {
            run_time = ((CommTask *)cur_task)->cost();
            comm_time += run_time;
        }
        float end_time = start_time + run_time;
        device_times[cur_sub_device] = end_time;
        if (measure_main_loop and cur_task->is_main)
        {
            main_loop_start = fminf(main_loop_start, start_time);
            main_loop_stop = fmaxf(main_loop_stop, end_time);
        }
        // if (cur_task->device->name == "GPU 4")
        //  if (run_time < 0)
        cout << cur_task->name << " --- " << cur_task->device->name << " --- "
             << "task_ready(" << cur_task->ready_time << ") device_ready(" << ready_time << ") start(" << start_time << ") run(" << run_time << ") end(" << end_time << ")" << endl;
        if (end_time > sim_time)
            sim_time = end_time;
        for (size_t i = 0; i < cur_task->next_tasks.size(); i++)
        {
            Task *next = cur_task->next_tasks[i];
            next->ready_time = max(next->ready_time, end_time);
            next->counter--;
            if (next->counter == 0)
            {
                ready_queue.push(next);
            }
        }
    }
    if (measure_main_loop)
    {
        cout << "main_loop " << main_loop_stop - main_loop_start << "ms" << endl;
    }
    cout << "sim_time " << sim_time << "ms" << endl;
    cout << "total_simulated_comp_tasks " << comp_count << endl;
    cout << "total_comp_time " << comp_time << "ms" << endl;
    cout << "total_comm_time " << comm_time << "ms" << endl;
    return;
}
