#include "simulator.h"
#include <unordered_set>
#include <fstream>
#include <utility>
#include <algorithm> // std::min

int num_bgworks;
int default_seg_size;
int max_num_segs;
double realm_comm_overhead;

using std::cout;
using std::endl;
using std::min;
using std::pair;
using std::string;
using std::to_string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

vector<string> split(string srcStr, const string &delim)
{
    int nPos = 0;
    vector<string> vec;
    nPos = srcStr.find(delim.c_str());
    while (-1 != nPos)
    {
        string temp = srcStr.substr(0, nPos);
        vec.push_back(temp);
        srcStr = srcStr.substr(nPos + 1);
        nPos = srcStr.find(delim.c_str());
    }
    vec.push_back(srcStr);
    return vec;
}

void stencil_1d_cpu()
{
    int num_nodes = 2;
    int num_sockets_per_node = 2;
    int num_cpus_per_socket = 2;
    int num_gpus_per_socket = 0;
    int num_sockets = num_nodes * num_sockets_per_node;
    int num_cpus = num_sockets * num_cpus_per_socket;

    // EnhancedMachineModel machine("/Users/xluo/Documents/simulator/final/machine_config");
    SimpleMachineModel machine(num_nodes, num_sockets_per_node * num_cpus_per_socket, num_sockets_per_node * num_gpus_per_socket);

    Simulator simulator(&machine);

    // 1D stencil
    // 0: 0 1
    // 1: 0 1 2
    // 2: 1 2 3
    // 3: 2 3

    // init comp tasks first
    int iters = 2;
    vector<vector<Task *> > comp_tasks;
    for (int t = 0; t < iters; t++)
    {
        comp_tasks.push_back({});
        for (int i = 0; i < num_nodes; i++)
        {
            for (int j = 0; j < num_sockets_per_node; j++)
            {
                for (int k = 0; k < num_cpus_per_socket; k++)
                {
                    int socket_id = i * num_sockets_per_node + j;
                    int device_id = socket_id * num_cpus_per_socket + k;
                    string task_name = "comp_task " + to_string(device_id) + " iter " + to_string(t);
                    float run_time = 1;
                    comp_tasks[t].emplace_back(simulator.new_comp_task(task_name, machine.get_cpu(socket_id, k), run_time, machine.get_sys_mem(socket_id)));
                }
            }
        }
    }

    for (int i = 0; i < comp_tasks[0].size(); i++)
    {
        simulator.enter_ready_queue((Task *)comp_tasks[0][i]);
    }

    // add comm tasks between iters
    for (int i = 1; i < iters; i++)
    {
        for (int j = 0; j < num_cpus; j++)
        {
            long message_size = 262144;
            // left
            int left = j - 1;
            if (left >= 0)
            {
                simulator.new_comm_task(comp_tasks[i - 1][j], comp_tasks[i][left], message_size);
            }
            // right
            int right = j + 1;
            if (right < num_cpus)
            {
                simulator.new_comm_task(comp_tasks[i - 1][j], comp_tasks[i][right], message_size);
            }
            // mid
            simulator.new_comm_task(comp_tasks[i - 1][j], comp_tasks[i][j], message_size);
        }
    }

    simulator.simulate();
}

void stencil_1d_gpu()
{
    int num_nodes = 2;
    int num_sockets_per_node = 1;
    int num_cpus_per_socket = 10;
    int num_gpus_per_socket = 4;
    int num_sockets = num_nodes * num_sockets_per_node;
    int num_cpus = num_sockets * num_cpus_per_socket;

    // EnhancedMachineModel machine("/Users/xluo/Documents/simulator/final/machine_config");
    SimpleMachineModel machine(num_nodes, num_sockets_per_node * num_cpus_per_socket, num_sockets_per_node * num_gpus_per_socket);
    
    Simulator simulator(&machine);

    // init comp tasks first
    int iters = 2;
    vector<vector<Task *> > comp_tasks;
    for (int t = 0; t < iters; t++)
    {
        comp_tasks.push_back({});
        for (int i = 0; i < num_nodes; i++)
        {
            for (int j = 0; j < num_sockets_per_node; j++)
            {
                for (int k = 0; k < num_gpus_per_socket; k++)
                {
                    int socket_id = i * num_sockets_per_node + j;
                    int device_id = socket_id * num_gpus_per_socket + k;
                    string task_name = "comp_task " + to_string(device_id) + " iter " + to_string(t);
                    float run_time = 1;
                    comp_tasks[t].emplace_back(simulator.new_comp_task(task_name, machine.get_gpu(device_id), run_time, machine.get_gpu_fb_mem(device_id)));
                }
            }
        }
    }

    for (int i = 0; i < comp_tasks[0].size(); i++)
    {
        simulator.enter_ready_queue((Task *)comp_tasks[0][i]);
    }

    // add comm tasks between iters
    int num_tasks = num_nodes * num_sockets_per_node * num_gpus_per_socket;
    for (int i = 1; i < iters; i++)
    {
        for (int j = 0; j < num_tasks; j++)
        {
            long message_size = 262144;
            // left
            int left = j - 1;
            if (left >= 0)
            {
                simulator.new_comm_task(comp_tasks[i - 1][j], comp_tasks[i][left], message_size);
            }
            // right
            int right = j + 1;
            if (right < num_tasks)
            {
                simulator.new_comm_task(comp_tasks[i - 1][j], comp_tasks[i][right], message_size);
            }
            // mid
            simulator.new_comm_task(comp_tasks[i - 1][j], comp_tasks[i][j], message_size);
        }
    }

    simulator.simulate();
}

vector<vector<string> > bgwork_ids;
std::unordered_map<std::string, pair<int, int> > cpu_id_map;
std::unordered_map<std::string, pair<int, int> > gpu_id_map;
std::unordered_map<std::string, int> mem_id_map;

// TODO: init id maps automaticly
static void init_id_maps(MachineModel *machine)
{
    // sapling
    {
        /* for circuit, stencil, pennant */
        vector<std::string> bgwork_array;
        bgwork_array.push_back("0000000013");
        bgwork_array.push_back("0000000012");
        bgwork_array.push_back("0000000011");
        bgwork_array.push_back("0000000010");
        bgwork_array.push_back("000000000f");
        bgwork_array.push_back("000000000e");
        bgwork_array.push_back("000000000d");
        bgwork_array.push_back("000000000c");
        bgwork_array.push_back("000000000b");
        bgwork_array.push_back("000000000a");
        bgwork_array.push_back("0000000009");
        bgwork_array.push_back("0000000008");
        bgwork_array.push_back("0000000007");
        bgwork_array.push_back("0000000006");
        bgwork_array.push_back("0000000005");
        bgwork_array.push_back("0000000004");
        bgwork_array.push_back("0000000003");
        bgwork_array.push_back("0000000002");

        // set up bgwork cores: socket_id, device_id
        bgwork_ids.push_back({});
        for (size_t i = 0; i < num_bgworks; i++) {
            bgwork_ids.back().push_back("0x1d0000" + bgwork_array[i]);
        }
        bgwork_ids.push_back({});
        for (size_t i = 0; i < num_bgworks; i++) {
            bgwork_ids.back().push_back("0x1d0001" + bgwork_array[i]);
        }
        bgwork_ids.push_back({});
        for (size_t i = 0; i < num_bgworks; i++) {
            bgwork_ids.back().push_back("0x1d0002" + bgwork_array[i]);
        }
        bgwork_ids.push_back({});
        for (size_t i = 0; i < num_bgworks; i++) {
            bgwork_ids.back().push_back("0x1d0003" + bgwork_array[i]);
        }
        // set up cpu_id_map: device_id
        cpu_id_map["0x1d00000000000000"] = {0, 0};
        cpu_id_map["0x1d00000000000001"] = {0, 1};
        cpu_id_map["0x1d00000000000002"] = {0, 2};
        cpu_id_map["0x1d00000000000003"] = {0, 3};
        cpu_id_map["0x1d00000000000004"] = {0, 4};
        cpu_id_map["0x1d00000000000005"] = {0, 5};
        cpu_id_map["0x1d00000000000006"] = {0, 6};
        cpu_id_map["0x1d00000000000007"] = {0, 7};
        cpu_id_map["0x1d00000000000008"] = {0, 8};
        cpu_id_map["0x1d00000000000009"] = {0, 9};
        cpu_id_map["0x1d0000000000000a"] = {0, 10};
        cpu_id_map["0x1d0000000000000b"] = {0, 11};
        cpu_id_map["0x1d0000000000000c"] = {0, 12};
        cpu_id_map["0x1d0000000000000d"] = {0, 13};
        cpu_id_map["0x1d0000000000000e"] = {0, 14};
        cpu_id_map["0x1d0000000000000f"] = {0, 15};
        cpu_id_map["0x1d00000000000010"] = {0, 16};
        cpu_id_map["0x1d00000000000011"] = {0, 17};
        cpu_id_map["0x1d00000000000012"] = {0, 18};
        cpu_id_map["0x1d00000000000013"] = {0, 19};

        cpu_id_map["0x1d00010000000000"] = {1, 20};
        cpu_id_map["0x1d00010000000001"] = {1, 21};
        cpu_id_map["0x1d00010000000002"] = {1, 22};
        cpu_id_map["0x1d00010000000003"] = {1, 23};
        cpu_id_map["0x1d00010000000004"] = {1, 24};
        cpu_id_map["0x1d00010000000005"] = {1, 25};
        cpu_id_map["0x1d00010000000006"] = {1, 26};
        cpu_id_map["0x1d00010000000007"] = {1, 27};
        cpu_id_map["0x1d00010000000008"] = {1, 28};
        cpu_id_map["0x1d00010000000009"] = {1, 29};
        cpu_id_map["0x1d0001000000000a"] = {1, 30};
        cpu_id_map["0x1d0001000000000b"] = {1, 31};
        cpu_id_map["0x1d0001000000000c"] = {1, 32};
        cpu_id_map["0x1d0001000000000d"] = {1, 33};
        cpu_id_map["0x1d0001000000000e"] = {1, 34};
        cpu_id_map["0x1d0001000000000f"] = {1, 35};
        cpu_id_map["0x1d00010000000010"] = {1, 36};
        cpu_id_map["0x1d00010000000011"] = {1, 37};
        cpu_id_map["0x1d00010000000012"] = {1, 38};
        cpu_id_map["0x1d00010000000013"] = {1, 39};

        cpu_id_map["0x1d00020000000000"] = {2, 40};
        cpu_id_map["0x1d00020000000001"] = {2, 41};
        cpu_id_map["0x1d00020000000002"] = {2, 42};
        cpu_id_map["0x1d00020000000003"] = {2, 43};
        cpu_id_map["0x1d00020000000004"] = {2, 44};
        cpu_id_map["0x1d00020000000005"] = {2, 45};
        cpu_id_map["0x1d00020000000006"] = {2, 46};
        cpu_id_map["0x1d00020000000007"] = {2, 47};
        cpu_id_map["0x1d00020000000008"] = {2, 48};
        cpu_id_map["0x1d00020000000009"] = {2, 49};
        cpu_id_map["0x1d0002000000000a"] = {2, 50};
        cpu_id_map["0x1d0002000000000b"] = {2, 51};
        cpu_id_map["0x1d0002000000000c"] = {2, 52};
        cpu_id_map["0x1d0002000000000d"] = {2, 53};
        cpu_id_map["0x1d0002000000000e"] = {2, 54};
        cpu_id_map["0x1d0002000000000f"] = {2, 55};
        cpu_id_map["0x1d00020000000010"] = {2, 56};
        cpu_id_map["0x1d00020000000011"] = {2, 57};
        cpu_id_map["0x1d00020000000012"] = {2, 58};
        cpu_id_map["0x1d00020000000013"] = {2, 59};

        cpu_id_map["0x1d00030000000000"] = {3, 60};
        cpu_id_map["0x1d00030000000001"] = {3, 61};
        cpu_id_map["0x1d00030000000002"] = {3, 62};
        cpu_id_map["0x1d00030000000003"] = {3, 63};
        cpu_id_map["0x1d00030000000004"] = {3, 64};
        cpu_id_map["0x1d00030000000005"] = {3, 65};
        cpu_id_map["0x1d00030000000006"] = {3, 66};
        cpu_id_map["0x1d00030000000007"] = {3, 67};
        cpu_id_map["0x1d00030000000008"] = {3, 68};
        cpu_id_map["0x1d00030000000009"] = {3, 69};
        cpu_id_map["0x1d0003000000000a"] = {3, 70};
        cpu_id_map["0x1d0003000000000b"] = {3, 71};
        cpu_id_map["0x1d0003000000000c"] = {3, 72};
        cpu_id_map["0x1d0003000000000d"] = {3, 73};
        cpu_id_map["0x1d0003000000000e"] = {3, 74};
        cpu_id_map["0x1d0003000000000f"] = {3, 75};
        cpu_id_map["0x1d00030000000010"] = {3, 76};
        cpu_id_map["0x1d00030000000011"] = {3, 77};
        cpu_id_map["0x1d00030000000012"] = {3, 78};
        cpu_id_map["0x1d00030000000013"] = {3, 79};

        // set up mem_id_map for system memory, registered memory and zero-copy memory : socket_id
        mem_id_map["0x1e00000000000000"] = {0}; // system memory
        mem_id_map["0x1e00000000000007"] = {0}; // registered memory
        mem_id_map["0x1e00000000000005"] = {0}; // zero-copy memory
        mem_id_map["0x1e00010000000000"] = {1};
        mem_id_map["0x1e00010000000007"] = {1};
        mem_id_map["0x1e00010000000005"] = {1};
        mem_id_map["0x1e00020000000000"] = {2};
        mem_id_map["0x1e00020000000007"] = {2};
        mem_id_map["0x1e00020000000005"] = {2};
        mem_id_map["0x1e00030000000000"] = {3};
        mem_id_map["0x1e00030000000007"] = {3};
        mem_id_map["0x1e00030000000005"] = {3};

        // set up gpu_id_map: socket_id device_id
        gpu_id_map["0x1d00000000000012"] = {0, 0};
        gpu_id_map["0x1d00000000000013"] = {0, 1};
        gpu_id_map["0x1d00000000000014"] = {0, 2};
        gpu_id_map["0x1d00000000000015"] = {0, 3};
        gpu_id_map["0x1d00010000000012"] = {1, 4};
        gpu_id_map["0x1d00010000000013"] = {1, 5};
        gpu_id_map["0x1d00010000000014"] = {1, 6};
        gpu_id_map["0x1d00010000000015"] = {1, 7};
        gpu_id_map["0x1d00020000000012"] = {2, 8};
        gpu_id_map["0x1d00020000000013"] = {2, 9};
        gpu_id_map["0x1d00020000000014"] = {2, 10};
        gpu_id_map["0x1d00020000000015"] = {2, 11};
        gpu_id_map["0x1d00030000000012"] = {3, 12};
        gpu_id_map["0x1d00030000000013"] = {3, 13};
        gpu_id_map["0x1d00030000000014"] = {3, 14};
        gpu_id_map["0x1d00030000000015"] = {3, 15};

        // set up mem_id_map for gpu framebuffer memory
        mem_id_map["0x1e00000000000001"] = {0};
        mem_id_map["0x1e00000000000002"] = {1};
        mem_id_map["0x1e00000000000003"] = {2};
        mem_id_map["0x1e00000000000004"] = {3};
        mem_id_map["0x1e00010000000001"] = {4};
        mem_id_map["0x1e00010000000002"] = {5};
        mem_id_map["0x1e00010000000003"] = {6};
        mem_id_map["0x1e00010000000004"] = {7};
        mem_id_map["0x1e00020000000001"] = {8};
        mem_id_map["0x1e00020000000002"] = {9};
        mem_id_map["0x1e00020000000003"] = {10};
        mem_id_map["0x1e00020000000004"] = {11};
        mem_id_map["0x1e00030000000001"] = {12};
        mem_id_map["0x1e00030000000002"] = {13};
        mem_id_map["0x1e00030000000003"] = {14};
        mem_id_map["0x1e00030000000004"] = {15};
    }

    // summit
    {
    //     int num_nodes = machine->get_num_nodes();
    //     int num_sockets_per_node = machine->get_num_sockets_per_node();
    //     int num_cpus_per_socket = machine->get_num_cpus_per_socket();
    //     int num_gpus_per_socket = machine->get_num_gpus_per_socket();

    //     /* for circuit, stencil, pennant */
    //     vector<std::string> bgwork_array;
    //     bgwork_array.push_back("0000000029");
    //     bgwork_array.push_back("0000000028");
    //     bgwork_array.push_back("0000000027");
    //     bgwork_array.push_back("0000000026");

    //     // set up bgwork cores: node_id, device_id
    //     for (size_t i = 0; i < num_nodes; i++)
    //     {
    //         bgwork_ids.push_back({});
    //         std::stringstream stream;
    //         stream << std::hex << i;
    //         std::string node_str = stream.str();
    //         if (node_str.size() < 4)
    //         {
    //             node_str.insert(0, 4 - node_str.size(), '0');
    //         }
    //         for (size_t j = 0; j < num_bgworks; j++)
    //         {
    //             std::string device_str = "0x1d" + node_str + bgwork_array[j];
    //             std::cout << "bgwork_ids: " << i << ", " << device_str << std::endl;
    //             bgwork_ids.back().push_back(device_str);
    //         }
    //     }

    //     // set up cpu_id_map: node_id, device_id
    //     vector<std::string> cpu_array;
    //     cpu_array.push_back("0000000000"); // utility
    //     cpu_array.push_back("0000000001"); // utility
    //     cpu_array.push_back("0000000002"); // utility
    //     cpu_array.push_back("0000000003"); // utility
    //     cpu_array.push_back("0000000004"); // cpu
    //     cpu_array.push_back("0000000005"); // cpu
    //     cpu_array.push_back("0000000006"); // cpu
    //     cpu_array.push_back("0000000007"); // cpu
    //     cpu_array.push_back("0000000008"); // cpu
    //     cpu_array.push_back("0000000009"); // cpu
    //     cpu_array.push_back("000000000a"); // cpu
    //     cpu_array.push_back("000000000b"); // cpu
    //     cpu_array.push_back("000000000c"); // cpu
    //     cpu_array.push_back("000000000d"); // cpu
    //     cpu_array.push_back("000000000e"); // cpu
    //     cpu_array.push_back("000000000f"); // cpu
    //     cpu_array.push_back("0000000010"); // cpu
    //     cpu_array.push_back("0000000011"); // cpu
    //     cpu_array.push_back("0000000012"); // cpu
    //     cpu_array.push_back("0000000013"); // cpu
    //     cpu_array.push_back("0000000014"); // cpu
    //     cpu_array.push_back("0000000015"); // cpu
    //     cpu_array.push_back("0000000016"); // cpu
    //     cpu_array.push_back("0000000017"); // cpu
    //     cpu_array.push_back("0000000018"); // cpu
    //     cpu_array.push_back("0000000019"); // cpu
    //     cpu_array.push_back("000000001a"); // cpu
    //     cpu_array.push_back("000000001b"); // cpu
    //     cpu_array.push_back("000000001c"); // cpu
    //     cpu_array.push_back("000000001d"); // cpu
    //     cpu_array.push_back("000000001e"); // cpu
    //     cpu_array.push_back("000000001f"); // cpu
    //     cpu_array.push_back("0000000020"); // cpu
    //     cpu_array.push_back("0000000021"); // cpu
    //     cpu_array.push_back("0000000022"); // cpu
    //     cpu_array.push_back("0000000023"); // cpu
    //     cpu_array.push_back("0000000024"); // cpu
    //     cpu_array.push_back("0000000025"); // cpu
    //     cpu_array.push_back("0000000026"); // bgwork
    //     cpu_array.push_back("0000000027"); // bgwork
    //     cpu_array.push_back("0000000028"); // bgwork
    //     cpu_array.push_back("0000000029"); // bgwork
    //     if (cpu_array.size() != num_cpus_per_socket * num_sockets_per_node) {
    //         assert(false);
    //     }
    //     for (size_t i = 0; i < num_nodes; i++)
    //     {
    //         std::stringstream stream;
    //         stream << std::hex << i;
    //         string node_str = stream.str();
    //         if (node_str.size() < 4)
    //         {
    //             node_str.insert(0, 4 - node_str.size(), '0');
    //         }
    //         for (size_t j = 0; j < num_cpus_per_socket * num_sockets_per_node; j++)
    //         {
    //             std::string device_str = "0x1d" + node_str + cpu_array[j];
    //             int device_id = i * num_sockets_per_node * num_cpus_per_socket + j;
    //             std::cout << "cpu_id_map: " << device_str << " -> (" << i << ", " << device_id << ")" << std::endl;
    //             cpu_id_map[device_str] = {i, device_id};
    //         }
    //     }

    //     // set up mem_id_map for system memory, registered memory and zero-copy memory : node_id
    //     vector<std::string> mem_array;
    //     mem_array.push_back("0000000000"); // system memory
    //     mem_array.push_back("0000000009"); // registered memory
    //     mem_array.push_back("0000000007"); // zero-copy memory

    //     for (size_t i = 0; i < num_nodes; i++)
    //     {
    //         std::stringstream stream;
    //         stream << std::hex << i;
    //         string node_str = stream.str();
    //         if (node_str.size() < 4)
    //         {
    //             node_str.insert(0, 4 - node_str.size(), '0');
    //         }
    //         for (size_t j = 0; j < mem_array.size(); j++)
    //         {
    //             std::string device_str = "0x1e" + node_str + mem_array[j];
    //             std::cout << "mem_id_map: " << device_str << " -> (" << i << ")" << std::endl;
    //             mem_id_map[device_str] = i;
    //         }
    //     }

    //     // set up gpu_id_map: socket_id device_id
    //     vector<std::string> gpu_array;
    //     gpu_array.push_back("0000000026"); // gpu
    //     gpu_array.push_back("0000000027"); // gpu
    //     gpu_array.push_back("0000000028"); // gpu
    //     gpu_array.push_back("0000000029"); // gpu
    //     gpu_array.push_back("000000002a"); // gpu
    //     gpu_array.push_back("000000002b"); // gpu

    //     for (size_t i = 0; i < num_nodes; i++)
    //     {
    //         for (size_t j = 0; j < num_sockets_per_node; j++)
    //         {
    //             int socket_id = i * num_sockets_per_node + j;
    //             std::stringstream stream;
    //             stream << std::hex << i;
    //             string node_str = stream.str();
    //             if (node_str.size() < 4)
    //             {
    //                 node_str.insert(0, 4 - node_str.size(), '0');
    //             }
    //             for (size_t k = 0; k < num_gpus_per_socket; k++)
    //             {
    //                 int node_local_id = j * num_gpus_per_socket + k;
    //                 int device_id = socket_id * num_gpus_per_socket + k;
    //                 std::string device_str = "0x1d" + node_str + gpu_array[node_local_id];
    //                 std::cout << "gpu_id_map: " << device_str << " -> (" << socket_id << ", " << device_id << ")" << std::endl;
    //                 gpu_id_map[device_str] = {socket_id, device_id};
    //             }
    //         }
    //     }

    //     // set up mem_id_map for gpu framebuffer memory
    //     mem_array.clear();
    //     mem_array.push_back("0000000001"); // fb
    //     mem_array.push_back("0000000002"); // fb
    //     mem_array.push_back("0000000003"); // fb
    //     mem_array.push_back("0000000004"); // fb
    //     mem_array.push_back("0000000005"); // fb
    //     mem_array.push_back("0000000006"); // fb

    //     for (size_t i = 0; i < num_nodes; i++)
    //     {
    //         for (size_t j = 0; j < num_sockets_per_node; j++)
    //         {
    //             int socket_id = i * num_sockets_per_node + j;
    //             std::stringstream stream;
    //             stream << std::hex << i;
    //             string node_str = stream.str();
    //             if (node_str.size() < 4)
    //             {
    //                 node_str.insert(0, 4 - node_str.size(), '0');
    //             }
    //             for (size_t k = 0; k < num_gpus_per_socket; k++)
    //             {
    //                 int deivde_id = socket_id * num_gpus_per_socket + k;
    //                 int node_local_id = j * num_gpus_per_socket + k;
    //                 std::string device_str = "0x1d" + node_str + mem_array[node_local_id];
    //                 std::cout << "mem_id_map: " << device_str << " -> (" << deivde_id << ")" << std::endl;
    //                 mem_id_map[device_str] = deivde_id;
    //             }
    //         }
    //     }
    }
}
// create machine model
EnhancedMachineModel *create_enhanced_machine_model(string machine_config)
{
    EnhancedMachineModel *machine = new EnhancedMachineModel(machine_config);
    machine->default_seg_size = default_seg_size;
    machine->max_num_segs = max_num_segs;
    machine->realm_comm_overhead = realm_comm_overhead;
    std::cout << machine->to_string() << std::endl;
    init_id_maps(machine);
    return machine;
}

SimpleMachineModel *create_simple_machine_model()
{
    SimpleMachineModel *machine = new SimpleMachineModel(4, 40, 6);
    std::cout << machine->to_string() << std::endl;
    init_id_maps(machine);
    return machine;
}

void run_dag_file(Simulator &simulator, MachineModel *machine, string folder)
{
    unordered_map<int, float> cost_map;
    // get costs of tasks
    std::ifstream cost_file(folder + "/cost");
    int uid;
    float cost;
    while (cost_file >> uid >> cost)
    {
        // cout << uid << " " << cost << endl;
        if (cost_map.find(uid) == cost_map.end())
        {
            cost_map[uid] = cost * 0.001; // us -> ms
        }
        else
        {
            cout << "Has duplicate uid in cost file" << endl;
        }
    }

    int num_comp_tasks = 0;
    int num_comm_tasks = 0;
    unordered_map<string, Task *> comp_tasks_map;
    unordered_map<string, long> messages_map;
    unordered_set<string> left;
    unordered_set<string> right;
    unordered_map<int, int> alias_map;
    // get alias
    std::ifstream alias_file(folder + "/alias");
    if (alias_file.is_open())
    {
        std::string line;
        while (std::getline(alias_file, line))
        {
            vector<string> line_array = split(line, " ");
            if (line_array[0] == "alias:")
            {
                int t0 = stoi(line_array[1]);
                int t1 = stoi(line_array[2]);
                alias_map[t0] = t1;
                alias_map[t1] = t0;
            }
        }
    }

    // get comp tasks
    std::ifstream comp_file(folder + "/comp");
    if (comp_file.is_open())
    {
        std::string line;
        while (std::getline(comp_file, line))
        {
            vector<string> line_array = split(line, " ");
            /*
            for (int i = 0; i < line_array.size(); i++) {
                cout << line_array[i] << " ";
            }
            cout << endl;
            */
            if (line_array[0] == "comp:")
            {
                num_comp_tasks++;
                int task_id = -1;
                bool is_main = false;
                bool is_skip = false;
                CompDevice *comp_device = nullptr;
                MemDevice *mem_device = nullptr;
                if ((line_array[1] == "Conv2D" and line_array[2] == "Forward")
                    // or (line_array[1] == "SGD" and line_array[2] == "Parameter")
                )
                {
                    is_main = true;
                }
                // if ((line_array[1] == "Conv2D" and line_array[2] == "Init")
                //     or (line_array[1] == "Load" and line_array[2] == "Entire" and line_array[3] == "Dataset")
                //     //or (line_array[1] == "Zero" and line_array[2] == "Init")
                //     ) {
                //     is_skip = true;
                // }
                for (int i = 0; i < line_array.size(); i++)
                {
                    // skip some init tasks
                    if (line_array[i] == "(UID:")
                    {
                        task_id = stoi(line_array[i + 1].substr(0, line_array[i + 1].size() - 1));
                    }
                    if (line_array[i] == "Processor:")
                    {
                        if (line_array[i + 1] == "CPU")
                        {
                            pair<int, int> ids = cpu_id_map[line_array.back()];
                            comp_device = machine->get_cpu(ids.second);
                            // TODO: set up mem from comm
                            mem_device = machine->get_sys_mem(ids.first);
                        }
                        else if (line_array[i + 1] == "GPU")
                        {
                            pair<int, int> ids = gpu_id_map[line_array.back()];
                            comp_device = machine->get_gpu(ids.second);
                            mem_device = machine->get_gpu_fb_mem(ids.second);
                        }
                        else
                        {
                            cout << "Unknow type of processor " << line_array[i + 1] << endl;
                        }
                        break;
                    }
                }
                string task_name = "op_node_" + to_string(task_id);
                float cost = 0.0;
                if (cost_map.find(task_id) != cost_map.end())
                {
                    cost = cost_map[task_id];
                }
                else
                {
                    cost = cost_map[alias_map[task_id]];
                    cout << "========= task " << task_id << " has alias " << alias_map[task_id] << " with cost " << cost << endl;
                }
                if (is_skip)
                {
                    cost = 0.0;
                }
                Task *cur_task = simulator.new_comp_task(task_name, comp_device, cost, mem_device);
                cur_task->is_main = is_main;
                // cout << cur_task->to_string() << endl;
                comp_tasks_map[task_name] = cur_task;
            }
            else
            {
                cout << "error" << endl;
            }
        }
        comp_file.close();
    }
    // get comm tasks
    std::ifstream comm_file(folder + "/comm");
    if (comm_file.is_open())
    {
        std::string line;
        while (std::getline(comm_file, line))
        {
            vector<string> line_array = split(line, " ");
            /*
            for (int i = 0; i < line_array.size(); i++) {
                cout << line_array[i] << " ";
            }
            cout << endl;
            */
            if (line_array[0] == "comm:")
            {
                num_comm_tasks++;
                int loc = 2;
                if (line_array[loc] == "'Realm" and (line_array[loc + 1] == "Copy" or line_array[loc + 1] == "Fill"))
                {
                    string task_name;
                    string comp_device_id = line_array.back().substr(0, line_array.back().size() - 3);
                    string comp_device_type = line_array[line_array.size() - 3];
                    comp_device_type = comp_device_type.substr(comp_device_type.size() - 3, 3);
                    string realm_id = line_array[loc + 2].substr(1, line_array[loc + 2].size() - 2);
                    string tar_mem_device_id = "";
                    string tar_mem_device_type = "";
                    // cout << realm_id << endl;
                    if (line_array[loc + 1] == "Copy")
                    {
                        task_name = "realm_copy_" + realm_id;
                        tar_mem_device_id = line_array[line_array.size() - 4];
                        tar_mem_device_id = tar_mem_device_id.substr(0, tar_mem_device_id.size() - 1);
                        tar_mem_device_type = line_array[line_array.size() - 6];
                    }
                    else
                    {
                        task_name = "realm_fill_" + realm_id;
                        tar_mem_device_id = line_array[line_array.size() - 4];
                        tar_mem_device_id = tar_mem_device_id.substr(0, tar_mem_device_id.size() - 1);
                        tar_mem_device_type = line_array[line_array.size() - 6];
                    }
                    // cout << task_name << " " << comp_device_type << "-" << comp_device_id << " " << tar_mem_device_type << "-" << tar_mem_device_id << endl;
                    Task *cur_task;
                    CompDevice *comp_device = NULL;
                    MemDevice *mem_device = NULL;
                    int random_bgwork_id = rand() % bgwork_ids[0].size();
                    if (tar_mem_device_type == "System" or tar_mem_device_type == "Zero-Copy")
                    {
                        int socket_id = mem_id_map[tar_mem_device_id];
                        mem_device = machine->get_sys_mem(socket_id);
                        comp_device = machine->get_cpu(socket_id, 0); // handle processor type unknown, but memory type is available
                    }
                    else if (tar_mem_device_type == "Framebuffer")
                    {
                        int device_id = mem_id_map[tar_mem_device_id];
                        mem_device = machine->get_gpu_fb_mem(device_id);
                        comp_device = machine->get_gpu(device_id); // handle processor type unknown, but memory type is available
                    }
                    else
                    {
                        cout << "wrong tar_mem_device_type" << endl;
                        assert(0);
                    }
                    if (comp_device_type == "CPU")
                    {
                        pair<int, int> ids = cpu_id_map[comp_device_id];
                        // comp_device = machine->get_cpu(ids.second);
                        pair<int, int> temp_ids = cpu_id_map[bgwork_ids[ids.first][random_bgwork_id]];
                        comp_device = machine->get_cpu(temp_ids.second);
                    }
                    else if (comp_device_type == "GPU")
                    {
                        pair<int, int> ids = gpu_id_map[comp_device_id];
                        comp_device = machine->get_gpu(ids.second);
                    }
                    if (comp_device == NULL)
                    {
                        cout << "wrong comp_device_type" << endl;
                        assert(0);
                    }

                    cur_task = simulator.new_comp_task(task_name, comp_device, machine->realm_comm_overhead, mem_device);
                    // cout << cur_task->to_string() << endl;
                    comp_tasks_map[task_name] = cur_task;
                    long index_size = 0, field_size = 0;
                    string task_uid = "";
                    for (int i = 0; i < line_array.size(); i++)
                    {
                        if (line_array[i] == "Index_Space_Size:")
                        {
                            index_size = stol(line_array[i + 1]);
                        }
                        if (line_array[i] == "Field_Size:")
                        {
                            field_size = stol(line_array[i + 1]);
                            break;
                        }
                        // Realm Fill do not have "UID:" in comm
                        if (line_array[i] == "(UID:")
                        {
                            task_uid = line_array[i + 1];
                            task_uid = task_uid.substr(0, task_uid.size() - 3);
                        }
                    }
                    assert(index_size > 0 and field_size > 0);
                    messages_map[task_name] = index_size * field_size;
                    // cout << task_name << " - " << index_size * field_size << " bytes" << endl;
                }
                else
                {
                    cout << line << endl;
                    cout << "comm: has other types" << endl;
                    assert(0);
                }
            }
            else
            {
                cout << "error" << endl;
            }
        }
        comp_file.close();
    }
    // get deps
    std::ifstream deps_file(folder + "/deps");
    if (deps_file.is_open())
    {
        std::string line;
        while (std::getline(deps_file, line))
        {
            vector<string> line_array = split(line, " ");
            /*
            for (int i = 0; i < line_array.size(); i++) {
                cout << line_array[i] << " ";
            }
            cout << endl;
            */
            if (line_array[0] == "deps:")
            {
                // cout << line << endl;
                // cout << line_array[1] << " " << line_array[3] << endl;
                if (comp_tasks_map.find(line_array[1]) != comp_tasks_map.end() and
                    comp_tasks_map.find(line_array[3]) != comp_tasks_map.end())
                {
                    long message_size = 0;
                    // realm_copy has to depend on some tasks
                    if (starts_with(line_array[3], "realm_copy"))
                    {
                        message_size = messages_map[line_array[3]];
                    }
                    else if (starts_with(line_array[1], "realm_copy"))
                    {
                        message_size = 0;
                    }
                    // realm_fill do not has to depend on some tasks
                    else if (starts_with(line_array[3], "realm_fill"))
                    {
                        message_size = 0;
                    }
                    else if (starts_with(line_array[1], "realm_fill"))
                    {
                        message_size = messages_map[line_array[1]];
                    }
                    simulator.new_comm_task(comp_tasks_map[line_array[1]], comp_tasks_map[line_array[3]], message_size);
                    if (left.find(line_array[1]) == left.end())
                    {
                        left.insert(line_array[1]);
                    }
                    if (right.find(line_array[3]) == right.end())
                    {
                        right.insert(line_array[3]);
                    }
                }
                else
                {
                    if (comp_tasks_map.find(line_array[1]) == comp_tasks_map.end())
                        // cout << "deps: cannot find " << line_array[1] << endl;
                        ;
                    if (comp_tasks_map.find(line_array[3]) == comp_tasks_map.end())
                        // cout << "deps: cannot find " << line_array[3] << endl;
                        ;
                }
            }
            else
            {
                cout << "error" << endl;
            }
        }
        deps_file.close();
    }

    for (auto i : left)
    {
        if (right.find(i) == right.end())
        {
            cout << "starts with:" << i << endl;
            simulator.enter_ready_queue(comp_tasks_map[i]);
        }
    }

    simulator.simulate();
    cout << "num_comp_tasks " << num_comp_tasks << endl;
    cout << "num_comm_tasks " << num_comm_tasks << endl;
}

// max_peer: the number of concurrent communications
void test_comm(Simulator &simulator, MachineModel *machine, size_t message_size, int max_peer)
{
    cout << machine->to_string();
    int num_nodes = 2; // only need 2 nodees
    int num_sockets_per_node = machine->get_num_sockets_per_node();
    int num_cpus_per_socket = machine->get_num_cpus_per_socket();
    int num_gpus_per_socket = machine->get_num_gpus_per_socket();

    int task_id = 0;
    vector<Task *> comp_tasks;
    for (int i = 0; i < num_nodes; i++)
    {
        for (int j = 0; j < num_sockets_per_node; j++)
        {
            for (int k = 0; k < num_cpus_per_socket; k++)
            {
                int socket_id = i * num_sockets_per_node + j;
                int device_id = socket_id * num_cpus_per_socket + k;
                string task_name = "comp_task " + to_string(task_id++) + " on socket " + to_string(socket_id) + " cpu " + to_string(k) + " device_id " + to_string(device_id);
                float run_time = 0;
                comp_tasks.emplace_back(simulator.new_comp_task(task_name, machine->get_cpu(device_id), run_time, machine->get_sys_mem(socket_id)));
            }
        }
    }

    for (int i = 0; i < num_nodes; i++)
    {
        for (int j = 0; j < num_sockets_per_node; j++)
        {
            for (int k = 0; k < num_gpus_per_socket; k++)
            {
                int socket_id = i * num_sockets_per_node + j;
                int device_id = socket_id * num_gpus_per_socket + k;
                string task_name = "comp_task " + to_string(task_id++) + " on socket " + to_string(socket_id) + " gpu " + to_string(k) + " device_id " + to_string(device_id);
                float run_time = 0;
                comp_tasks.emplace_back(simulator.new_comp_task(task_name, machine->get_gpu(device_id), run_time, machine->get_gpu_fb_mem(device_id)));
            }
        }
    }

    Task *src = comp_tasks[94];
    Task *tar = comp_tasks[0];
    simulator.enter_ready_queue(src);
    for (int i = 0; i < max_peer; i++)
    {
        simulator.new_comm_task(src, tar, message_size);
    }

    simulator.simulate();
}

// multiple nodes communicate with one node
void test_congestion(Simulator &simulator, MachineModel *machine, size_t message_size, int max_peer)
{
    // cout << machine->to_string();
    int num_nodes = machine->get_num_nodes();
    int num_sockets_per_node = machine->get_num_sockets_per_node();
    int num_cpus_per_socket = machine->get_num_cpus_per_socket();
    int num_gpus_per_socket = machine->get_num_gpus_per_socket();
    int task_id = 0;
    vector<Task *> comp_tasks;
    for (int i = 0; i < num_nodes; i++)
    {
        for (int j = 0; j < num_sockets_per_node; j++)
        {
            for (int k = 0; k < num_cpus_per_socket; k++)
            {
                int socket_id = i * num_sockets_per_node + j;
                int device_id = socket_id * num_cpus_per_socket + k;
                string task_name = "comp_task " + to_string(task_id++) + " on socket " + to_string(socket_id) + " cpu " + to_string(k) + " device_id " + to_string(device_id);
                float run_time = 0;
                comp_tasks.emplace_back(simulator.new_comp_task(task_name, machine->get_cpu(device_id), run_time, machine->get_sys_mem(socket_id)));
            }
        }
    }

    for (int i = 0; i < num_nodes; i++)
    {
        for (int j = 0; j < num_sockets_per_node; j++)
        {
            for (int k = 0; k < num_gpus_per_socket; k++)
            {
                int socket_id = i * num_sockets_per_node + j;
                int device_id = socket_id * num_gpus_per_socket + k;
                string task_name = "comp_task " + to_string(task_id++) + " on socket " + to_string(socket_id) + " gpu " + to_string(k) + " device_id " + to_string(device_id);
                float run_time = 0;
                comp_tasks.emplace_back(simulator.new_comp_task(task_name, machine->get_gpu(device_id), run_time, machine->get_gpu_fb_mem(device_id)));
            }
        }
    }
    /**
     * NODE 0: CPU (0-19)  GPU (80-83)
     * NODE 1: CPU (20-39) GPU (84-87)
     * NODE 2: CPU (40-59) GPU (88-91)
     * NODE 3: CPU (60-79) GPU (92-95)
     */
    int num_peers = min(max_peer, num_nodes - 1);
    for (int i = 0; i < num_peers; i++)
    {
        Task *src = comp_tasks[80 + 4 * i];
        Task *tar = comp_tasks[92];
        simulator.enter_ready_queue(src);
        simulator.new_comm_task(src, tar, message_size);
    }

    simulator.simulate();
}

int main(int argc, char **argv)
{
    sleep(10);
    num_bgworks = 1;
    default_seg_size = 4194304;
    max_num_segs = 10;
    realm_comm_overhead = 0.1;

    string log_folder = "";
    size_t message_size = 64 << 20;
    int max_peer = 1;
    int model_version = 0;
    string model_config = "/Users/xluo/Programs/simulator_experiments/summit/machine_config_summit";
    int if_run_dag_file = 0;
    int if_test_comm = 0;
    int if_test_congestion = 0;
    for (int i = 1; i < argc; i++)
    {
        std::string arg = argv[i];
        if (arg == "--num_bgworks" or arg == "-nbg")
        {
            num_bgworks = atoi(argv[++i]);
        }
        if (arg == "--log_folder" or arg == "-f")
        {
            log_folder = argv[++i];
        }
        if (arg == "--message_size" or arg == "-m")
        {
            message_size = atoi(argv[++i]);
        }
        if (arg == "--max_peer" or arg == "-p")
        {
            max_peer = atoi(argv[++i]);
        }
        if (arg == "--model_version" or arg == "-v")
        {
            model_version = atoi(argv[++i]);
        }
        if (arg == "--model_config" or arg == "-c")
        {
            model_config = argv[++i];
        }
        if (arg == "--default_seg_size")
        {
            default_seg_size = atoi(argv[++i]);
        }
        if (arg == "--max_num_segs")
        {
            max_num_segs = atoi(argv[++i]);
        }
        if (arg == "--realm_comm_overhead")
        {
            realm_comm_overhead = atof(argv[++i]);
        }
        if (arg == "--if_run_dag_file" or arg == "-dag")
        {
            if_run_dag_file = atoi(argv[++i]);
        }
        if (arg == "--if_test_comm" or arg == "-comm")
        {
            if_test_comm = atoi(argv[++i]);
        }
        if (arg == "--if_test_congestion" or arg == "-cong")
        {
            if_test_congestion = atoi(argv[++i]);
        }
    }
    cout << "num_bgworks = " << num_bgworks << endl;
    cout << "default_seg_size = " << default_seg_size << endl;
    cout << "max_num_segs = " << max_num_segs << endl;
    cout << "realm_comm_overhead = " << realm_comm_overhead << endl;
    cout << "log_folder = " << log_folder << endl;
    cout << "message_size = " << message_size << endl;
    cout << "max_peer = " << max_peer << endl;
    cout << "model_version = " << model_version << endl;
    cout << "model_config = " << model_config << endl;

    MachineModel *machine = NULL;
    if (model_version == 0)
    {
        machine = create_simple_machine_model();
    }
    else
    {
        machine = create_enhanced_machine_model(model_config);
    }

    Simulator simulator(machine);
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    if (if_run_dag_file)
    {
        run_dag_file(simulator, machine, log_folder);
    }
    if (if_test_comm)
    {
        test_comm(simulator, machine, message_size, max_peer);
    }
    if (if_test_congestion)
    {
        test_congestion(simulator, machine, message_size, max_peer);
    }
    // stencil_1d_cpu();
    std::chrono::steady_clock::time_point stop = std::chrono::steady_clock::now();
    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double> >(stop - start);
    cout << "simulator runs: " << time_span.count() << " seconds" << endl;
}
