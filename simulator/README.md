# A simulator for legion
1. Run Circuit
   ```
   cd /scratch2/xluo/program/legion/examples/circuit
   mpirun -np 4 --bind-to none -H g0001,g0002,g0003,g0004 -x LD_LIBRARY_PATH=/scratch2/xluo/opt/regent_gpu/llvm/install/lib:/scratch2/xluo/opt/regent_gpu/hdf/install/lib:/home/xluo/program/cudnn/lib64:/usr/local/cuda/lib:/usr/local/openmpi-4.1.0/lib -x CPATH=/scratch2/xluo/opt/regent_gpu/llvm/install/include:/scratch2/xluo/opt/regent_gpu/hdf/install/include:/usr/local/cuda/include /scratch2/xluo/program/legion/examples/circuit/circuit -p 64 -l 10 -npp 128 -wpp 256 -ll:cpu 16 -ll:gpu 4 -ll:util 2 -ll:bgwork 2 -ll:fsize 1000 -ll:csize 1000 -ll:zsize 1000 -ll:rsize 1000 -lg:prof 4 -lg:spy -logfile ./spy_%.log -lg:prof_logfile ./prof_%.gz
    ```

2. create the logs with Leigon prof and spy
   ```
   /scratch2/xluo/program/legion/tools/legion_prof.py /scratch2/xluo/program/legion/examples/circuit/prof_*.gz
   /scratch2/xluo/program/legion/tools/legion_spy.py -eztmrxvu /scratch2/xluo/program/legion/examples/circuit/spy_*.log
   ```

3. Compile and build simulator
   ```
   cd /scratch2/xluo/program/legion/simulator
   mkdir build
   cd build
   cmake ..
   make
   ```

4. Run the simulator with the generated logs. To get accurate estimations, the model config file and the init_id_maps need to be updated based on the acutal machine configurations.
   ```
   // with old machine model
   /scratch2/xluo/program/legion/simulator/build/main --if_run_dag_file 1 --num_bgworks 2 --log_folder /scratch2/xluo/program/legion/examples/circuit/ --model_version 0

   // with new machine model
   /scratch2/xluo/program/legion/simulator/build/main --if_run_dag_file 1 --num_bgworks 2 --log_folder /scratch2/xluo/program/legion/examples/circuit/ --model_version 1 --model_config /scratch2/xluo/program/legion/simulator/machine_config_sapling
   ```