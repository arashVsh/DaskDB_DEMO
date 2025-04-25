1. Download and install Anaconda3 from its official website.
-----------------------------------------------------------------
2. Install the required packages by running:
conda env create -f DaskDB-requirements.yaml
-----------------------------------------------------------------
3. In the first terminal, activate the environment and start the scheduler:
conda activate DaskDB
dask-scheduler
The dask-scheduler starts the centralized coordination service that manages task
distribution.
-----------------------------------------------------------------
4. In a second terminal, connect a worker:
conda activate DaskDB
dask-worker tcp://<scheduler-ip>:8786
Replace <scheduler-ip> with the actual IP from the scheduler output. The dask-
worker executes assigned data tasks.
-----------------------------------------------------------------
5. Finally, in a third terminal, activate the environment and run the desired Python
script:
conda activate DaskDB
python task1.py
You can substitute task1.py with any of the four task scripts to explore different UI
workflows.