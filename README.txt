1. Download and install Anaconda3 from its official website.
-----------------------------------------------------------------
2. Install the required packages by running:
conda env create -f DaskDB-requirements.yaml
-----------------------------------------------------------------
3. Download the dataset from the following link:
https://unbcloud-my.sharepoint.com/personal/sdas6_unb_ca/_layouts/15/onedrive.aspx?csf=1&web=1&e=Y6jALy&CID=79874ebb%2D719b%2D4bc1%2Dacca%2D7bd7a5f59aaf&id=%2Fpersonal%2Fsdas6%5Funb%5Fca%2FDocuments%2FCode%2FDaskDB%5FDEMO%2Fdata&FolderCTID=0x0120007487B6F06F67254F95D01524A9776B31&view=0
-----------------------------------------------------------------
4. In the first terminal, activate the environment and start the scheduler:
conda activate DaskDB
dask-scheduler
The dask-scheduler starts the centralized coordination service that manages task
distribution.
-----------------------------------------------------------------
5. In a second terminal, connect a worker:
conda activate DaskDB
dask-worker tcp://<scheduler-ip>:8786
Replace <scheduler-ip> with the actual IP from the scheduler output. The dask-
worker executes assigned data tasks.
-----------------------------------------------------------------
6. Finally, in a third terminal, activate the environment and run the desired Python
script:
conda activate DaskDB
python task1.py
You can substitute task1.py with any of the four task scripts to explore different UI
workflows.