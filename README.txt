1. Download Anaconda3 and install it
----------------------------------------
wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh (A recent version of Anaconda could also be used but I haven't tried that)
bash  Anaconda3-2020.02-Linux-x86_64.sh


2. Install the necessary packages required for DaskDB
----------------------------------------------------------
conda env create -f DaskDB-environment.yaml


3. Activate DaskDB environment and install sql-metadata
----------------------------------------------------------
conda activate DaskDB
pip install sql-metadata==1.7.1

