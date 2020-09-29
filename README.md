# SCOPE

Provide simple scripts to test SDAP end-points.


# DEPLOY


export PATH=~/opt/anaconda3/bin/:$PATH 
conda create --no-default-packages -n myenv python=3.7
conda install -n myenv pip
conda activate myenv
pip install -r requirements.txt
conda install -c conda-forge cartopy


# USE

1. `simple_test.py`: do concurrent requests on end points, print partial json response
2. `plot_test.py` : plots results for all datasets published on a SDAP server
3. `asynchronous_test` : run 2 requests asynchronously (one bug, one small in this order) and get result of small request first.

The rudimentary scripts needs to be edited to set tested end-point and test parameters



