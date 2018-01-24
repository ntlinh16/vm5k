I***************************
Readme for the VM/Container boot time experiments
***************************

VM/Container boot time experiments
============
The experiments on VM/Container boot time are conducted by using the scripts extended from `vm5k` (see [below](#vm5k)).

The scripts can be found in `engines/` with:

* `VMBootTime.py`: experiments on boot time of VMs
* `ContainerBootTime.py`: experiments on boot time of Containers

Each script takes several command-line parameters as input, you can use:
```./VMBootTime.py -h``` or
```./ContainerBootTime.py -h```
to print the list of parameters needed.

In order to run the script, you need access to Grid5000 servers. The scripts can be modified to be used in other platforms as well. After `git clone` the repository and choose the experiment to perform, you can run the experiment with your custom scenario. For example:

```
./VMBootTime.py --vm 16 -k econome -c econome-test
```

will run the VM boot time experiment with `16` VMs on the cluster named `econome` and the output is saved in folder `econome-test`.

### Analyzing the results

`BootTime2csv.py` is the script to convert the results of the experiments, which can be found in `postprocess/`. To run the script (use argument `-h` to see the list of all other arguments):

```
python boottime2csv.py -i <input_directory> -o results.csv
```

The csv file contains information about the experiments, each row is one run of a experiment with the columns are the scenario parameters and the boot time result. You can do some further analysis on this file using [RStudio](https://www.rstudio.com/), [pandas](https://pandas.pydata.org/), [MS Excel](https://products.office.com/en/excel) or the tool of your choice.


vm5k
============

A python module to ease the experimentations of virtual Machines on the Grid'5000 platform.
It is composed of:

* a script that deploy virtual machines (vm5k)
* an experimental engine that conduct user defined workflow for a set of parameters (vm5k_engine)
* a lib to setup Debian hosts with libvirt and manage virtual machines 

Developed by the Inria Hemera initiative 2010-2014 
https://www.grid5000.fr/mediawiki/index.php/Hemera

See documentation on http://vm5k.readthedocs.org

Requirements
============
The module requires:

* execo 2.4, <http://execo.gforge.inria.fr/>


Installation
============
Connect on a Grid'5000 frontend and type the following commands::

  export http_proxy="http://proxy:3128"
  export https_proxy="https://proxy:3128"
  easy_install --user execo
  easy_install --user vm5k

Add .local/bin to your path and run vm5k !


People
======

Contributors
------------
* Laurent Pouilloux
* Daniel Balouek-Thomert
* Jonathan Rouzaud-Cornabas
* Flavien Quesnel
* Jonathan Pastor
* Takahiro Hirofuchi
* Adrien Lèbre


Grid'5000 technical support
---------------------------
* Matthieu Imbert
* Simon Delamare
