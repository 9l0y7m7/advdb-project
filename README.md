# advdb-project
advance database project
## Environment

python3
install vagrant and repozip


## Test on local machine
cd into project root dir
```
python3 main.py -f <test_file>

example:
python3 main.py -f test/test22
```

We make our own expected correct output in /res folder
if you want to check whether the output of current project align with result in /res folder
please run
```
bash test.sh
```

## vagrant up and use repozip
In local machine
```


vagrant up
vagrant ssh
cd /vagrant


## test in different environment
python3 main.py -f test/test22

## trace experiment with reprozip
reprozip trace python3 main.py -f test/test22
reprozip trace -continue python3 main.py -f <test file>

## pack the experiment
reprozip pack advdbproject

## shut down
vagrant halt

## delete
vagrant destroy


```


## reproduce with repozip.rpz

```
## scp rpz into access(cims)
scp advdbproject.rpz user@access.cims.nyu.edu:~

## import and load packages
module load python-2.7
python -c "import reprozip"


## unzip rpz project
pip install reprounzip

reprounzip directory setup advdbproject.rpz ~/advDB

```