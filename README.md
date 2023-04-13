# alphabot

An automated trading system based on backtrader and vnpy, support joinquant/zipline style strategy, and is planned to support reinforcement learning.

## install

```bash
    conda create -n alphabot python=3.8
    conda activate alphabot

    pip install setuptools==57.5.0
    pip install -r requirements.txt

    # install alphabot package
    make install
```

## run

```bash
    cd ./examples

    # fetch data
    ./run_data_fetch.sh

    # backtesing
    ./run_cli.sh
```

## test

```bash
    pip install nose
    pip install git+https://github.com/fs714/ialgotest.git

    make test
```

