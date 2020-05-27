# tiden_gridgain_examples

New home for Tiden Gridgain test suites and examples.

## Install pre-requisites

Install `tiden` and `tiden_gridgain` packages:

```bash
    sudo -H pip3.7 install --index-url https://test.pypi.org/simple tiden tiden_gridgain
```

Alternatively, you can install those packages from source:
    git clone git@github.com:mshonichev/tiden_pkg.git
    cd tiden_pkg
    bash ./build.sh
    bash ./install.sh
    cd ..
    git clone git@github.com:mshonichev/tiden_gridgain_pkg.git
    cd tiden_gridgain_pkg
    bash ./build.sh
    bash ./install.sh
```

## Checkout examples

To clone example git repository you must have Git Lfs installed.

https://git-lfs.github.com/

```bash
    git clone git@github.com:mshonichev/tiden_gridgain_examples.git
    cd tiden_gridgain_examples
```

## Run examples
This is old-fashioned examples test suite with non-Ignite apps.

```bash
    bash ./run_hazelcast.sh
    bash ./run_mysql.sh
```