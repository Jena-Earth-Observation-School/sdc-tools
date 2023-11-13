# Installation

Provided that a Conda-based package manager (e.g. 
[Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html))
is installed on your system, the most up-to-date version of the `sdc-tools` package can 
be installed using the following steps:

## Create and activate environment

```bash
micromamba create --file https://raw.githubusercontent.com/Jena-Earth-Observation-School/sdc-tools/main/environment.yml
micromamba activate sdc_env
```

If you are not able to install the environment directly from the GitHub repository 
(e.g., due to firewall restrictions on HPC systems), you can also download the 
`environment.yml` file and install the environment from it locally.

## Install `sdc-tools` package

```bash
pip install git+https://github.com/Jena-Earth-Observation-School/sdc-tools.git
```

### _Optional_: Installation of a specific version

If you wish to install a specific version of the package, you can do so by specifying 
the version tag. It is important to specify the same version tag for both the 
environment and the package installation.

```bash
micromamba create --file https://raw.githubusercontent.com/Jena-Earth-Observation-School/sdc-tools/v0.2.0/environment.yml
micromamba activate sdc_env
pip install git+https://github.com/Jena-Earth-Observation-School/sdc-tools.git@v0.2.0
```

See the [releases page](https://github.com/Jena-Earth-Observation-School/sdc-tools/releases) 
for a list of available versions.
