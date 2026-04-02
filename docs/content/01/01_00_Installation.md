# Installation

Provided that a Conda-based package manager (e.g. 
[Micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html))
is installed on your system, the most up-to-date version of the `sdc-tools` 
package can be installed using the following steps:

## Create and activate environment

```bash
micromamba create --file https://raw.githubusercontent.com/Jena-Earth-Observation-School/sdc-tools/main/environment.yml
micromamba activate sdc_env
```

If you are not able to install the environment directly from the GitHub 
repository (e.g., due to firewall restrictions on HPC systems), you can also 
download the `environment.yml` file and install the environment from it locally:

```bash
wget https://raw.githubusercontent.com/Jena-Earth-Observation-School/sdc-tools/main/environment.yml
micromamba create --file environment.yml
micromamba activate sdc_env
```

## Install `sdc-tools` package

```bash
pip install git+https://github.com/Jena-Earth-Observation-School/sdc-tools.git
```

### _Optional_: Installation of a specific version

If you wish to install a specific version of the package, you can do so by 
specifying the version tag. It is important to specify the same version tag for 
both the environment and the package installation.

```bash
micromamba create --file https://raw.githubusercontent.com/Jena-Earth-Observation-School/sdc-tools/v0.9.0/environment.yml
micromamba activate sdc_env
pip install git+https://github.com/Jena-Earth-Observation-School/sdc-tools.git@v0.9.0
```

See the [releases page](https://github.com/Jena-Earth-Observation-School/sdc-tools/releases) 
for a list of available versions.

### _Optional_: Use Pixi instead of Conda/(Micro)mamba

If you want to use [Pixi](https://pixi.sh) as your package manager, you can follow
these steps: 

```bash
wget https://raw.githubusercontent.com/Jena-Earth-Observation-School/sdc-tools/main/environment.yml
pixi init --import environment.yml
pixi install
pixi add --pypi 'sdc @ git+https://github.com/Jena-Earth-Observation-School/sdc-tools.git'
```

If the last installation step fails with _"Error: failed to map conda packages to their PyPI equivalents. [...]"_,
the following steps hopefully resolve this issue (see also https://github.com/prefix-dev/pixi/issues/474): 

```bash
wget https://raw.githubusercontent.com/prefix-dev/parselmouth/refs/heads/main/files/compressed_mapping.json
sed -i '/^version = /a conda-pypi-map = { conda-forge = "compressed_mapping.json" }' pixi.toml
pixi add --pypi 'sdc @ git+https://github.com/Jena-Earth-Observation-School/sdc-tools.git'
```
