# Python Environment

## Conda environment

The provided conda environment at `env.yml` can be built and activated locally if you have [mamba](https://mamba.readthedocs.io/en/latest/) or [conda](https://docs.conda.io/en/latest/) installed (in replace `mamba` by `conda` in that case), by running the following from the project root:
```
mamba env create --file ./env/env.yml
conda activate context_tracer_env
```

And can be cleaned up afterwards with:
```
conda deactivate && conda remove --yes --name context_tracer_env --all
```

More info how to work with conda/mamba:
* [Conda Cheatsheet](https://docs.conda.io/projects/conda/en/latest/_downloads/843d9e0198f2a193a3484886fa28163c/conda-cheatsheet.pdf)


### Local package

The local package at `./src/context_tracer` can be installed with `pip install --editable .`, this makes all the packages importable and editable while preventing any issues with mis-specified PythonPath variables.


## Pre-commit hooks
[Pre-commit](https://pre-commit.com/) [hooks](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) can be setup using:
```
conda run -n fin_agent pre-commit install
```
