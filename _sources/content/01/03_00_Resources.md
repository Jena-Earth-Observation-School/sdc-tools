# Additional Resources

## Python and Jupyter Notebooks 

If you want to get an introduction to [Python](https://www.python.org/) and/or 
[Jupyter](https://jupyter.org/) Notebooks, I recommend the following resources from 
Project Pythia:
- [Quickstart: Zero to Python](https://foundations.projectpythia.org/foundations/quickstart.html)
- [Getting Started with Jupyter](https://foundations.projectpythia.org/foundations/getting-started-jupyter.html)

[Project Pythia Foundations](https://foundations.projectpythia.org/landing-page.html) 
also provides tutorials on various core scientific Python packages, such as NumPy, 
Matplotlib and Pandas, which you will likely encounter at some point.

(xarray-dask-intro)=
## Xarray, Dask and lazy loading

The `load_product`-function returns an `xarray.Dataset` object, which is a 
powerful data structure for working with multidimensional data. [Xarray](https://xarray.dev/) 
is a Python library that _"[...] introduces labels in the form of dimensions, 
coordinates and attributes on top of raw NumPy-like arrays, which allows for more 
intuitive, more concise, and less error-prone user experience."_. 

See the following resources for more information:
- [Overview: Why Xarray?](https://docs.xarray.dev/en/latest/getting-started-guide/why-xarray.html)
- [Tutorial: Xarray in 45 minutes](https://tutorial.xarray.dev/overview/xarray-in-45-min.html)
- [Xarray Documentation](https://docs.xarray.dev/en/latest/index.html) (Very important resource! 😉)

Xarray closely integrates with the [Dask](https://dask.org/) library, which is a 
_"[...] flexible library for parallel computing in Python."_ and allows for 
datasets to be loaded lazily, meaning that the data is not loaded into memory 
until it is actually needed. This is especially useful when working with large 
datasets that might not fit into the available memory. These large datasets are split 
into smaller chunks that can then be efficiently processed in parallel. 

Most of this is happening in the background, so you don't have to worry too much about 
it. However, it is important to be aware of it, as it affects the way you need to 
work with the data. For example, you need to be careful when applying certain 
Xarray operations, such as calling [`.values`](https://docs.xarray.dev/en/latest/generated/xarray.DataArray.values.html#xarray.DataArray.values), 
as they might trigger the entire dataset to be loaded into memory and can result in 
performance issues if the data has not been [aggregated](https://docs.xarray.dev/en/latest/api.html#aggregation) 
or [indexed](https://docs.xarray.dev/en/latest/user-guide/indexing.html) beforehand. 
Furthermore, you might reach a point where you need to use advanced techniques 
to optimize your workflow, such as re-orienting the chunks or [persisting](https://docs.dask.org/en/latest/best-practices.html#persist-when-you-can) 
intermediate results in memory. For now, just keep all of this in mind and reach 
out to me if you have any questions or need help with optimizing your workflow. 

The following resources provide more information:
- [User Guide: Using Dask with xarray](https://docs.xarray.dev/en/latest/user-guide/dask.html#using-dask-with-xarray)
- [Tutorial: Parallel computing with Dask](https://tutorial.xarray.dev/intermediate/xarray_and_dask.html#parallel-computing-with-dask)

## Digital Earth Africa

### Tutorials

The two main data products of the SDC, Sentinel-1 RTC and Sentinel-2 L2A, are direct 
copies of the open and free "Analysis Ready Data" products provided by [Digital Earth Africa (DE Africa)](https://www.digitalearthafrica.org/).

The team of DE Africa provides a lot of very helpful tutorials as Jupyter Notebooks. 
Some of these tutorials cover more advanced and analysis-specific topics to address 
real-world problems. While the loading of the data differs between these tutorials and 
the SDC, most of the analysis techniques can be directly applied to the SDC data 
products as well. It is therefore highly recommended to have a look at the tutorials in 
the course of your work with the SDC data products: 
- [DE Africa Real World Examples](https://docs.digitalearthafrica.org/en/latest/sandbox/notebooks/Real_world_examples/index.html)

### `deafrica-tools` package

Some of these tutorials are using a package called `deafrica-tools`, which includes 
useful functions and utilities, e.g. for the calculation of [vegetation phenology statistics](https://docs.digitalearthafrica.org/en/latest/sandbox/notebooks/Real_world_examples/Phenology_optical.html). You can find the package on GitHub:
- [Digital Earth Africa Tools Package](https://github.com/digitalearthafrica/deafrica-sandbox-notebooks/tree/main/Tools)

If you want to use any functions of `deafrica-tools` and need assistance with the 
installation or usage of the package, please let me know!
