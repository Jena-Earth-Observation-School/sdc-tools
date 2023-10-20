---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.15.2
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Data Access

```{code-cell} ipython3
from sdc.s1 import load_s1_rtc

vec = "../../_assets/test.geojson"
ds = load_s1_rtc(vec=vec, time_range=("2020-01-01", "2021-01-01"))
ds.vh
```
