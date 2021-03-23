# airbnb

## Data
You can find data collected with [01_scrape.py](https://github.com/gumdropsteve/airbnb/blob/main/01_scrape.py) from Bentonville, London, Tokyo, Riyadh, San Francisco, Seattle, & 42+ other locations here: [gumdropsteve/datasets/airbnb](https://github.com/gumdropsteve/datasets/tree/master/airbnb)

Data can be downloaded or read directly from GitHub like so;
```python
import pandas as pd

pd.read_parquet('https://github.com/gumdropsteve/datasets/raw/master/airbnb/las_vegas.parquet')
```

#### `/wip_notebooks/`
Work in progress [WIP] Jupyter Notebooks from setting up in [`00_building_base_scrape.ipynb`](https://github.com/gumdropsteve/airbnb/blob/main/wip_notebooks/00_building_base_scrape.ipynb) to parallelizing the worklaod with dask.delayed in [`02_building_multi_location_multi_page_base_scrape.ipynb`](https://github.com/gumdropsteve/airbnb/blob/main/wip_notebooks/02_building_multi_location_multi_page_base_scrape.ipynb);  examples going from raw to data we care about can be found in [`/03_cleaning/`](https://github.com/gumdropsteve/airbnb/tree/main/wip_notebooks/03_cleaning).
