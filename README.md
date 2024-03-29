[![Travis](https://travis-ci.org/NextGeoss/ckanext-nextgeossharvest.svg?branch=master)](https://travis-ci.org/NextGeoss/ckanext-nextgeossharvest)

[![Coveralls](https://coveralls.io/repos/NextGeoss/ckanext-nextgeossharvest/badge.svg)](https://coveralls.io/r/NextGeoss/ckanext-nextgeossharvest)

# ckanext-nextgeossharvest
This extension contains harvester plugins for harvesting from sources used by NextGEOSS as well as a metaharvester plugin for adding additional tags and metadata retrieved from an iTag instance.

## Contents
1. [What's in the repository](#repo)
2. [Basic usage](#usage)
3. [Harvesting Sentinel products](#harvesting)
    1. [Harvesting from SciHub](#scihub)
    2. [Harvesting from NOA](#noa)
    3. [Harvesting from CODE-DE](#code-de)
    4. [Sentinel settings (SciHub, NOA & CODE-DE)](#generalsettings)
    5. [Harvesting from more than one Sentinel source](#multi)
    6. [How the three Sentinel harvesters work together](#alltogether)
4. [Harvesting CMEMS products](#harvesting-cmems)
    1. [CMEMS Settings](#cmems-settings)
    2. [Running a CMEMS harvester](#running-cmems)
5. [Harvesting GOME-2 products](#harvesting-gome2)
    1. [GOME-2 Settings](#gome2-settings)
    2. [Running a GOME-2 harvester](#running-gome2)
6. [Harvesting PROBA-V products](#harvesting-proba-v)
    1. [PROBA-V Settings](#proba-v-settings)
    2. [Running a PROBA-V harvester](#running-proba-v)
7. [Harvesting Static EBVs](#harvesting-static-ebvs)
    1. [Static EBVs Settings](#static-ebvs-settings)
    2. [Running a static EBVs harvester](#running-static-ebvs)
8. [Harvesting GLASS LAI products](#harvesting-glass-lai)
    1. [GLASS LAI Settings](#glass-lai-settings)
    2. [Running a GLASS LAI harvester](#running-glass-lai)
9. [Harvesting Plan4All products](#harvesting-plan4all)
    1. [Plan4All Settings](#plan4all-settings)
    2. [Running a Plan4All harvester](#running-plan4all)
10. [Harvesting DEIMOS-2 products](#harvesting-deimos2)
    1. [DEIMOS-2 Settings](#deimos2-settings)
    2. [Running a DEIMOS-2 harvester](#running-deimos2)
11. [Harvesting EBAS-NILU products](#harvesting-ebasnilu)
    1. [EBAS-NILU Settings](#ebasnilu-settings)
    2. [Running a EBAS-NILU harvester](#running-ebasnilu)
12. [Harvesting SIMOcean products](#harvesting-simocean)
    1. [SIMOcean Settings](#simocean-settings)
    2. [Running a SIMOcean harvester](#running-simocean)
13. [Harvesting EPOS-Sat products](#harvesting-epos)
    1. [EPOS-Sat Settings](#epos-settings)
    2. [Running a EPOS-Sat harvester](#running-epos)
14. [Harvesting MODIS products](#harvesting-modis)
    1. [MODIS Settings](#modis-settings)
    2. [Running a MODIS harvester](#running-modis)
15. [Harvesting GDACS Average Flood products](#harvesting-gdacs)
    1. [GDACS Settings](#gdacs-settings)
    2. [Running a GDACS harvester](#running-gdacs)
16. [Harvesting DEIMOS-2 products](#harvesting-deimos2)
    1. [DEIMOS-2 Settings](#deimos2-settings)
    2. [Running a DEIMOS-2 harvester](#running-deimos2)
17. [Harvesting Food Security pilot outputs](#harvesting-foodsecurity)
    1. [Food Security Settings](#foodsecurity-settings)
    2. [Running a Food Security harvester](#running-foodsecurity)
18. [Harvesting SAEON products](#harvesting-saeon)
    1. [SAEON Settings](#saeon-settings)
    2. [Running a SAEON harvester](#running-saeon)
19. [Harvesting Landsat-8 outputs](#harvesting-landsat8)
    1. [Landsat-8 Settings](#flandsat8-settings)
    2. [Running a Landsat-8 harvester](#running-landsat8)
20. [Harvesting VITO CGS S1 products](#harvesting-vitocgss1)
    1. [VITO CGS S1 Settings](#vitocgss1-settings)
    2. [Running a VITO CGS S1 harvester](#running-vitocgss1)
21. [Harvesting Cold Regions pilot outputs](#harvesting-coldregions)
    1. [Running a Cold Regions harvester](#running-coldregions)
22. [Harvesting NOA Groundsegment products](#harvesting-noa_gs)
    1. [NOA GS Settings](#noa_gs-settings)
    2. [Running a NOA GS harvester](#running-noa_gs)
23. [Harvesting MELOA products](#harvesting-meloa)
    1. [MELOA Settings](#meloa-settings)
    2. [Running a MELOA harvester](#running-meloa)
24. [Harvesting FSSCAT products](#harvesting-fsscat)
    1. [FSSCAT Settings](#fsscat-settings)
    2. [Running a FSSCAT harvester](#running-fsscat)
25. [Harvesting NOA GeObservatory products](#harvesting-noa_geob)
    1. [NOA GeObservatory Settings](#noa_geob-settings)
    2. [Running a NOA GeObservatory harvester](#running-noa_geob)
25. [Harvesting Energy Data products](#harvesting-energydata)
    1. [Energy Data Settings](#energydata-settings)
    2. [Running an Energy Data harvester](#running-energydata)
26. [Developing new harvesters](#develop)
    1. [The basic harvester workflow](#basicworkflow)
        1. [gather_stage](#gather_stage)
        2. [fetch_stage](#fetch_stage)
        3. [import_stage](#import_stage)
    2. [Example of an OpenSearch-based harvester](#opensearchexample)
27. [iTag](#itag)
    1. [How ITagEnricher works](#itagprocess)
    2. [Setting up ITagEnricher](#setupitag)
    3. [Handling iTag errors](#handlingitagerrors)
28. [Testing testing testing](#tests)
29. [Suggested cron jobs](#cron)
30. [Logs](#logs)
    1. [How ITagEnricher works](#itagprocess)
    2. [Setting up ITagEnricher](#setupitag)
    3. [Handling iTag errors](#handlingitagerrors)

## <a name="repo"></a>What's in the repository
The repository contains four plugins:
1. `nextgeossharvest`, the base CKAN plugin
2. `esa`, a harvester plugin for harvesting Sentinel products from SciHub, NOA, and CODE-DE via their DHuS interfaces
3. `cmems`, a harvester plugin for harvesting the following types of CMEMS products:
    1. Arctic Ocean Physics Analysis and Forecast (OCN)
    2. Global Observed Sea Surface Temperature (SST)
    3. Antarctic Ocean Observed Sea Ice Concentration (SIC South)
    4. Arctic Ocean Observed Sea Ice Concentration (SIC North)
4. `gome2`, a harvester plugin for harvesting the following types of GOME-2 coverage products:
    1. GOME2_O3
    2. GOME2_NO2
    3. GOME2_TropNO2
    4. GOME2_SO2
    5. GOME2_SO2mass
5. `itag` a harvester plugin for adding additional tags and metadata to datasets that have already been harvested (more on this later)

## <a name="usage"></a>Basic usage
1. Run `python setup.py develop` in the `ckanext-nextgeossharvest` directory.
2. Run `pip install -r requirements.txt` n the `ckanext-nextgeossharvest` directory.
3. You will also need the following CKAN extensions:
    1. `ckanext-harvest`
    2. `ckanext-spatial`
1. You will want to configure `ckanext-spatial` to use `solr-spatial-field` for the spatial search backend. Instructions can be found here: http://docs.ckan.org/projects/ckanext-spatial/en/latest/spatial-search.html. You cannot use `solr` as the spatial search backend because `solr` only supports  footprints that are effectively bounding boxes (polygons composed of five points), while the footprints of the datasets harvested by these plugins can be considerably more complex. Using `postgis` as the spatial search backend is strongly discouraged, as it will choke on the large numbers of datasets that these harvesters will pull down.
2. Add the harvester and spatial plugins to the list of plugins in your `.ini` file, as well as `nextgeossharvest` and any of the NextGEOSS harvester plugins that you want to use.
3. If you will be harvesting from SciHub, NOA or CODE-DE, add your username and password to `ckanext.nextgeossharvest.nextgeoss_username=` and `ckanext.nextgeossharvest.nextgeoss_password=` in your `.ini` file. The credentials are stored here rather than in the source config partly for security reasons and partly because of the way the extension is deployed. (It may make sense to move them to the source config in the future.)
4. If you want to log the response times and status codes of requests to harvest sources, you must include `ckanext.nextgeossharvest.provider_log_dir=/path/to/your/logs` in your `.ini` file. The log entries will look like this: `INFO | esa_scihub   | 2018-03-08 14:17:04.474262 | 200 | 2.885231s` (the second field will always be 12 characters and will be padded if necessary).
4. Create a cron job like the following so that your harvest jobs will be marked `Finished` when complete:
 `0 * * * * paster --plugin=ckanext-harvest harvester run -c /srv/app/production.ini >> /var/log/cron.log 2>&1`

## <a name="harvesting"></a>Harvesting Sentinel products
To harvest Sentinel products, activate the `esa` plugin, which you will use to create a harvester that harvests from SciHub, NOA or CODE-DE. To harvest from more than one of those sources, just create more than one harvester and point it at a different source.

Note: The [configuration object](#generalsettings) is _required_ for all of these harvesters.

### <a name="scihub"></a>Harvesting from SciHub
Create a new harvest source and select `ESA Sentinel Harvester New`. The URL does not matter—the harvester only harvests from SciHub, NOA, or CODE-DE, depending on the configuration below.

To harvest from SciHub, `source` must be set to `"esa_scihub"` in the configuration. See [Sentinel settings (SciHub, NOA & CODE-DE)](#generalsettings) for a complete description of the settings.

Note: you must place your username and password in the `.ini` file as described above.

After saving the configuration, you can click Reharvest and the job will begin (assuming you have a cronjob like the one described above). Alternatively, you can use the paster command `run_test` described in the `ckanext-harvest` documentation to run the harvester without setting up the the gather consumer, etc.

### <a name="noa"></a>Harvesting from NOA
Create a new harvest source and select `ESA Sentinel Harvester New`. The URL does not matter—the harvester only harvests from SciHub, NOA, or CODE-DE, depending on the configuration below.

To harvest from NOA, `source` must be set to `"esa_noa"` in the configuration. See [Sentinel settings (SciHub, NOA & CODE-DE)](#generalsettings) for a complete description of the settings.

Note: you must place your username and password in the `.ini` file as described above.

After saving the configuration, you can click Reharvest and the job will begin (assuming you have a cronjob like the one described above). Alternatively, you can use the paster command `run_test` described in the `ckanext-harvest` documentation to run the harvester without setting up the the gather consumer, etc.

### <a name="code-de"></a>Harvesting from CODE-DE
Create a new harvest source and select `ESA Sentinel Harvester New`. The URL does not matter—the harvester only harvests from SciHub, NOA, or CODE-DE, depending on the configuration below.

To harvest from NOA, `source` must be set to `"esa_code"` in the configuration. See [Sentinel settings (SciHub, NOA & CODE-DE)](#generalsettings) for a complete description of the settings.

Note: you must place your username and password in the `.ini` file as described above.

After saving the configuration, you can click Reharvest and the job will begin (assuming you have a cronjob like the one described above). Alternatively, you can use the paster command `run_test` described in the `ckanext-harvest` documentation to run the harvester without setting up the the gather consumer, etc.

### <a name="generalsettings"></a>Sentinel settings (SciHub, NOA and CODE-DE)
1. `source`: **(required, string)** determines whether the harvester harvests from SciHub, NOA, or CODE-DE. To harvest from SciHub, use `"source": "esa_scihub"`. To harvest from NOA, use `"source": "esa_noa"`. To harvest from CODE-DE, use `"source": "esa_code"`.
2. `update_all`: (optional, boolean, default is `false`) determines whether or not the harvester updates datasets that already have metadadata from _this_ source. For example: if we have `"update_all": true`, and dataset Foo has already been created or updated by harvesting from SciHub, then it will be updated again when the harvester runs. If we have `"update_all": false` and Foo has already been created or updated by harvesting from SciHub, then the dataset will _not_ be updated when the harvester runs. And regardless of whether `update_all` is `true` or `false`, if a dataset has _not_ been created or updated with metadata from SciHub (it's new, or it was created via NOA or CODE-DE and has no SciHub metadata), then it will be updated with the additional SciHub metadata.
3. `start_date`: (optional, datetime string, default is "any" or "from the earliest date onwards" if the harvester is new, or from the ingestion date of the most recently harvested product if it has been run before) determines the end of the date range for harvester queries. Example: "start_date": "2018-01-16T10:30:00.000Z". Note that the entire datetime string is required. `2018-01-01` is not valid. Using full datetimes is especially useful when testing, as it is possible to restrict the number of possible results by searching only within a small time span, like 20 minutes. 
4. `end_date`: (optional, datetime string, default is "now" or "to the latest possible date") determines the end of the date range for harvester queries. Example: "end_date": "2018-01-16T11:00:00.000Z". Note that the entire datetime string is required. `2018-01-01` is not valid. Using full datetimes is especially useful when testing, as it is possible to restrict the number of possible results by searching only within a small time span, like 20 minutes.
5. `product_type`: (optional, string) determines the Sentinel collection (product type) to be considered by the harvester when querying the data provider interface. The possible values are `SLC`, `GRD`, `OCN`, `S2MSI1C`, `S2MSI2A`, `S2MSI2Ap`, `OL_1_EFR___`, `OL_1_ERR___`, `OL_2_LFR___`, `OL_2_LRR___`, `SR_1_SRA___`, `SR_1_SRA_A_`, `SR_1_SRA_BS`, `SR_2_LAN___`, `SL_1_RBT___`, `SL_2_LST___`, `SY_2_SYN___`, `SY_2_V10___`, `SY_2_VG1___` or `SY_2_VGP___`. If no product_type is provided, the harvester will have the normal behavior and consider all.
6. `aoi`: (optional, string with POLYGON) determines the Area of Interest to be considered by the harvester when querying the data provider interface. The aoi shall be provided with the following format: `POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))`. More points can be added to the polygon. If no aoi is provided, the harvester will consider as global.
7. `datasets_per_job`: (optional, integer, defaults to 1000) determines the maximum number of products that will be harvested during each job. If a query returns 2,501 results, only the first 1000 will be harvested if you're using the default. This is useful for running the harvester via recurring jobs intended to harvest products incrementally (i.e., you want to start from the beginning and harvest all available products). The harvester will harvest products in groups of 1000, rather than attmepting to harvest all x-hundred-thousand at once. You'll get feedback after each job, so you'll know if there are errors without waiting for the whole job to run. And the harvester will automatically resume from the harvested dataset if you're running it via a recurring cron job.
8. `timeout`: (optional, integer, defaults to 4) determines the number of seconds to wait before timing out a request.
9. `skip_raw`: (optional, boolean, defaults to false) determines whether RAW products are skipped or included in the harvest.
10. `make_private` is optional and defaults to `false`. If `true`, the datasets created by the harvester will be marked private. This setting is not retroactive. It only applies to datasets created by the harvester while the setting is `true`.

Example configuration with all variables present:
```
{
  "source": "esa_scihub",
  "update_all": false,
  "start_date": "2018-01-16T10:30:00.000Z",
  "end_date": "2018-01-16T11:00:00.000Z",
  "datasets_per_job": 100,
  "timeout": 4,
  "skip_raw": true,
  "make_private": false
}
```
```
{
  "source": "esa_scihub",
  "update_all": false,
  "start_date": "2019-01-01T00:00:00.000Z",
  "aoi": "POLYGON((2.0524444097380456 51.60572085265915,5.184653052425238 51.67771256185287,7.138937077349725 50.43826001622307,5.612989277066222 49.25292867929642,1.9721313676178616 50.83443942461676,2.0524444097380456 51.60572085265915,2.0524444097380456 51.60572085265915))",
  "product_type": "S2MSI2A",
  "datasets_per_job": 100,
  "timeout": 20,
  "skip_raw": true,
  "make_private": false
}
```

Note: you must place your username and password in the `.ini` file as described above.

### <a name="multi"></a>Harvesting from more than one Sentinel source
To harvest from more than one Sentinel source, just create a harvester source for each Sentinel source.For example, to harvest from all three sources:
1. Create a harvest source called (just a suggestion) "SciHub Harvester", select `ESA Sentinel Harvester New` and make sure that the configuration contains `"source": "esa_scihub"`.
2. Create a harvest source called (just a suggestion) "NOA Harvester", select `ESA Sentinel Harvester New` and make sure that the configuration contains `"source": "esa_noa"`.
3. Create a harvest source called (just a suggestion) "CODE-DE Harvester", select `ESA Sentinel Harvester New` and make sure that the configuration contains `"source": "esa_code"`.

You'll probably want to specify start and end times as well as the number of datasets per job for each harvester. If you don't, don't worry—the default number of datasets per job is 1000, so you won't be flooded with datasets.

Then just run each of the harvesters. You can run them all at the same time. If a product has already been harvested by another harvester, then the other harvesters will only update the existing dataset and add additional resources and metadata. They will not overwrite the resources and metadata that already exist (e.g., the SciHub harvester won't replace resources from CODE-DE with resources from SciHub, it will just add SciHub resources to the dataset alongside the existing CODE-DE resources.

## <a name="alltogether"></a>How the three Sentinel harvesters work together
The three (really, two) Sentinel harvesters all inherit from the same base harvester classes. As mentioned above, the SciHub, NOA and CODE-DE "harvesters" are all the same harvester with different configurations. The `"source"` configuration is a switch that 1) causes the harvester to use a different base URL for querying the OpenSearch service and 2) changes the labels added to the resources. In all cases, the same methods are used for creating/updating the datasets.

The workflow for all the harvesters is:
1. Gather: query the OpenSearch service for all products within the specified date range, then page through the results, creating harvest objects for each entry. Each harvest object contains the content of the entry, which will be parsed later, as well as a preliminary test of whether the product already exists in CKAN or not.
2. Fetch: just returns true, as the OpenSearch service already provides all the content in the gather stage.
3. Import: parse the content of each harvest object and then either create a new dataset for the respective product, or update an existing dataset. It's possible that another harvester may have created a dataset for the product before the current import phase began, so if creating a dataset for a "new" product fails because the dataset already exists, the harvester catches the exception and performs an update instead. For the sake of simplicity, the create and update pipelines are the same. The only difference is the API call at the end. All three harvesters can run at the same time, harvesting from the same date range, without conflicts.

#### A note on datasets counts
The created/updated counts for each harvester job will be accurate. The count that appears in the sidebar on each harvester's page, however, will not be accurate. Besides issues with how Solr updates the `harvest_source_id` associated with each dataset, the fact that up to three harvesters may be creating or updating a single dataset means that only one harvest source can "own" a dataset at any given time. If you need to evaluate the performance of a harvester, use the job reports.

## <a name="harvesting-cmems"></a>Harvesting CMEMS products
To harvest CMEMS products, activate the `cmems` plugin, which you will use to create a harvester that harvests one of the following types of CMEMS product:
1. Global Observed Sea Surface Temperature (sst) from ftp://nrt.cmems-du.eu/Core/SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2/
2. Arctic Ocean Observed Sea Ice Concentration (sic_north) from ftp://mftp.cmems.met.no/Core/SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS/
3. Antarctic Ocean Observed Sea Ice Concentration (sic_south) from ftp://mftp.cmems.met.no/Core/SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS/
4. Arctic Ocean Physics Analysis and Forecast (ocn) from ftp://mftp.cmems.met.no/Core/ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a/dataset-topaz4-arc-myoceanv2-be/
5. Global Ocean Gridded L4 Sea Surface Heights and Derived Variables NRT (slv) from ftp://nrt.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/dataset-duacs-nrt-global-merged-allsat-phy-l4
6. Global Ocean Physics Analysis and Forecast - Hourly (gpaf) from ftp://nrt.cmems-du.eu/Core/GLOBAL_ANALYSIS_FORECAST_PHY_001_024/global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh
7. Global Total Surface and 15m Current - Hourly (mog) from ftp://nrt.cmems-du.eu/Core/MULTIOBS_GLO_PHY_NRT_015_003/dataset-uv-nrt-hourly

To harvest more than one of those types of product, just create more than one harvester and configure a different `harvester_type`.

The URL you enter in the harvester GUI does not matter--the plugin determines the correct URL based on the `harvester_type`.

The different products are hosted on different services, so separate harvesters are necessary for ensuring that the harvesting of one is not affected by errors or outages on the others.

### <a name="cmems-settings"></a>CMEMS Settings
`harvester_type` determines which type of product will be harvested. It must be one of the following seven strings: `sst`, `sic_north`, `sic_south`, `ocn`, `gpaf`, `slv` or `mog`.

`start_date` determines the start date for the harvester job. It must be the string `YESTERDAY` or a string describing a date in the format `YYYY-MM-DD`, like `2017-01-01`.

`end_date` determines the end date for the harvester job. It must be the string `TODAY` or a string describing a date in the format `YYYY-MM-DD`, like `2017-01-01`. The end_date is not mandatory and if not included the harvester will run until catch up the current day.

The harvester will harvest all the products available on the start date and on every date up to but not including the end date. If the start and end dates are `YESTERDAY` and `TODAY`, respectively, then the harvester will harvest all the products available yesterday but not any of the products available today. If the start and end dates are `2018-01-01` and `2018-02-01`, respectively, then the harvester will harvest all the products available in the month of January (and none from the month of February).

`timeout` determines how long the harvester will wait for a response from a server before cancelling the attempt. It must be a postive integer. Not mandatory.

`username` and `password` are your username and password for accessing the CMEMS products at the source for the harvester type you selected above.

`make_private` is optional and defaults to `false`. If `true`, the datasets created by the harvester will be marked private. This setting is not retroactive. It only applies to datasets created by the harvester while the setting is `true`.

Examples of config:
```
{
"harvester_type":"slv",
"start_date":"2017-01-01",
"username":"your_username",
"password":"your_password",
"make_private":false
}
```
```
{
  "harvester_type": "sic_south",
  "start_date": "2017-01-01",
  "end_date": "TODAY",
  "timeout": 10,
  "username": "your_username",
  "password": "your_password",
  "make_private": false
}
```
### <a name="running-cmems"></a>Running a CMEMS harvester
You can run the harvester on a Daily update frequencey with `YESTERDAY` and `TODAY` as the start and end dates. Since requests may time out, you can also run the harvester more than once a day using the Manual update frequency and a cron job. There's no way to recover from outages at the moment; the CMEMS harvester could be more robust.

## <a name="harvesting-gome2"></a>Harvesting GOME-2 products
The GOME-2 harvester harvests products from the following GOME-2 coverages:
1. GOME2_O3
2. GOME2_NO2
3. GOME2_TropNO2
4. GOME2_SO2
5. GOME2_SO2mass

Unlike other harvesters, the GOME-2 harvester only makes requests to verify that a product exists. It programmatically creates datasets and resources for products that do exist within the specified date range.

### <a name="gome2-settings"></a>GOME-2 Settings
The GOME-2 harvester has two required and one optional setting.
1. `start_date` (required) determines the date on which the harvesting begins. It must be in the format `YYY-MM-DD` or the string `"YESTERDAY"`. If you want to harvest from the earliest product onwards, use `2007-01-04`. If you will be harvesting on a daily basis, use `"YESTERDAY"`
2. `end_date` (required) determines the date on which the harvesting ends. It must be in the format `YYY-MM-DD` or the string `"TODAY"`. It is exclusive, i.e., if the end date is `2017-03-2`, then products will be harvested up to _and including_ 2017-03-01 and no products from 2017-03-02 will be included. For daily harvesting use `"TODAY"`.
3. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Example of GOME-2 settings
```
{
    "start_date": "2017-03-01",
    "end_date": "2017-03-02",
    "make_private": false
}
```

or

```
{
    "start_date": "YESTERDAY",
    "end_date": "TODAY",
    "make_private": false
}
```
### <a name="running-gome2"></a>Running a GOME-2 harvester
1. Add `gome2` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. The URL you enter does not matter--the GOME-2 harvester only makes requests to a predetermined set of URLs. Select `GOME2` from the list of harvesters.
4. Add a config as described above.
5. Select a frequency from the frequencey options. If you want to use a cron job (recommended) to run the harvester, select `Manual`.

## <a name="harvesting-proba-v"></a>Harvesting PROBA-V products
The PROBA-V harvester harvests products from the following collections:

- On time collections:
    1. PROBAV_L2A_1KM_V001
    2. Proba-V Level-1C
    3. Proba-V S1-TOC (1KM)
    4. Proba-V S1-TOA (1KM)
    5. Proba-V S10-TOC (1KM)
    6. Proba-V S10-TOC NDVI (1KM)
- One month delayed collections with 333M resolution:
    1. Proba-V Level-2A (333M)
    2. Proba-V S1-TOA (333M)
    3. Proba-V S1-TOC (333M)
    4. Proba-V S10-TOC (333M)
    5. Proba-V S10-TOC NDVI (333M)
- One month delayed collections with 100M resolution:
    1. Proba-V Level-2A (100M)
    2. Proba-V S1-TOA (100M)
    3. Proba-V S1-TOC (100M)
    4. Proba-V S1-TOC NDVI (100M)
    5. Proba-V S5-TOA (100M)
    6. Proba-V S5-TOC (100M)
    7. Proba-V S5-TOC NDVI (100M)

The products from the on time collections are created and published on the same day.
The product from delayed collections are published with one month delay after being created.

The collections were also splitted according to the resoltion to avoid a huge number of datasets being harvested.
L1C, L2A and S1 products are published daily. S5 products are published every 5 days. S10 products are published every 10 days.
S1, S5 and S10 products are tiles covering almost the entire world. Each dataset correspond to a single tile.

### <a name="proba-v-settings"></a>PROBA-V Settings
The PROBA-V harvester has configuration as:
1. `start_date` (required) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DD`. If you want to harvest from the earliest product onwards, use `2018-01-01`
2. `end_date` (optional) determines the end date for the harvester job. It must be a string describing a date in the format `YYYY-MM-DD`, like 2018-01-31. The end_date is not mandatory and if not included the harvester will run until catch up the current day. To limit the number of datasets per job each job will harvest a maximum of 2 days of data.
3. `username` and `password` are your username and password for accessing the PROBA-V products at the source.
4. `collection` (required) to define the collection that will be collected. It can be `PROBAV_P_V001`, `PROBAV_S1-TOA_1KM_V001`, `PROBAV_S1-TOC_1KM_V001`, `PROBAV_S10-TOC_1KM_V001`, `PROBAV_S10-TOC-NDVI_1KM_V001`, `PROBAV_S1-TOA_100M_V001`, `PROBAV_S1-TOC-NDVI_100M_V001`, `PROBAV_S5-TOC-NDVI_100M_V001`, `PROBAV_S5-TOA_100M_V001`, `PROBAV_S5-TOC_100M_V001`, `PROBAV_S1-TOC_100M_V001`, `PROBAV_S1-TOA_333M_V001`, `PROBAV_S1-TOC_333M_V001`, `PROBAV_S10-TOC_333M_V001`, `PROBAV_S10-TOC-NDVI_333M_V001`, `PROBAV_L2A_1KM_V001`, `PROBAV_L2A_100M_V001` or `PROBAV_L2A_333M_V001`.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of PROVA-V settings
```
{
"start_date":"2018-08-01",
"collection":"PROBAV_S1-TOC_1KM_V001",
"username":"nextgeoss",
"password":"nextgeoss",
"make_private":false
}
```
```
{
"start_date":"2018-08-01",
"collection":"PROBAV_L2A_1KM_V001",
"username":"nextgeoss",
"password":"nextgeoss",
"make_private":false
}
```
```
{
"start_date":"2018-08-01",
"collection":"PROBAV_P_V001",
"username":"nextgeoss",
"password":"nextgeoss",
"make_private":false
}
```

The start_date for the delayed collections can be any date before the current_day - 1 month. For the current collections the start_date can be any date.

### <a name="running-proba-v"></a>Running a PROBA-V harvester
1. Add `probav` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `Proba-V Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select a frequency from the frequencey options. If you want to use a cron job (recommended) to run the harvester, select `Manual`.
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-glass-lai"></a>Harvesting GLASS LAI products
The GLASS LAI harvester harvests products from the following collections:

- LAI_1KM_AVHRR_8DAYS_GL (from 1982 to 2015)
- LAI_1KM_MODIS_8DAYS_GL (from 2001 to 2015)

### <a name="glass-lai-settings"></a>GLASS LAI Settings
The GLASS LAI harvester has configuration as:
1. `sensor` to define if the harvester will collect products based on AVHRR (`avhrr`) or MODIS (`modis`).
6. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of GLASS LAI settings
```
{
"sensor":"avhrr",
"make_private":false
}
```
```
{
"sensor":"modis",
"make_private":false
}
```

### <a name="running-glass-lai"></a>Running a GLASS LAI harvester
1. Add `glass_lai` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `GLASS LAI Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. The harvester only needs to run twice (with two different configurations).
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-static-ebvs"></a>Harvesting static EBVs
The static EBVs harvester harvests products from the following collections:

- TREE_SPECIES_DISTRIBUTION_HABITAT_SUITABILITY
- FLOOD_HAZARD_EU_GL
- RSP_AVHRR_1KM_ANNUAL_USA
- EMODIS_PHENOLOGY_250M_ANNUAL_USA

### <a name="static-ebvs-settings"></a>Static EBVs Settings
The Static EBVs harvester has configuration as:
1. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of GLASS LAI settings
```
{
"make_private":false
}
```

### <a name="running-static-ebvs"></a>Running a static EBVs harvester
1. Add `ebvs` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `EBVs` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. The harvester only needs to run once because the datasets are static.


## <a name="harvesting-plan4all"></a>Harvesting Plan4All products
The Plan4All harvester harvests products from the following collections:

- Open Land Use Map (from the European Project: Plan4All)

### <a name="plan4all-settings"></a>Plan4All Settings
The Plan4All harvester has configuration as:
1. `datasets_per_job` (optional, integer, defaults to 100) determines the maximum number of products that will be harvested during each job. If a query returns 2,501 results, only the first 100 will be harvested if you're using the default. This is useful for running the harvester via recurring jobs intended to harvest products incrementally (i.e., you want to start from the beginning and harvest all available products). The harvester will harvest products in groups of 100, rather than attmepting to harvest all x-hundred-thousand at once. You'll get feedback after each job, so you'll know if there are errors without waiting for the whole job to run. And the harvester will automatically resume from the harvested dataset if you're running it via a recurring cron job.
2. `timeout` (optional, integer, defaults to 60) determines the number of seconds to wait before timing out a request.
3. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of Plan4All settings
```
{
  "datasets_per_job": 10,
  "timeout": 60,
  "make_private": false
}
```
### <a name="running-plan4all"></a>Running a Plan4All harvester
1. Add `plan4all` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `Plan4All Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.


## <a name="harvesting-modis"></a>Harvesting MODIS products
The MODIS harvester harvests products from the following collections, which can be divided by time resolution:

- 8 days:
    1. MOD17A2H (currently 249848 datasets, starting at 2000-02-18T00:00:00Z)
    2. MYD15A2H (currently 218456 datasets, starting at 2002-07-04T00:00:00Z)
    3. MOD15A2H (currently 248647 datasets, starting at 2000-02-18T00:00:00Z)
    4. MOD14A2  (currently 255161 datasets, starting at 2000-02-18T00:00:00Z)
    5. MYD14A2  (currently 223424 datasets, starting at 2002-07-04T00:00:00Z)
- 16 days:
    1. MYD13Q1  (currently 110551 datasets, starting at 2002-07-04T00:00:00Z)
    2. MYD13A1  (currently 110551 datasets, starting at 2002-07-04T00:00:00Z)
    3. MYD13A2  (currently 110540 datasets, starting at 2002-07-04T00:00:00Z)
    4. MOD13Q1  (currently 126268 datasets, starting at 2000-02-18T00:00:00Z)
    5. MOD13A1  (currently 126268 datasets, starting at 2000-02-18T00:00:00Z)
    6. MOD13A2  (currently 126268 datasets, starting at 2000-02-18T00:00:00Z)
- Yearly:
    1. MOD17A3H (currently   4110 datasets, starting at 2000-12-26T00:00:00Z)

All collections, with the exception of collection MOD17A3H, are updated on a weekly / biweekly basis. Collection MOD17A3H is the only collection that is static, where the last dataset refers to 2015-01-03. 

Due to the fact that granule queries now require collection identifiers, each collection has to be harvested with different harvesters.

### <a name="modis-settings"></a>MODIS Settings
The MODIS harvester has configuration has:
1. `collection` (required) to define the collection that will be collected. It can be `MYD13Q1`, `MYD13A1`, `MYD13A2`, `MOD13Q1`, `MOD13A1`, `MOD13A2`, `MOD17A3H`, `MOD17A2H`, `MYD15A2H`, `MOD15A2H`, `MOD14A2`, `MYD14A2`.
2. `start_date` (required) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DDTHH:MM:SSZ`. If you want to harvest from the earliest product onwards, use the starting dates presented in "Harvesting MODIS products"
3. `timeout` (optional, integer, defaults to 10) determines the number of seconds to wait before timing out a request.
4. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of MODIS settings
```
{
  "collection": "MYD13Q1",
  "start_date": "2002-07-04T00:00:00Z",
  "make_private": false
}
```

### <a name="running-modis"></a>Running a MODIS harvester
1. Add `modis` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `MODIS Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the freuqency options. 

## <a name="harvesting-gdacs"></a>Harvesting GDACS Average Flood products
The GDACS harvester harvests products from the following collections:
- AVERAGE_FLOOD_SIGNAL (from 1997/12 to present - 1 dataset per day)
- AVERAGE_FLOOD_MAGNITUDE (from 1997/12 to present - 1 dataset per day)

### <a name="gdacs-settings"></a>GDACS Settings
The GDACS harvester has configuration as:
1. `data_type` determines which collection will be harvested. It must be one of the following two strings: `signal` or `magnitude`.

2. `request_check` determines if the URL of each harvested dataset will be tested. It must be one of the following two strings: `yes` or `no`.

3. `start_date` determines the start date for the harvester job. It must be the string `YESTERDAY` or a string describing a date in the format `YYYY-MM-DD`, like `1997-12-01`.

4. `end_date` determines the end date for the harvester job. It must be the string `TODAY` or a string describing a date in the format `YYYY-MM-DD`, like `1997-12-01`. The end_date is not mandatory and if not included the harvester will run until catch up the current day.

5. `timeout` determines how long the harvester will wait for a response from a server before cancelling the attempt. It must be a postive integer. Not mandatory.

6. `make_private` is optional and defaults to `false`. If `true`, the datasets created by the harvester will be marked private. This setting is not retroactive. It only applies to datasets created by the harvester while the setting is `true`.

#### Examples of GDACS settings
```
{
"data_type":"signal",
"request_check":"yes",
"start_date":"1997-12-01",
"make_private":false
}

or

{
"data_type":"magnitude",
"request_check":"yes",
"start_date":"1997-12-01",
"make_private":false
}
```

### <a name="running-gdacs"></a>Running a GDACS harvester
1. Add `gdacs` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `GDACS` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-deimos2"></a>Harvesting DEIMOS-2 products
The DEIMOS-2 harvester harvests products from the following collections:

- DEIMOS-2 PM4 Level-1B 
- DEIMOS-2 PSH Level-1B 
- DEIMOS-2 PSH Level-1C 

The number of products is static, and thus the harvaster only needs to be run once.

### <a name="deimos2-settings"></a>DEIMOS-2 Settings
The DEIMOS-2 harvester has configuration as:
1. `harvester_type` determines the ftp domain, as well as the directories in said domain.
2. `username` and `password` are your username and password for accessing the DEIMOS-2 products at the source for the harvester type you selected above.
3. `timeout` (optional, integer, defaults to 60) determines the number of seconds to wait before timing out a request.
4. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of DEIMOS-2 settings
```
{
"harvester_type":"deimos_imaging",
"username":"your_username",
"password":"your_password",
"make_private":false
}
```

### <a name="running-deimos2"></a>Running a DEIMOS-2 harvester
1. Add `deimosimg` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `DEIMOS Imaging` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-ebasnilu"></a>Harvesting EBAS-NILU products
The EBAS-NILU harvester collects products from the following collections:
- EBAS NILU Data Archive

### <a name="ebasnilu-settings"></a>EBAS-NILU Settings
The EBAS-NILU harvester has configuration as:
1. `start_date`: (optional, datetime string, if the harvester is new, or from the ingestion date of the most recently harvested product if it has been run before) determines the start of the date range for harvester queries. Example: "start_date": "2018-01-16T10:30:00Z". Note that the entire datetime string is required. `2018-01-01` is not valid. 
2. `end_date`: (optional, datetime string, default is "NOW") determines the end of the date range for harvester queries. Example: "end_date": "2018-01-16T11:00:00Z". Note that the entire datetime string is required. `2018-01-01` is not valid.
4. `timeout`: (optional, integer, defaults to 10) determines the number of seconds to wait before timing out a request.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of EBAS-NILU settings
```
{
"start_date": "2017-01-01T00:00:00Z",
"timeout": 4,
"make_private": false
}
```

### <a name="running-ebasnilu"></a>Running a EBAS-NILU harvester
1. Add `ebas` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `EBAS Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 

## <a name="harvesting-simocean"></a>Harvesting SIMOcean products
The SIMOcean harvester harvests products from the following collections:

- SIMOcean Mean Sea Level Pressure Forecast 
- SIMOcean Port Sea State Forecast From SMARTWAVE
- SIMOcean Tidal Data
- SIMOcean Sea Surface Temperature Forecast
- SIMOcean Data From Multiparametric Buoys
- SIMOcean Surface Wind Forecast From AROME
- SIMOcean Cloudiness Forecast From AROME
- SIMOcean Air Surface Temperature Forecast
- SIMOcean Surface Currents From HF Radar
- SIMOcean Sea Wave Period Forecast
- SIMOcean Nearshore Sea State Forecast From SWAN
- SIMOcean Sea Surface Wind Forecast
- SIMOcean Precipitation Forecast From AROME
- SIMOcean Significant Wave Height Forecast
- SIMOcean Sea Wave Direction Forecast
- SIMOcean Surface Forecast From HYCOM 

New products of these collections are created and published daily.

### <a name="simocean-settings"></a>SIMOcean Settings
The SIMOcean harvester has configuration as:
1. `start_date`: (required, datetime string, if the harvester is new, or from the ingestion date of the most recently harvested product if it has been run before) determines the start of the date range for harvester queries. Example: "start_date": "2018-01-16T10:30:00Z". Note that the entire datetime string is required. `2018-01-01` is not valid. 
2. `end_date`: (optional, datetime string, default is "NOW") determines the end of the date range for harvester queries. Example: "end_date": "2018-01-16T11:00:00Z". Note that the entire datetime string is required. `2018-01-01` is not valid.
3. `datasets_per_job`: (optional, integer, defaults to 100) determines the maximum number of products that will be harvested during each job. 
4. `timeout`: (optional, integer, defaults to 10) determines the number of seconds to wait before timing out a request.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of SIMOcean settings
```
{
"start_date": "2017-01-01T00:00:00Z",
"timeout": 4,
"datasets_per_job": 100,
"make_private": false
}
```

### <a name="running-simocean"></a>Running a SIMOcean harvester
1. Add `simocean` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `SIMOcean Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 

## <a name="harvesting-epos"></a>Harvesting EPOS-Sat products
The EPOS-Sat harvester harvests products from the following collections:

- Unwrapped Interferogram 
- Wrapped Interferogram  
- LOS Displacement Timeseries
- Spatial Coherence 
- Interferogram APS Global Model
- Map of LOS Vector

The number of products is low, due to the fact that currently there are only sample data. A large quantity of data is expected to start being injected in September of 2019. 

### <a name="epos-settings"></a>EPOS-Sat Settings
The EPOS-Sat harvester has configuration as:
1. `collection` (required) to define the collection that will be collected. It can be `inu`, `inw`, `dts`, `coh`, `aps`, `cosneu`.
2. `start_date` (required) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DDTHH:MM:SSZ`. If you want to harvest from the earliest product onwards, use `2010-01-01T00:00:00Z`. 
3. `end_date` (optional) determines the date on which the harvesting ends. It must be in the format `YYYY-MM-DDTHH:MM:SSZ`, it defaults into `TODAY`.
4. `datasets_per_job` (optional, integer, defaults to 100) determines the maximum number of products that will be harvested during each job.
5. `timeout` (optional, integer, defaults to 4) determines the number of seconds to wait before timing out a request.
6. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of EPOS-Sat settings
```
{
  "collection": "inw",
  "start_date": "2010-01-16T10:30:00Z",
  "timeout": 4,
  "make_private":  false
}
```

### <a name="running-epos"></a>Running a EPOS-Sat harvester
1. Add `epos` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `EPOS Sat Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-foodsecurity"></a>Harvesting Food Security pilot outputs
The Food Security harvester harvests the VITO pilot outputs for the following collections:

    1. NextGEOSS Sentinel-2 FAPAR
    2. NextGEOSS Sentinel-2 FCOVER
    3. NextGEOSS Sentinel-2 LAI
    4. NextGEOSS Sentinel-2 NDVI

The date of the pilot outputs can be different of the current date since the pilot processes old Sentinel Data.

### <a name="foodsecurity-settings"></a>Food Security Settings
The Food Security harvester has configuration has:
1. `start_date` (required) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DD`. If you want to harvest from the earliest product onwards, use `2017-01-01`
2. `end_date` (optional) determines the end date for the harvester job. It must be a string describing a date in the format `YYYY-MM-DD`, like 2018-01-31. The end_date is not mandatory and if not included the harvester will run until catch up the current day. To limit the number of datasets per job each job will harvest a maximum of 2 days of data.
3. `username` and `password` are your username and password for accessing the PROBA-V products at the source.
4. `collection` (required) to define the collection that will be collected. It can be `FAPAR`, `FCOVER`, `LAI` or `NDVI`.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of Food Security settings
```
{
"start_date":"2017-01-01",
"collection":"FAPAR",
"username":"nextgeoss",
"password":"nextgeoss",
}
```

### <a name="running-foodsecurity"></a>Running a Food Security harvester
1. Add `foodsecurity` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `Food Security Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options.
6. Run the harvester. It will programmatically create datasets.


## <a name="harvesting-vitocgss1"></a>Harvesting VITO CGS S1 products
The VITO CGS S1 harvester collects the products of an external VITO project for the following collections:

    1. VITO CGS S1
    2. CGS S1 GRD L1
    3. CGS S1 GRD SIGMA0 L1 (NOT AVAILABLE YET)



### <a name="vitocgss1-settings"></a>VITO CGS S1 Settings
The Food Security harvester has configuration has:
1. `start_date` (required) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DD`. If you want to harvest from the earliest product onwards, use `2017-01-01`
2. `timeout` (optional, integer, defaults to 4) determines the number of seconds to wait before timing out a request.
3. `username` and `password` are your username and password for accessing the PROBA-V products at the source.
4. `collection` (required) to define the collection that will be collected. It can be `SLC_L1`, `GRD_L1`.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of VITO CGS S1 settings
```
{
"start_date":"2018-01-01",
"collection":"SLC_L1",
"username":"username",
"password":"password",
"timeout":1,
"make_private":false
}
```


### <a name="running-gdacs"></a>Running a GDACS harvester
1. Add `gdacs` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `GDACS Harvester` from the list of harvesters.
### <a name="running-vitocgss1"></a>Running a VITO CGS S1 harvester
1. Add `cgss1` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `VITO CGS S1 Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options.
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-coldregions"></a>Harvesting Cold Regions pilot outputs
The Cold Regions harvester harvests the NERSC pilot outputs for the following collections:

    1. Sentinel-1 HH/HV based ice/water classification
    2. Sea ice and water classification in the Arctic for INTAROS 2018 field experiment
    3. Sea ice and water classification in the Arctic for CAATEX/INTAROS 2019 field experiment
    4. Average sea ice drift in the Arctic for INTAROS 2018 field experiment
    5. Average sea ice drift in the Arctic for CAATEX 2019 field experiment

### <a name="running-coldregions"></a>Running a Cold Regions Harvester
The Cold Regions harvester will run one time per collection and it will collect all the cold regions datasets within the input collection(static data). In the command line run:

```
$ python ./ckanext/nextgeossharvest/harvesters/coldregions.py <destination_ckan_URL> <destination_ckan_apikey> "nersc" <collection_id>
```
The following collection IDs are available:
- S1_ARCTIC_SEAICEEDGE_CLASSIFICATION
- S1_ARCTIC_SEAICEEDGE_CLASSIFICATION_INTAROS_2018
- S1_ARCTIC_SEAICEEDGE_CLASSIFICATION_CAATEX_INTAROS_2019
- S1_ARCTIC_SEAICEDRIFT_AVERAGE_INTAROS_2018
- S1_ARCTIC_SEAICEDRIFT_AVERAGE_CAATEX_2019

## <a name="harvesting-landsat8"></a>Harvesting Landsat-8 products
The Landsat-8 harvester collects the Level-1 data products generated from Landsat 8 Operational Land Imager (OLI)/Thermal Infrared Sensor (TIRS). The following collection 1 Tiers are harvested:

    1. Landsat-8 Real-Time (RT)
    2. Landsat-8 Tier 1 (T1)
    3. Landsat-8 Tier 2 (T2)

The pre-processed products are not harvested due to the fact that they are deleted in a time interval of 6 months in favor of calibrated products.

### <a name="landsat8-settings"></a>Landsat-8 Settings
The Landsat-8 harvester has configuration has:
1. `path` (optional) determines the WRS path, where the product collection will start.
2. `row` (optional) determines the WRS row, where the product collection will start.
3. `access_key` and `secret_key` (required) are your AWS account access and secret key.
4. `bucket` (required) to define the AWS S3 bucket to harvest, for Landsat-8 use `landsat-pds`.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of Landsat-8 settings
```
{
  "path":1,
  "row":1,
  "access_key":"your_access_key",
  "secret_key": "your_secret_key",
  "bucket": "landsat-pds",
  "make_private": false
}
```

### <a name="running-landsat8"></a>Running a Landsat-8 harvester
1. Add `landsat8` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `Landsat-8 Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options.
6. Run the harvester. It will programmatically create datasets.

## <a name="harvesting-meloa"></a>Harvesting MELOA products
The MELOA harvester harvests products from the following collections:

- MELOA Wavy Measurements - Littoral
- MELOA Wavy Measurements - Ocean
- MELOA Wavy Measurements - Basic

New products of these collections are created and published after the campaigns.

### <a name="meloa-settings"></a>MELOA Settings
The MELOA harvester has configuration as:
1. `start_date`: (required, datetime string, if the harvester is new, or from the ingestion date of the most recently harvested product if it has been run before) determines the start of the date range for harvester queries. Example: "start_date": "2019-10-01T00:00:00Z". Note that the entire datetime string is required. `2019-10-01` is not valid. 
2. `end_date`: (optional, datetime string, default is "NOW") determines the end of the date range for harvester queries. Example: "end_date": "2020-01-01T00:00:00Z". Note that the entire datetime string is required. `2020-01-01` is not valid.
3. `datasets_per_job`: (optional, integer, defaults to 100) determines the maximum number of products that will be harvested during each job. 
4. `timeout`: (optional, integer, defaults to 10) determines the number of seconds to wait before timing out a request.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of MELOA settings
```
{
"start_date": "2019-10-01T00:00:00Z",
"timeout": 4,
"datasets_per_job": 100,
"make_private": false
}
```

### <a name="running-meloa"></a>Running a MELOA harvester
1. Add `meloa` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `MELOA Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 

## <a name="harvesting-saeon"></a>Harvesting SAEON products
The SAEON harvester collects the products for the following collections:

    1. Climate Systems Analysis Group (South Africa)

### <a name="saeon-settings"></a>SAEON Settings
The SAEON harvester has configuration has:
1. `datasets_per_job` (optional, integer, defaults to 100) determines the maximum number of products that will be harvested during each job. If a query returns 2,501 results, only the first 100 will be harvested if you're using the default. This is useful for running the harvester via recurring jobs intended to harvest products incrementally (i.e., you want to start from the beginning and harvest all available products). The harvester will harvest products in groups of 100, rather than attmepting to harvest all x-hundred-thousand at once. You'll get feedback after each job, so you'll know if there are errors without waiting for the whole job to run. And the harvester will automatically resume from the harvested dataset if you're running it via a recurring cron job.
2. `timeout` (optional, integer, defaults to 60) determines the number of seconds to wait before timing out a request.
3. `update_all` (optional, boolean, default is `false`) determines whether or not the harvester updates datasets that already have metadadata from this source. For example: if we have "update_all": true, and dataset Foo has already been created or updated by harvesting, then it will be updated again when the harvester runs. If we have "update_all": false and Foo has already been created or updated by harvesting, then the dataset will not be updated when the harvester runs. And regardless of whether update_all is true or false, if a dataset has not been collected, then it will be created in the catalogue.
4. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.
5. `source_url` determines the base URL for the data source to query.

#### Examples of SAEON settings
```
{
  "datasets_per_job": 100,
  "timeout": 60,
  "make_private": false,
  "source_url": "https://staging.saeon.ac.za"
}
```
### <a name="running-saeon"></a>Running a SAEON harvester
1. Add `saeon` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `SAEON Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.


## <a name="harvesting-noa_gs"></a>Harvesting NOA Groundsegment products
The NOA Groundsegment harvester collects the products for the following instruments:

    1. VIIRS (Visible Infrared Imaging Radiometer Suite)
    2. MODIS (Moderate Resolution Imaging Spectroradiometer)
    3. AIRS (Atmospheric InfraRed Sounder)
    4. MERSI (Medium Resolution Spectral Imager)
    5. AVHRR/3 (Advanced Very-High-Resolution Radiometer)

### <a name="noa_gs-settings"></a>NOA Groundsegment Settings
The NOA Groundsegment harvester configuration contains the following options:
1. `start_date` (optional) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DDTHH:mm:ssZ`.
2. `end_date` (optional) determines the date on which the harvesting ends. It must be in the format `YYYY-MM-DDTHH:mm:ssZ`.
3. `username`  (required) Enter your NOA groundsegment username.
4. `password`  (required) Enter your NOA groundsegment password.
5. `page_timeout` (optional, integer, defaults to 2) determines the maximum number of pages that will be harvested during each job. If a query returns 25 pages, only the first 5 will be harvested if you're using the default. Each page corresponds to 100 products. This is useful for running the harvester via recurring jobs intended to harvest products incrementally (i.e., you want to start from the beginning and harvest all available products). The harvester will harvest products in groups of 500, rather than attempting to harvest all x-hundred-thousand at once. You'll get feedback after each job, so you'll know if there are errors without waiting for the whole job to run. And the harvester will automatically resume from the harvested dataset if you're running it via a recurring cron job.
6. `update_all` (optional, boolean, default is `false`) determines whether or not the harvester updates datasets that already have metadadata from this source. For example: if we have "update_all": true, and dataset Foo has already been created or updated by harvesting, then it will be updated again when the harvester runs. If we have "update_all": false and Foo has already been created or updated by harvesting, then the dataset will not be updated when the harvester runs. And regardless of whether update_all is true or false, if a dataset has not been collected, then it will be created in the catalogue.
7. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of NOA GS settings
```
{
  "start_date":"2019-01-01T00:00:00Z",
  "end_date":"2020-08-01T23:59:00Z",
  "username":"your_username",
  "password":"your_password",
  "page_timeout": "2"
}
```
### <a name="running-noa_gs"></a>Running a NOA Groundsegment harvester
1. Add `noa_groundsegment` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `NOA Groundsegment Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.


## <a name="harvesting-fsscat"></a>Harvesting FSSCAT products
The FSSCAT harvester collects the products for the following file types:

    1. FS1_GRF_L1B_CAL
    2. FS1_GRF_L1B_SCI
    3. FS1_GRF_L1C_SCI
    4. FS1_GRF_L2__SIE
    5. FS1_GRF_L3__ICM
    6. FS1_MWR_L1B_SCI
    7. FS1_MWR_L1C_SCI
    8. FS1_MWR_L2A_TB_
    9. FS1_MWR_L2B_SIT
    10. FS1_MWR_L2B_SM_
    11. FS1_MWR_L3__TB_
    12. FS1_MWR_L3__SIT
    13. FS1_MWR_L3__SM_
    14. FS1_MWR_L4__SM_
    15. FS2_HPS_L1C_SCI
    16. FS2_HPS_L2__RDI
    17. FSS_SYN_L4__SM_

### <a name="fsscat-settings"></a>FSSCAT Settings
The FSSCAT harvester has configuration as:
1. `file_type` (required) determines the FSSCAT file type to be catalogued. It can be `FS1_GRF_L1B_CAL`, `FS1_GRF_L1B_SCI`, `FS1_GRF_L1C_SCI`, `FS1_GRF_L2__SIE`, `FS1_GRF_L3__ICM`, `FS1_MWR_L1B_SCI`, `FS1_MWR_L1C_SCI`, `FS1_MWR_L2A_TB_`, `FS1_MWR_L2B_SIT`, `FS1_MWR_L2B_SM_`, `FS1_MWR_L3__TB_`, `FS1_MWR_L3__SIT`, `FS1_MWR_L3__SM_`, `FS1_MWR_L4__SM_`, `FS2_HPS_L1C_SCI`, `FS2_HPS_L2__RDI` or `FSS_SYN_L4__SM_`.
2. `start_date` (mandatory) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DD`.
3. `end_date` (optional) determines the date on which the harvesting ends. It must be in the format `YYYY-MM-DD`.
4. `ftp_domain` (required) URL of the FSSCAT FTP.
5. `ftp_path` (required) Path of the FSSCAT FTP where the list of files are published.
6. `ftp_pass` (required) Password of the FSSCAT FTP.
7. `ftp_user` (required) Username of the user allowed to access the harvesting directory in the FSSCAT FTP.
8. `ftp_port`  (optional, integer, defaults to 21) Port of the FSSCAT FTP.
9. `ftp_timeout` (optional, integer, defaults to 20) determines the seconds until the timeout when accessing the FTP.
10. `update_all` (optional, boolean, default is `false`) determines whether or not the harvester updates datasets that already have metadadata from this source. For example: if we have "update_all": true, and dataset Foo has already been created or updated by harvesting, then it will be updated again when the harvester runs. If we have "update_all": false and Foo has already been created or updated by harvesting, then the dataset will not be updated when the harvester runs. And regardless of whether update_all is true or false, if a dataset has not been collected, then it will be created in the catalogue.
11. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.
12. `max_datasets` (optional, integer, defaults to 100) determines the maximum number of datasets to be catalogued.

#### Examples of FSSCAT settings
```
{
  "start_date": "2020-10-30",
  "end_date": "2020-11-01",
  "file_type": "FS1_GRF_L1C_SCI",
  "ftp_domain": "<FSSCAT_FTP_DOMAIN>",
  "ftp_path": "<FSSCAT_FTP_PATH>",
  "ftp_pass": "<FSSCAT_FTP_PASS>",
  "ftp_user": "<FSSCAT_FTP_USER>",
  "ftp_port": 21,
  "make_private": false,
  "max_dataset": 10
}
```
### <a name="running-fsscat"></a>Running a FSSCAT harvester
1. Add `fsscat` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `FSSCAT Harvester` from the list of harvesters.

## <a name="harvesting-noa_geob"></a>Harvesting NOA GeObservatory products
The NOA GeObservatory is activated in major geohazard events (earthquakes, volcanic activity, landslides,etc.) and automatically produces a series of Sentinel-1 based co-event interferograms (DInSAR) to map the surface deformation associated with the event. It also produces pre-event interferograms to be used as a benchmark.

This harvester collects the aforementioned interferograms.

### <a name="noa_geob-settings"></a>NOA GeObservatory Settings
The NOA GeObservatory harvester configuration contains the following options:
1. `start_date` (optional) determines the date on which the harvesting begins. It must be in the format `YYYY-MM-DDTHH:mm:ssZ`.
2. `end_date` (optional) determines the date on which the harvesting ends. It must be in the format `YYYY-MM-DDTHH:mm:ssZ`.
3. `page_timeout` (optional, integer, defaults to 2) determines the maximum number of pages that will be harvested during each job. If a query returns 25 pages, only the first 5 will be harvested if you're using the default. Each page corresponds to 100 products. This is useful for running the harvester via recurring jobs intended to harvest products incrementally (i.e., you want to start from the beginning and harvest all available products). The harvester will harvest products in groups of 500, rather than attempting to harvest all x-hundred-thousand at once. You'll get feedback after each job, so you'll know if there are errors without waiting for the whole job to run. And the harvester will automatically resume from the harvested dataset if you're running it via a recurring cron job.
4. `update_all` (optional, boolean, default is `false`) determines whether or not the harvester updates datasets that already have metadadata from this source. For example: if we have "update_all": true, and dataset Foo has already been created or updated by harvesting, then it will be updated again when the harvester runs. If we have "update_all": false and Foo has already been created or updated by harvesting, then the dataset will not be updated when the harvester runs. And regardless of whether update_all is true or false, if a dataset has not been collected, then it will be created in the catalogue.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of NOA GeObservatory settings
```
{
  "start_date":"2017-01-01T00:00:00Z",
  "end_date":"2020-08-01T23:59:00Z",
  "page_timeout": "2"
}
```
### <a name="running-noa_geob"></a>Running a NOA GeObservatory harvester
1. Add `noa_geobservatory` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `NOA GeObservatory Harvester` from the list of harvesters.

4. Add a config as described above.
5. Select `Manual` from the frequency options. 
6. Run the harvester. It will programmatically create datasets.


## <a name="harvesting-energydata"></a>Harvesting Energy Data products
The Energy Data harvester harvests products from the Energy Data API

### <a name="energydata-settings"></a>Energy Data Settings
The Energy Data harvester has configuration as:
1. `start_date`: (required, datetime string, if the harvester is new, or from the ingestion date of the most recently harvested product if it has been run before) determines the start of the date range for harvester queries. Example: "start_date": "2019-10-01T00:00:00Z". Note that the entire datetime string is required. `2019-10-01` is not valid. 
2. `end_date`: (optional, datetime string, default is "NOW") determines the end of the date range for harvester queries. Example: "end_date": "2020-01-01T00:00:00Z". Note that the entire datetime string is required. `2020-01-01` is not valid.
3. `datasets_per_job`: (optional, integer, defaults to 100) determines the maximum number of products that will be harvested during each job. 
4. `timeout`: (optional, integer, defaults to 10) determines the number of seconds to wait before timing out a request.
5. `make_private` (optional) determines whether the datasets created by the harvester will be private or public. The default is `false`, i.e., by default, all datasets created by the harvester will be public.

#### Examples of Energy Data settings
```
{
"start_date": "2017-10-01T00:00:00Z",
"datasets_per_job": 100
}
```

### <a name="running-energydata"></a>Running an Energy Data harvester
1. Add `energydata` to the list of plugins in your .ini file.
2. Create a new harvester via the harvester interface.
3. Select `Energy Data Harvester` from the list of harvesters.
4. Add a config as described above.
5. Select `Manual` from the frequency options. 


## <a name="develop"></a>Developing new harvesters
### <a name="basicworkflow"></a>The basic harvester workflow
The basic harvester workflow is divided into three stages. Each stage has a related method, and each method must be included in the harvester plugin.

The three methods are:
1. `gather_stage()`
2. `fetch_stage()`
3. `import_stage()`

While the `fetch_stage()` method _must_ be included, it may be the case that the harvester does not require a fetch stage (for instance, if the source is an OpenSearch service, then the search results in the gather stage may already include the necessary content, so there's no need to fetch it again. In those cases, the `fetch_stage()` method will still be implemented, but it will just return `True`. The `gather_stage()` and `import_stage()` methods, however, will always include some amount of code, as they will always be used.
#### <a name="gather_stage"></a>gather_stage
To simplify things, the gather stage is used to create a list of datasets that will be created or updated in the final import stage. That's really all it's for. It is not meant for parsing content into dictionaries for creating or updating datasets (that occurs in the import stage). It also isn't meant for acquiring or storing raw content that will be parsed later (that occurs in the fetch stage)—with certain exceptions, like OpenSearch services, where the content is already provided in the initial search results.

The `gather_stage()` method returns a `list` of harvest object IDs, which the harvester will use for the next two stages. The IDs are generated by creating harvest objects for each dataset that should be created or updated. If the necessary content is already provided, it can be stored in the harvest object's `.content` attribute as a `str`. You can also create harvest object extras--ad hoc harvest object attributes--to store information like the status of the dataset (e.g., new or change), or to keep track of other information _about_ the harvest object or the dataset that will be created/updated. However, the harvest object extras are not intended to store things like the key/value pairs that will later be used to create the package dictionary for creating/updating the dataset. 1) The gather stage is not the time to perform such parsing and 2) since the raw content can be saved in the `.content` attribute, it is easier to just skip the intermediate step and create the package dictionary in the import stage.

The gather stage may proceed quickly because it does not require querying the source for each individual dataset. The goal is not to aquire the content in this stage—just to get a list of the datasets for which content is required. If individual source queries are necessary, they will be performed in the fetch stage.

During the gather stage, the `gather_stage()` method will be called once.

#### <a name="fetch_stage"></a>fetch_stage
During the fetch stage, the `fetch_stage()` method will be called for each harvest object/dataset in the list created during the gather stage.

The purpose of the fetch stage is to get the content necessary for creating or updating the dataset in the import stage. The raw content can be stored as a `str` in the harvest object's `.content` attribute.

As in the gather stage, the harvest object extras should only be used to store information about the harvest object.

The fetch stage is the time to make individual queries to the source. If that's not necessary (e.g., the source is an OpenSearch service), then `fetch_stage()` should just return `True`.

#### <a name="import_stage"></a>import_stage
During the import stage, the `import_stage()` method will be called for each harvest object/dataset in the list created during the gather stage except for those that raised exceptions during the fetch stage. In other words, the `import_stage()` method is called for every harvest object/dataset that has `.content`.

The purpose of the import stage is to parse the content and use it, as well as any additional context or information provided by the harvest object extras, to create or update a dataset.

### <a name="opensearchexample"></a>Example of an OpenSearch-based harvester
See the [OpenSearchExample harvester skeleton](https://github.com/NextGeoss/ckanext-nextgeossharvest/blob/master/ckanext/nextgeossharvest/harvesters/opensearch_example.py) for an example of how to use the libraries in this repository to build an OpenSearch-based harvester. There are detailed comments in the code, which can be copied as the starting point of a new harvester. If your harvester will not use an OpenSearch source, you'll also need to modify the `gather_stage` and possibly the `fetch_stage` methods, but the `import_stage` will remain the same.

## <a name="itag"></a>iTag
The iTag "harvester (`ITageEnricher`) is better described as a metaharvester. It uses the harvester infrastructure to add new tags and metadata to existing datasets. It is completely separate from the other harvesters, meaning: if you want to harvest Sentinel products, you'll use one of the Sentinel harvesters. If you want to enrich Sentinel datasets, you'll use an instance of `ITagEnricher`. But you'll use them separately, and they won't interact with eachother at all.

### <a name="itagprocess"></a>How ITagEnricher works
During the gather stage, it queries the CKAN instance itself to get a list of existing datasets that 1) have the `spatial` extra and 2) have not yet been updated by the ITageEnricher. Based on this list, it then creates harvest objects. This stage might be described as self-harvesting.

During the fetch stage, it queries an iTag instance using the coordinates from each dataset's `spatial` extra and then stores the response from iTag as `.content`, which will be used in the import stage. As long as iTag returns a valid response, the dataset moves on to the import stage—in other words, all that matters is that the query succeeded, not whether the iTag was able to find tags for a particular footprint. See below for an explanation.

During the import stage, it parses the iTag response to extract any additional tags and/or metadata. Regardless of whether any additional tags or metadata are found, the extra `itag: tagged` will be added to the dataset. This extra is used in the gather stage to filter out datasets for which successful iTag queries have been made.

### <a name="setupitag"></a> Setting up ITagEnricher
To set it up, create a new harvester source (we'll call ours "iTag Enricher" for the sake of example). Select `manual` for the update frequency. Select an organization (currently required—the metaharvester will only act on datasets that belong to that organization).

There are three configuration options:
1. `base_url`: **(required, string)** determines the base URL to use when querying your iTag instance.
2. `timeout`: (integer, defaults to 5) determines the number of seconds before a request times out.
3. `datasets_per_job`: (integer, defaults to 10) determines the maximum number of datasets per job.

Once you've created the harvester source, create the cron job below, using the name or ID of the source you just created:
`* * * * * paster --plugin=ckanext-harvest harvester job {name or id of harvest source} -c {path to CKAN config}`
The cron job will continually attempt to create a new harvest job. If there already is a running job for the source, the attempt will simply fail (this is the intended behaviour). If there is no running job, then a new job will be created, which will then be run by the `harvester run` cron job that you should already have set up. The metaharvester will then make a list of all the datasets that should be enriched with iTag, but which have not yet been enriched, and then try to enrich them.

### <a name="handlingitagerrors"></a>Handling iTag errors
If a query to iTag fails, 1) it will be reported in the error report for the respective job and 2) the metaharvester will automatically try to enrich that dataset the next time it runs. No additional logs or tracking are required--as long as a dataset hasn't been tagged, and should be tagged, it will be added to the list each time a job is created. Once a dataset has been tagged (or it has been determined that there are no tags that can be added to it), it will no longer appear on the list of datasets that should be tagged.

Currently, ITagEnricher only creates a list of max. 1,000 datasets for each job. This limit is intended to speed up the rate at which jobs are completed (and feedback on performance is available). Since a new job will be created as soon as the current one is marked `Finished`, this behaviour does not slow down the pace of tagging.

Sentinel-3 datasets have complex polygons that seem to cause iTag to timeout more often than it does when processesing requests related to other datasets, so Sentinel-3 datasets are currently filtered out of the list of datasets that need to be tagged.

In general, requests to iTag seem to timeout rather often, so it may be necessary to experiment with rate limiting. It may also be necessary to set up a more robust infrastructure for the iTag instance.

## <a name="tests"></a>Testing testing testing
All harvesters should have tests that actually run the harvester, from start to finish, more than once. Such tests verify that the harvester will work as intended in production. The `requests_mock` library allows us to easily mock the content returned by real requests to real URLs, so we can save the XML returned by OpenSearch interfaces, etc. and re-use it when testing. We can then write tests that verify 1) that the harvester starts, runs, finishes, and runs again (e.g., there are no errors that cause it to hang), 2) that it behaves as expected (e.g., it only updates datasets when a specific flag is set, or it restarts from a specific date following a failed request), and 3) that the datasets it creates or updates have exactly the metadata that we want them to have.

See `TestESAHarvester().test_harvester()` for an example of how to run a harvester in a testing environment with mocked requests that return real XML.

The test itself needs to be refined. Some of the blocks should be helper functions or fixtures. But the method itself contains all the necessary components of full test of harvester functionality: create a harvester with a given config, run it to completion under different conditions, and verify that the results are as expected.

The same structure can be used for our other harvesters (with different mocked requests, of course, and with different expected results).

Using the same structure, we can also add tests that verify that the metadata of the datasets that are created also match the expected/intended results.

## <a name="cron"></a>Suggested cron jobs
```
* * * * * paster --plugin=ckanext-harvest harvester run -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job itag-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job code-de-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job noa-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job scihub-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
```

## <a name="logs"></a>Logs
Both the ESA harvester and the iTag metadata harvester can optionally log the status codes and response times of the sources or services that they query. If you want to log the response times and status codes of requests to harvest sources and/or your iTag service, you must include `ckanext.nextgeossharvest.provider_log_dir=/path/to/your/logs` in your `.ini` file. The log entries will look like this: `INFO | esa_scihub   | 2018-03-08 14:17:04.474262 | 200 | 2.885231s` (the second field will always be 12 characters and will be padded if necessary).

The data provider log file is called `dataproviders_info.log`. The iTag service provider log is called `itag_uptime.log`
