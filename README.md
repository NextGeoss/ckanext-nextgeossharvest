# ckanext-nextgeossharvest
This extension contains harvester plugins for harvesting from sources used by NextGEOSS as well as a metaharvester plugin for adding additional tags and metadata retrieved from an iTag instance.

## Contents
1. [What's in the repository](#repo)
2. [Basic usage](#usage)
3. [Harvesting Sentinel products](#harvesting)
    1. [Harvesting from SciHub](#scihub)
    2. [Harvesting from NOA](#noa)
    3. [Harvesting from CODE-DE](#codede)
    4. [Harvesting from more than one Sentinel source](#multi)
    5. [How the three Sentinel harvesters work together](#alltogether)
4. [Developing new harvesters](#develop)
5. [iTag](#itag)
6. [A note on tests](#tests)

## <a name="repo"></a>What's in the repository
The repository contains four plugins:
1. `nextgeossharvest`, the base CKAN plugin
2. `esa`, a harvester plugin for harvesting Sentinel datasets from SciHub or NOA
3. `code_de`, a harvester plugin for harvesting Sentinel datasets from CODE-DE
4. `itag` a harvester plugin for adding additional tags and metadata to datasets that have already been harvested (more on this later)

## <a name="usage"></a>Basic usage
1. Run `python setup.py develop` in the `ckanext-nextgeossharvest` directory.
2. Run `pip install -r requirements.txt` n the `ckanext-nextgeossharvest` directory.
3. You will also need the following CKAN extensions:
    1. `ckanext-harvest`
    2. `ckanext-spatial`
1. You will want to configure `ckanext-spatial` to use `solr-spatial-field` for the spatial search backend. Instructions can be found here: http://docs.ckan.org/projects/ckanext-spatial/en/latest/spatial-search.html. You cannot use `solr` as the spatial search backend because `solr` only supports  footprints that are effectively bounding boxes (polygons composed of five points), while the footprints of the datasets harvested by these plugins can be considerably more complex. Using `postgis` as the spatial search backend is strongly discouraged, as it will choke on the large numbers of datasets that these harvesters will pull down.
2. Add the harvester and spatial plugins to the list of plugins in your `.ini` file, as well as `nextgeossharvest` and any of the NextGEOSS harvester plugins that you want to use.
3. If you will be harvesting from SciHub or NOA, add your username and password to `ckanext.nextgeossharvest.nextgeoss_username=` and `ckanext.nextgeossharvest.nextgeoss_password=` in your `.ini` file. The credentials are stored here rather than in the source config partly for security reasons and partly because of the way the extension is deployed. (It may make sense to move them to the source config in the future.)
4. Create a cron job like the following so that your harvest jobs will be marked `Finished` when complete:
 `0 * * * * paster --plugin=ckanext-harvest harvester run -c /srv/app/production.ini >> /var/log/cron.log 2>&1`

## <a name="harvesting"></a>Harvesting Sentinel products
To harvest Sentinel products, activate one or more of the following plugins:
1. `esa`, which creates harvesters that harvest from SciHub and NOA
2. `code_de`, which creates harvesters that harvest from CODE-DE

### <a name="scihub"></a>Harvesting from SciHub
Create a new harvest source and select `ESA Sentinel Harvester New`. The URL does not matter—the harvester only harvests from SciHub or NOA, depending on the configuration below.

In the configuration, place the following:
```
{
  "source": "scihub",
  "update_all": true,
  "start_date": "2018-01-16T10:30:00.000Z",
  "end_date": "2018-01-16T11:00:00.000Z"
}
```
`"source": "scihub"` instructs the harvester to harvest from SciHub (it could harvest from NOA instead).

If you will be running more than one Sentinel harvester, `"update_all"` must be `true` in order for this harvester to update datasets produced by the other harvester(s).

If you are developing or testing, `"start_date"` and `"end_date"` should also be included so that you can limit the harvester to specific time periods. Full datetimes are required (and are very useful, as you can choose to harvest only half an hour's worth of products, for example, in order to verify that the harvester works as expected without waiting for it to churn through an entire day's worth of products ).

Note: you must place your username and password in the `.ini` file as described above.

After saving the configuration, you can click Reharvest and the job will begin (assuming you have a cronjob like the one described above). Alternatively, you can use the paster command `run_test` described in the `ckanext-harvest` documentation to run the harvester without setting up the the gather consumer, etc.

### <a name="noa"></a>Harvesting from NOA
Create a new harvest source and select `ESA Sentinel Harvester New`. The URL does not matter—the harvester only harvests from SciHub or NOA, depending on the configuration below.

In the configuration, place the following:
```
{
  "source": "noa",
  "update_all": true,
  "start_date": "2018-01-16T10:30:00.000Z",
  "end_date": "2018-01-16T11:00:00.000Z"
}
```
`"source": "scihub"` instructs the harvester to harvest from NOA (it could harvest from SciHub instead).

If you will be running more than one Sentinel harvester, `"update_all"` must be `true` in order for this harvester to update datasets produced by the other harvester(s).

If you are developing or testing, `"start_date"` and `"end_date"` should also be included so that you can limit the harvester to specific time periods. Full datetimes are required (and are very useful, as you can choose to harvest only half an hour's worth of products, for example, in order to verify that the harvester works as expected without waiting for it to churn through an entire day's worth of products ).

Note: you must place your username and password in the `.ini` file as described above.

After saving the configuration, you can click Reharvest and the job will begin (assuming you have a cronjob like the one described above). Alternatively, you can use the paster command `run_test` described in the `ckanext-harvest` documentation to run the harvester without setting up the the gather consumer, etc.

### <a name="codede"></a>Harvesting from CODE-DE
Create a new harvest source and select `CODE-DE Harvester`. The URL does not matter—the harvester only harvests from CODE-DE.

In the configuration, place the following:
```
{
  "update_all": true,
  "start_date": "2018-01-16T10:30:00.000Z",
  "end_date": "2018-01-16T11:00:00.000Z"
}
```
If you will be running more than one Sentinel harvester, `"update_all"` must be `true` in order for this harvester to update datasets produced by the other harvester(s).

If you are developing or testing, `"start_date"` and `"end_date"` should also be included so that you can limit the harvester to specific time periods. Full datetimes are required (and are very useful, as you can choose to harvest only half an hour's worth of products, for example, in order to verify that the harvester works as expected without waiting for it to churn through an entire day's worth of products ).

After saving the configuration, you can click Reharvest and the job will begin (assuming you have a cronjob like the one described above). Alternatively, you can use the paster command `run_test` described in the `ckanext-harvest` documentation to run the harvester without setting up the the gather consumer, etc.

### <a name="multi"></a>Harvesting from more than one Sentinel source
To harvest from more than one Sentinel source, just create a harvester source for each Sentinel source and make sure that `"update_all"` is `"true"` in each configuration. For example, to harvest from all three sources:
1. Create a harvest source called (just a suggestion) "SciHub Harvester", select `ESA Sentinel Harvester New`, make sure that the configuration contains `"source": "scihub"` and `"update_all": true`, and (highly advised) enter a start and end time.
1. Create a harvest source called (just a suggestion) "NOA Harvester", select `ESA Sentinel Harvester New`, make sure that the configuration contains `"source": "noa"` and `"update_all": true`, and (highly advised) enter a start and end time.
1. Create a harvest source called (just a suggestion) "CODE-DE Harvester", select `CODE-DE Harvester`, make sure that the configuration contains `"update_all": true`, and (highly advised) enter a start and end time.

Then just run each of the harvesters. You can run them all at the same time. If a product has already been harvested by another harvester, then the other harvesters will only update the existing dataset, adding additional resources and metadata, but not overwriting the resources and metadata that already exist.

## <a name="alltogether"></a>How the three Sentinel harvesters work together
The three (really, two) Sentinel harvesters all inherit from the same base harvester classes. As mentioned above, the SciHub and NOA harvesters are the same. The `"source"` configuration is a switch that 1) causes the harvester to use a different base URL for querying the OpenSearch service and 2) changes the labels added to the resources. The CODE-DE harvester is a separate plugin because the OpenSearch service is different, but the only difference between it and the ESA plugin is the way the OpenSearch results are parsed. In all cases, the same methods are used for creating/updating the datasets.

The workflow for all the harvesters is:
1. Gather: query the OpenSearch service for all products within the specified date range, then page through the results, creating harvest objects for each entry. Each harvest object contains the content of the entry, which will be parsed later, as well as a preliminary test of whether the product already exists in CKAN or not.
2. Fetch: just returns true, as the OpenSearch service already provides all the content in the gather stage.
3. Import: parse the content of each harvest object and then either create a new dataset for the respective product, or update an existing dataset. It's possible that another harvester may have created a dataset for the product before the current import phase began, so if creating a dataset for a "new" product fails because the dataset already exists, the harvester catches the exception and performs an update instead. For the sake of simplicity, the create and update pipelines are the same. The only difference is the API call at the end. All three harvesters can run at the same time, harvesting from the same date range, without conflicts.

#### A note on datasets counts
The created/updated counts for each harvester job will be accurate. The count that appears in the sidebar on each harvester's page, however, will not be accurate. Besides issues with how Solr updates the `harvest_source_id` associated with each dataset, the fact that up to three harvesters may be creating or updating a single dataset means that only one harvest source can "own" a dataset at any given time. If you need to evaluate the performance of a harvester, use the job reports.

## <a name="develop"></a>Developing new harvesters
WORK IN PROGRESS

## <a name="itag"></a>iTag
WORK IN PROGRESS

## <a name="tests"></a>A note on tests
The current tests need to be updated and additional tests are necessary for maintaining consitency across all harvesters.
