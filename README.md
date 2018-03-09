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
4. [Developing new harvesters](#develop)
    1. [The basic harvester workflow](#basicworkflow)
        1. [gather_stage](#gather_stage)
        2. [fetch_stage](#fetch_stage)
        3. [import_stage](#import_stage)
5. [iTag](#itag)
    1. [How ITagEnricher works](#itagprocess)
    2. [Setting up ITagEnricher](#setupitag)
    3. [Handling iTag errors](#handlingitagerrors)
6. [A note on tests](#tests)
7. [Suggested cron jobs](#cron)

## <a name="repo"></a>What's in the repository
The repository contains three plugins:
1. `nextgeossharvest`, the base CKAN plugin
2. `esa`, a harvester plugin for harvesting Sentinel datasets from SciHub, NOA, and CODE-DE via their DHuS interfaces
3. `itag` a harvester plugin for adding additional tags and metadata to datasets that have already been harvested (more on this later)

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
To harvest Sentinel products, activate the `esa` plugin, which you will use to create a harvester that harvest from SciHub, NOA or CODE-DE. To harvest from more than one of those sources, just create more than one harvester and point it at a different source.

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
5. `datasets_per_job`: (optional, integer, defaults to 1000) determines the maximum number of products that will be harvested during each job. If a query returns 2,501 results, only the first 1000 will be harvested if you're using the default. This is useful for running the harvester via recurring jobs intended to harvest products incrementally (i.e., you want to start from the beginning and harvest all available products). The harvester will harvest products in groups of 1000, rather than attmepting to harvest all x-hundred-thousand at once. You'll get feedback after each job, so you'll know if there are errors without waiting for the whole job to run. And the harvester will automatically resume from the harvested dataset if you're running it via a recurring cron job.
6. `timeout`: (optional, integer, defaults to 4) determines the number of seconds to wait before timing out a request.
7. `skip_raw`: (optional, boolean, defaults to false) determines whether RAW products are skipped or included in the harvest.

Example configuration with all variables present:
```
{
  "source": "esa_scihub",
  "update_all": false,
  "start_date": "2018-01-16T10:30:00.000Z",
  "end_date": "2018-01-16T11:00:00.000Z",
  "datasets_per_job": 1000,
  "timeout": 4,
  "skip_raw": true
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

## <a name="itag"></a>iTag
The iTag "harvester (`ITageEnricher`) is better described as a metaharvester. It uses the harvester infrastructure to add new tags and metadata to existing datasets. It is completely separate from the other harvesters, meaning: if you want to harvest Sentinel products, you'll use one of the Sentinel harvesters. If you want to enrich Sentinel datasets, you'll use an instance of `ITagEnricher`. But you'll use them separately, and they won't interact with eachother at all.

### <a name="itagprocess"></a>How ITageEnricher works
During the gather stage, it queries the CKAN instance itself to get a list of existing datasets that 1) have the `spatial` extra and 2) have not yet been updated by the ITageEnricher. Based on this list, it then creates harvest objects. This stage might be described as self-harvesting.

During the fetch stage, it queries an iTag instance using the coordinates from each dataset's `spatial` extra and then stores the response from iTag as `.content`, which will be used in the import stage. As long as iTag returns a valid response, the dataset moves on to the import stage—in other words, all that matters is that the query succeeded, not whether the iTag was able to find tags for a particular footprint. See below for an explanation.

During the import stage, it parses the iTag response to extract any additional tags and/or metadata. Regardless of whether any additional tags or metadata are found, the extra `itag: tagged` will be added to the dataset. This extra is used in the gather stage to filter out datasets for which successful iTag queries have been made.

### <a name="setupitag"></a> Setting up ITagEnricher
To set it up, create a new harvester source (we'll call ours "iTag Enricher" for the sake of example). No configuration is necessary. Select `manual` for the update frequency. Select an organization (currently required—the metaharvester will only act on datasets that belong to that organization).

Once you've created the harvester source, create the cron job below, using the name or ID of the source you just created:
`* * * * * paster --plugin=ckanext-harvest harvester job {name or id of harvest source} -c {path to CKAN config}`
The cron job will continually attempt to create a new harvest job. If there already is a running job for the source, the attempt will simply fail (this is the intended behaviour). If there is no running job, then a new job will be created, which will then be run by the `harvester run` cron job that you should already have set up. The metaharvester will then make a list of all the datasets that should be enriched with iTag, but which have not yet been enriched, and then try to enrich them.

### <a name="handlingitagerrors"></a>Handling iTag errors
If a query to iTag fails, 1) it will be reported in the error report for the respective job and 2) the metaharvester will automatically try to enrich that dataset the next time it runs. No additional logs or tracking are required--as long as a dataset hasn't been tagged, and should be tagged, it will be added to the list each time a job is created. Once a dataset has been tagged (or it has been determined that there are no tags that can be added to it), it will no longer appear on the list of datasets that should be tagged.

Currently, ITagEnricher only creates a list of max. 1,000 datasets for each job. This limit is intended to speed up the rate at which jobs are completed (and feedback on performance is available). Since a new job will be created as soon as the current one is marked `Finished`, this behaviour does not slow down the pace of tagging.

Sentinel-3 datasets have complex polygons that seem to cause iTag to timeout more often than it does when processesing requests related to other datasets, so Sentinel-3 datasets are currently filtered out of the list of datasets that need to be tagged.

In general, requests to iTag seem to timeout rather often, so it may be necessary to experiment with rate limiting. It may also be necessary to set up a more robust infrastructure for the iTag instance.

## <a name="tests"></a>A note on tests
The current tests need to be updated and additional tests are necessary for maintaining consitency across all harvesters.


## <a name="cron"></a>Suggested cron jobs
```
* * * * * paster --plugin=ckanext-harvest harvester run -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job itag-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job code-de-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job noa-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
* * * * * paster --plugin=ckanext-harvest harvester job scihub-sentinel -c /srv/app/production.ini >> /var/log/cron.log 2>&1
```