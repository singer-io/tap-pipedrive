# tap-pipedrive

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Pipedrive's [REST API](https://developers.pipedrive.com/docs/api/v1/)
- Extracts the following resources from Pipedrive
  - [Currencies](https://developers.pipedrive.com/docs/api/v1/#!/Currencies)
  - [ActivityTypes](https://developers.pipedrive.com/docs/api/v1/#!/ActivityTypes)
  - [Filters](https://developers.pipedrive.com/docs/api/v1/#!/Filters)
  - [Stages](https://developers.pipedrive.com/docs/api/v1/#!/Stages)
  - [Pipelines](https://developers.pipedrive.com/docs/api/v1/#!/Pipelines)
  - [Goals](https://developers.pipedrive.com/docs/api/v1/#!/Goals)
  - [Recent Notes](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Users](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Activities](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Deals](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Files](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Organizations](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Persons](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
  - [Recent Products](https://developers.pipedrive.com/docs/api/v1/#!/Recents)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


---

Copyright &copy; 2017 Stitch
