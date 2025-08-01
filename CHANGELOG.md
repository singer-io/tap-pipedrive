# Changelog

# 1.3.1
  * Fix Nonetype and datetime conversion error  [#146](https://github.com/singer-io/tap-pipedrive/pull/146)

# 1.3.0
  * Upgrade api version to v2 below streams [#144](https://github.com/singer-io/tap-pipedrive/pull/144)
    * activities
    * deals
    * organizations
    * persons
    * pipelines
    * products
    * stages
  * Replace /recent endpoints with respective /list endpoints to sync the historical data
  * Update replication method to INCREMENTAL where ever it is possible

# 1.2.4
  * Fix the organizations schema  [#142](https://github.com/singer-io/tap-pipedrive/pull/142)

# 1.2.3
  * Fix Dependabot issue  [#140](https://github.com/singer-io/tap-pipedrive/pull/140)

# 1.2.1
  * Adds backoff/retry for Http5xx errors [#115](https://github.com/singer-io/tap-pipedrive/pull/115)

# 1.2.0
  * Adds `deal_fields` stream [#134](https://github.com/singer-io/tap-pipedrive/pull/134)

# 1.1.8
  * Retries requests with 200 status and null bodies for dealsflow stream [#131](https://github.com/singer-io/tap-pipedrive/pull/131)

# 1.1.7
  * Reverts state change from 1.1.6 due to child streams using parent stream pagination [#130](https://github.com/singer-io/tap-pipedrive/pull/130)

## 1.1.6
  * Makes several fields in `notes` nullable
  * Writes state during pagination for more efficient extraction
  * [#128](https://github.com/singer-io/tap-pipedrive/pull/128)

## 1.1.5
  * Allows a deal_products record to contain a null `product_id` value per Pipedrive's [hard-deletion policy](https://developers.pipedrive.com/changelog/post/permanent-deletion-logic-for-6-core-entities) [#126](https://github.com/singer-io/tap-pipedrive/pull/126)

## 1.1.4
  * Fixed `NoneType` and Transformstion issues for Anamolous Failed Jobs [#120](https://github.com/singer-io/tap-pipedrive/pull/120)

## 1.1.3
  * Access `users` data using new structure, rather than indexing into a single `list` [#119](https://github.com/singer-io/tap-pipedrive/pull/119)

## 1.1.2
  * Adding missing fields [#111](https://github.com/singer-io/tap-pipedrive/pull/111)
  * Add missing tap-tester cases [#113](https://github.com/singer-io/tap-pipedrive/pull/113)
  * Fix bookmark strategy [#117](https://github.com/singer-io/tap-pipedrive/pull/117)

## 1.1.1
  * Request timeout implemented [#105](https://github.com/singer-io/tap-pipedrive/pull/105)

## 1.1.0
  * Added retry mechanism for http error codes [#93](https://github.com/singer-io/tap-pipedrive/pull/93)
  * Removed support for delete_logs stream [#97](https://github.com/singer-io/tap-pipedrive/pull/97)
  * Fixed organizations missing fields [#94](https://github.com/singer-io/tap-pipedrive/pull/94)

## 1.0.6
  * Use anyOf schema for a date-time field in the deals schema [#71](https://github.com/singer-io/tap-pipedrive/pull/71)

## 1.0.5
 * Handles null `add_time` for deals when retrieving IDs [#67](https://github.com/singer-io/tap-pipedrive/pull/67)

## 1.0.4
 * Make bookmark property automatic [commit](https://github.com/singer-io/tap-pipedrive/commit/1390c9c36491c80ffc0f89b4efc25500412d16f1)

## 1.0.3
  * Fix deal id pagination [#53](https://github.com/singer-io/tap-pipedrive/pull/53)

## 1.0.1
  * Discover catalog when none was passed in sync mode [#52](https://github.com/singer-io/tap-pipedrive/pull/52)

## 1.0.0
  * Add stream/field selection
  * Add `deal_products` endpoint
  * [#51](https://github.com/singer-io/tap-pipedrive/pull/51)

## 0.2.4
  * Allows a file's user_id field to be nullable [#49](https://github.com/singer-io/tap-pipedrive/pull/49)

## 0.2.3
  * Removes the `required` elements from JSON schemas as they are unnecessary [#45](https://github.com/singer-io/tap-pipedrive/pull/45)

## 0.2.2
  * Fixed missing deals in dealflow stream

## 0.2.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.2.0
  * Remove `goals` stream [#44](https://github.com/singer-io/tap-pipedrive/pull/44)

## 0.1.1
  * Add bookmarking to the dealflow stream [#38](https://github.com/singer-io/tap-pipedrive/pull/38)

## 0.1.0
  * Add dealflow stream [#36](https://github.com/singer-io/tap-pipedrive/pull/36)

## 0.0.15
  * Additional schema updates to allow nulls [#26](https://github.com/singer-io/tap-pipedrive/pull/26)

## 0.0.14
  * Marks the products.json schema file to allow a null USD.cost [#24](https://github.com/singer-io/tap-pipedrive/pull/24)

## 0.0.13
  * Marks `timeline_last_activity_time` and `timeline_last_activity_time_by_owner` as not required in **organizations** and **persons** streams

## 0.0.12
  * Includes subdirectories explicitly in package to match current deployment method

## 0.0.11
  * Changes the users stream's `last_login` schema to accept string as a fallback for invalid datetimes
