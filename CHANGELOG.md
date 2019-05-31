# Changelog

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
