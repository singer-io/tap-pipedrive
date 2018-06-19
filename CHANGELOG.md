# Changelog

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
