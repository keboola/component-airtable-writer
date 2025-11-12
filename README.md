Airtable Writer
===============

Keboola component for writing data from Keboola Storage into Airtable tables.

**Table of Contents:**

[TOC]

Description
===========

The Airtable Writer component allows you to seamlessly write datasets from Keboola Storage into Airtable, a cloud platform that combines the simplicity of spreadsheets with the power of relational databases. Airtable is widely used for project management, content planning, CRM, and workflow automation thanks to its intuitive interface and flexible data structures.

By connecting Keboola with Airtable, this component enables synchronization of structured data into Airtable bases and tables, making it easier for teams to collaborate, visualize, and act on their data within Airtable's ecosystem.

Prerequisites
=============

- A valid Airtable account
- A Personal Access Token (PAT) with the following scopes:
  - `data.records:write` (required for writing data)
  - `schema.bases:write` (optional, needed only if creating new tables)

To generate a PAT token, visit your [Airtable account settings](https://airtable.com/create/tokens).

Features
========

| **Feature**             | **Description**                                                          |
|-------------------------|--------------------------------------------------------------------------|
| Row-Based Configuration | Configure multiple table writes in a single component instance.         |
| Dynamic Table Selection | Load and select bases and tables directly from Airtable via UI.         |
| Automatic Type Mapping  | Automatically maps Keboola column types to Airtable field types.        |
| Multiple Load Types     | Supports Full Load, Incremental Load (upsert), and Append modes.        |
| Schema Auto-creation    | Creates new tables in Airtable if they don't exist.                     |
| Column Auto-detection   | Loads columns from Storage tables with automatic type inference.        |
| Connection Testing      | Test your API token before configuring data writes.                     |

Load Types
==========

The component supports three load types:

**Append**
- Adds new records without checking for duplicates
- Fastest option for simple data additions

**Full Load**
- Deletes all existing records in the target table
- Reloads all data from scratch
- Ensures complete data refresh

**Incremental Load**
- Updates existing records using upsert keys
- Only modifies records that match the upsert key
- Adds new records that don't match existing keys

Configuration
=============

API Token
---------
Your Airtable Personal Access Token with appropriate scopes. This is stored securely as an encrypted parameter.

Base ID
-------
The unique identifier of the Airtable base where you want to write data. Use the "LOAD BASES" button to fetch available bases from your account.

Destination Table
-----------------
The name or ID of the target table in Airtable. You can:
- Select an existing table from the dropdown (loaded via "LOAD TABLES")
- Create a new table by typing a new name

Load Type
---------
Choose between:
- **Append**: Add new records without checking duplicates
- **Full Load**: Delete all records and reload from scratch
- **Incremental Load**: Update/insert records based on upsert keys

Columns
-------
Define the column mapping between your Keboola Storage table and Airtable:
- **Source Column**: Column name from your input table
- **Destination Column**: Target field name in Airtable
- **Upsert Key**: Check this for columns used to match records in Incremental Load mode
- **Airtable Field Type**: The data type for the field in Airtable

Use the "LOAD COLUMNS" button to automatically load columns from your input table with suggested Airtable types.

Supported Airtable Field Types
-------------------------------

The component supports writing to the following Airtable field types:
- singleLineText
- multilineText
- richText
- email
- url
- phoneNumber
- number
- percent
- currency
- rating
- checkbox
- singleSelect
- multipleSelects
- date
- dateTime
- barcode

**Note**: Calculated/formula fields and lookup fields are not writable and cannot be used as destinations.

Input
=====

The component expects exactly one input table mapped from Keboola Storage. This table should contain the data you want to write to Airtable.

Output
======

The component writes data directly to Airtable and does not produce output tables in Keboola Storage. A state file may be created to track the execution status.

Development
===========

If you want to develop this component locally:

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, initialize the workspace, and run the component using the following commands:

```bash
git clone https://github.com/keboola/component-airtable-writer component-airtable-writer
cd component-airtable-writer
docker-compose build
docker-compose run --rm dev
```

Run the test suite and perform lint checks using this command:

```bash
docker-compose run --rm test
```

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
