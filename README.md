
# AI Feature Hub Billing and Usage Tracking

This project provides a set of SQL scripts for managing billing and usage tracking for an AI-powered application. It is designed to be used with a Snowflake database.

## Project Structure

The project is organized into the following directories:

* `sql/ddl`: Contains Data Definition Language (DDL) scripts for creating tables and views.
* `sql/ops`: Contains operational scripts for tasks such as creating streams, applying rates, running billing, and seeding data.
* `sql/tasks`: Contains scripts for creating tasks, such as aggregating usage data.
* `sql/register`: Contains scripts for registering procedures.

## Features

* **Rate Card Management:** Manage rate cards for different features.
* **Usage Tracking:** Track feature usage for each tenant.
* **Billing:** Generate invoices and line items based on usage and rate cards.
* **Data Aggregation:** Aggregate usage data for reporting and analysis.
* **Invoice Archiving:** Archive invoices for historical purposes.
* **Evidence Bundles:** Export evidence bundles for auditing and compliance.

## Getting Started

To use these scripts, you will need a Snowflake account with the necessary permissions. You can execute the scripts using the Snowflake UI or a command-line tool like `snowsql`.

For example, to seed the database with demo data, you can run the following command:

```
snowsql -f sql/ops/300g_014_seeding_demo_min.sql
```
