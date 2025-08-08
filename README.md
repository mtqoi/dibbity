# Dibbity - DBT Utility Tool

Tool to take some of the edge off using dbt.

Also I am trying to learn Go.


## Features

- Dry run DBT models
- List and compile DBT models

### Basic Commands


## TODO:
- refactor to use cobra bindings
- create options struct to pass to dbt & bq
- handle bq output + calculate costs
- pretty print output

## Other ideas for commands:
- return the file for a specific model (useful for piping)
- open bq in browser set to the specific model input
- open github in browser & go to the specific model file
- find columns from model?
- grab just the models that have been modified recently (git diff vs main) and then compile / run them
- add defer flags
- add --no-populate-cache flag
- compile sql & send to clipboard 
- better auditing?
- run model with `LIMIT 100` and print output? With flags for cost?
- commit history for specific model
- find specific model doc?
- fun with DAG visualisation?
- pull all data for model through locally (e.g. to populate a duckdb database)

Long term:
- better interactions with LookML e.g. get difference between `view.lkml` fields and dbt model fields
- syncing descriptions
