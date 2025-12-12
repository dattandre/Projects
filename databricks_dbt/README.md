# Welcome to your SQL take home test

## Introduction

This assessment will test your ability to parse through raw data and create SQL models to manage and transform the data. Open the GOALS.md file for more specific criteria, but in general you will be expected to be able to:

1) Outputs target tables ( to be specified at another time )
2) Verifies data quality and integrity
3) Runs efficiently and cleanly, even with new data added later on

The assessment uses a combination of DuckDB and dbt to orchestrate a facsimile of a data pipeline. There is more information on getting started with these tools below.

## Getting Started

###  *(Optional) Set up a virtual environment before you get started*

### 1. Install DuckDB

Using your preferred package manager (brew, pipx, etc) install [DuckDB](https://duckdb.org/docs/installation/?version=stable&environment=cli&download_method=direct)

DuckDB is a local SQL database that can run queries quickly and supports most modern advanced SQL syntax (eg 'qualify')

### 2. Install dbt

The [dbt-duckdb](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup) adapter is relatively quick and easy to set up. 

From your python environment, run the command:

`python -m pip install dbt-core dbt-duckdb`

### 3. Exploring the project

There is a Justfile that includes some basic commands you can use to get a sense for how the project works. Either copy and run the commands in your console, or (with Just installed, use `just <command>` to test it out).

You can explore the raw data with SQL by entering the duckdb shell with the command:
```duckdb takehome.duckdb```

If you accidentally delete or modify the raw tables, you can recreate them by executing the SQL script in the src data file. From the duckdb shell use the command:
```.read 'src_data/create_src_tables.sql'```

### 4. Running some basic dbt commants to get a feel for the technology
Examples:
```
dbt run
dbt build
dbt test -s stg_patients
```

### 5. Copy the pattern in the stg_patients model to create staging versions of each raw table

### 6. Flesh out the data model, completing (at minimum) the models under the 'marts' folder


## FAQ

### 1. I've never used dbt before

It's okay. Explore the docs at [getdbt.com](https://docs.getdbt.com/) if you have questions, but for the most part this is a test of your ability to write SQL and handle data transformations. There are some starter SQL models you can use as templates. The most important parts are using '{{ source(...)}}' and '{{ ref(..) }}' jinja macros to refer to other tables in your database.

To test your project you can run the command ```dbt run```
To only run specific models you can run the command ```dbt run --select {model1} {model2}```

Look in the dbt docs section on 'graph operators' for more details on how to select specific models.

### 2. I've never used duckdb before

It's just a SQL database. The syntax is closer to Postgres, but they also have good documentation [available online](https://duckdb.org/docs/stable/).

### 3. I can't get the package or dependencies to work.

No fear. 'brew install poetry' if you're on a Mac and 'apt install poetry' on Linux (honestly if you're on Linux you definitely know how to install packages) and then 'poetry shell/poetry install' in the root directory should get you set up. If you're having trouble with that, the two packages you really need to have are:
* duckdb
* dbt-duckdb  ( Note the specific name! )

If you just can't get anything to work no matter how hard you try, spend your time focusing on writing the SQL models and tables instead.
