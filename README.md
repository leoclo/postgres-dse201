# postgres-dse201

---

# Introduction

Project in postgres for the DSE 201 course


## Local Enviroment Setup with Docker
___

To setup app local enviroment install [docker](https://docs.docker.com/get-docker/)
and run the following command on the root directory of the project, this will create
containers for mysql and phpmyadmin:

```bash
docker-compose -f app.yaml up
```

## Local Installation
___

Clone the repository or download the .zip file and extract it on the desired directory


Create a virtual environment with the command:

```bash
python -m venv venv
```

Activate the created enviroment:

```bash
source venv/bin/activate
```

## Creating dbs

```bash
 python db/build.py settings.json

 ```

## M2

```bash
 python db/m2/cats.py settings.json

 ```
```bash
 python db/m2/sales_cube.py settings.json
 ```

## Tip
___

For database modeling use [sqldbm](https://app.sqldbm.com/)

