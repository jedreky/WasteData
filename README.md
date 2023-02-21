## Dataset

### General

I have analysed the following dataset: https://data.defra.gov.uk/Waste/201213_england_wastedata.csv
(see https://data.europa.eu/data/datasets/waste-data-flow?locale=en for more details).

This is a CSV file, but the data does not have a rigid SQL structure. Even if we focus on the main part of the dataset (exclude the first 11 lines which are different), it is clear that it is more natural to store this data in some form of a nested structure. That is why I decided to use MongoDB.

### Data exploration and ingestion

The first 11 lines contain some metadata of the dataset, which we extract and store in the metadata collection.

Starting at line 15 we have the actual data about waste collection. The rows seem to represent a questionnaire regarding waste collection which is filled by every district for every quarter. In the entire dataset there are 42 different questions to which we have at least 1 reply. Sometimes a reply correspond to a single row in CSV (e.g.: `Q015: Total no. of Civic Amenity Sites operated by LA or its contractors`), but more often the reply occupies several rows. By looking at each question and the corresponding records we deduce that:
- the first two questions (`Q001`, `Q002`) contain some static data about the district,
- questions `Q004` through `Q035` contain details about various types of waste collection, its destination, number of households participating in the scheme, etc,
- questions `Q051` through `Q065` and `Q070` contain info about the total amount of waste disposed in various manners,
- question `Q069` concerns the ratio between household and non-household waste.

In this task we are only interested in the percentage of waste that ends up in some form of landfill, so we could restrict our attention just to questions Q051 through Q065 (Q070 should be a sum of some subset of those). However, as at some later point we might want to use other parts of the data, it makes sense to load the data from other questions too.

Let us briefly look at the remaining columns: columns `RowOrder` through `columngroup` seem to carry no relevant information. Column `MaterialGroup` whenever defined seems to be a function of `RowText` (indeed, I've checked that it is a less granular classification of waste), so let us store this relationship in the metadata collection.

I have written a script that ingests all the data and creates a nested structure of lists and dictionaries. The structure was designed to ensure that we can easily access the entire questionnaire corresponding to a specific district in a specific period (year and quarter). The nesting level depends on the fields present in every row. I have looked at every type of row (determined by which fields are present in the CSV file) and I think I have extracted all the information that might be relevant. This leads to a complicated `if-elif` logic in the `list_to_dict` function, which is not particularly elegant, but is necessary to deal with this kind of data.

### Reporting

By looking at the different questions, I have reached the conclusion that in order to find information about the total amount landfill/non-landfill waste produced by a specific district in a specific period of time, it should be sufficient to look at questions `Q051` through `Q065`. Questions `Q051` through `Q053` concern different types of landfill, while the remaining types of questions concern other ways of waste disposal (but they might still contribute to the total landfill amount as waste sent to some destination can be then rejected to landfill). So the reports I generate are based entirely on these questions.

For a specific district and period I would not expect all these summary questions to have answers, but I would expect at least 3-4 answers per questionnaire (the waste cannot all end up in a single destination, there must be some variety). Given that out of 1408 questionnaires there are 480 with at least 4 answers in the summary section, I would estimate that this script gives somewhat reasonable results in ~35% of the cases.

To treat the remaining cases one would have to look into the remaining fields. Ideally, one would look at some complete questionnaires to determine how these summary fields are computed and perform analogous computation for the other questionnaires. However, this felt beyond the scope of this assignment.

## How to install and run

This procedure has been tested on `Python 3.10.6`.

To install requirements:

```commandline
pip install -r -requirements
```

To start MongoDB container:

```commandline
docker-compose up
```

Note that in the current all the data will be lost once the container is gone, in order to go for persistent storage would have to mount some volume. 

To ingest data:

```commandline
python -m src.data_loader
```

The input CSV files is about ~50 MB and the ingestion process should take ~20 seconds.

To generate reports:

```commandline
python -m src.report_generator
```

Generating a single report takes a fraction of a second, generating all 1408 reports takes about 10 seconds.