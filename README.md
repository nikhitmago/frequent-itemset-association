# Market Basket Analysis

- Implementation of SON algorithm that leverages Apriori to find frequent items bought together in a supermarket
- Developed the algorithm in a truly 'Big Data' environment using Spark
- SON algorithm uses MapReduce in two phases
- Algorithm implemented for different support thresholds and data sizes (1KB - 500MB)

__Execution code for small data size:__

```html
bin/spark-submit Nikhit_Mago_SON.py [case] [......../Small2.csv] [support]
```

__Execution code for medium data size:__

```html
bin/spark-submit Nikhit_Mago_SON.py [case] [......../MovieLens.Small.csv] [support]
```

__Execution code for large data size:__

```html
bin/spark-submit Nikhit_Mago_SON.py [case] [......../MovieLens.Big.csv] [support]
```

__Notes:__
- Please use `Python 2.7` and `Spark 2.2.1` to execute the PySpark script
- Data Source is provided [here](https://grouplens.org/datasets/movielens/)
- MovieLens.Big.csv is the ratings.csv file from ml-20m dataset
- MovieLens.Small.csv is the ratings.csv file from ml-latest-small dataset
- Case 1 is for combinations of frequent movies (as singletons, pairs, triples, etc. . . ) that were rated and are qualified as frequent given a support threshold value.
- Case 2 is for combinations of frequent users (as singletons, pairs, triples, etc. . . ) that were rated and are qualified as frequent given a support threshold value.

__Execution Table for Large Dataset__:

__Case 1:__

| Support Threshold  | Execution Time (s) |
| ------------- | ------------- |
| 30000  | ~1110  |
| 35000  | ~365  |

__Case 2:__

| Support Threshold  | Execution Time (s) |
| ------------- | ------------- |
| 2800  | ~1400  |
| 3000  | ~850 |
