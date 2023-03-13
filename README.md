# Nanbi


>*Nanbiquara*: speech of smart people, of clever people
>- Translated from the [Tupi Guarani Illustrated Dictionary](https://www.dicionariotupiguarani.com.br/dicionario/nanbiquara/)

Nanbi is a framework that allows you to define data transformations in a composable way, agnostic of data processing engine (Pandas,  mySQL, Spark etc).
- Its syntax is *sql-like*, inspired by PySpark and Scala-Spark approaches
- It allows you to define a set of data transformations in a more composable way than SQL, for example, allowing for better readability specially on complex queries
- It allows you to execute your data transformations definitions in multiple engines (Pandas, mySQL, Spark etc) without having to change the data transformation definition

>Nanbi is right now under the initial stages of development. It's not fully ready for a version 1. So far, there is no compatibility with engines other than Pandas.
>
>Please get in touch if you have interest in using Nanbi on your work or personal project. Feature requests are welcome.


## Setup

>While the library isn't published in PyPI
1. Clone the repo
2. Create a symlink to the repo
- TODO(eemery): Add installation details once package gets published in PyPI

## Getting Started

### 1. Creating a DataFrame

Nanbi uses the concept of a `DataFrame` to represent a table and its annotations (or metadata). Currently, Nanbi supports the creation of DataFrames from Pandas DataFrames and CSV files (using Pandas behind the scenes).

**From a Pandas DataFrame**

```python
import pandas as pd
import nanbi.connectors.pandas as nb

pandas_df = pd.DataFrame({"num_a": [10, 50, 20, 50, 20],
                          "num_b": [41, 51, 21, 31, 11]})

df = nb.from_data_frame(pandas_df)
```

**From a CSV file (with Pandas)**

```python
import nanbi.connectors.pandas as nb

df = nb.from_csv("path/to/my-file.csv")
```

**Viewing your imported data**

To visualize the imported or created data, just use the `.display()` method:

```python
import nanbi.connectors.pandas as nb

df = nb.from_csv("path/to/my-file.csv")

df.display()
```

The output will be a Pandas DataFrame, for example:

```
  col_a col_b
0 50    51
1 50    31
2 20    21
3 20    11
4 10    51
```

### 2. Enriching tables (`.with_columns()`)

Nanbi goal is to allow you to define data transformations to enrich your table with derived data in a composable way. One of the main ways that you can achieve this, is by the use of the `.with_column()` method. It creates a new column in your table according to the transformation formula you gave it. For example:

```python
import nanbi.connectors.pandas as nb

df = nb.from_csv("path/to/my-file.csv")

enriched_df = df.with_column("result", col("col_a") + col("col_a"))

enriched_df.display()
```

The output will be a Pandas DataFrame in the form of:

```
  col_a col_b result
0 50    51    101
1 50    31    81
2 20    21    41
3 20    11    31
4 10    51    61
```

#### Chaining Transformations

One improvement that we can make to the code above is to take advantage of chaining transformations. We could have written the above code like:

```python
import nanbi.connectors.pandas as nb

df = nb.from_csv("path/to/my-file.csv")
       .with_column("result", col("col_a") + col("col_a"))

df.display()
```

#### Improving Transformations Readability and Reusability

Another improvement that can be done, specially when transformations get complex, is to move the formula definition (i.e., `col("col_a") + col("col_a")`) to its own variable. In the code above, this would look like:

```python
import nanbi.connectors.pandas as nb

my_complex_formula = col("col_a") + col("col_a")

df = nb.from_csv("path/to/my-file.csv")
       .with_column("result", my_complex_formula)

df.display()
```
