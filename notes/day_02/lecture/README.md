# Data Modeling - Slowly Changing Dimensions and Idempotency

## Idempotent pipelines

- Your pipeline produces the same results regardless of when it's ran
- Idempotent: Denoting an element of a set which is unchanged in value when multiplied or otherwise operated on by itself

        --this is idempotent
        UPDATE users SET status = 'active' WHERE id = 1;
        --run this nultiple times still 1 result

- Pipelines should produce the same results regardless of the day you run it, how many times you run it, the hour that you run it

- Why is troubleshooting non-idempotent pipelines hard
    - Silent failure
    - You only see it when you get data inconsistencies (non-reproducable) and a data analyst yells at you

- What can make a pipeline not idempotent
    - `INSERT INTO` without `TRUNCATE`
        - Use `MERGE` or `INSERT OVERWRITE` every time please
    - Using `start_date >` without a corresponding `end_date <`
    - Not using a full set of partition sensors
    - Not using `depends_on_past` for cumulative pipelines
    - Relying on the "latest" partition of a not properly modeled SCD table
    - Relying on the "latest" partition of anything else
    > important: quality >> latency

## Slowly Changing Dimensions

- Most dimensions are slowly changing
- The slower their changing, the better

- 3 options to model the dimension:
    - Latest snapshot -> The pipeline is non-idempotent: The backfill is wrong because u just have only the current data
    - Daily/Monthly/Yearly snapshot
    - SCD: Collapsing daily snapshots 

- How can you model dimensions that change
    - Singular snapshot (like latest snapshot): not idempotent because when u backfill the data, u only have the latest value then all dimensional values of your old data would pull that latest value, which migh not be correct for the older data. For the most part, **Never ever do this**
    - Daily partitioned snapshots
    - SCD Types 1, 2, 3

### SCD type 0 - idempotent
- Aren't actually slowly changing (e.g. birth date) - will never change

### SCD type 1 - non-idempotent
- You only care about the latest value
- NEVER USE THIS TYPE BECAUSE IT MAKES PIPELINES NOT IDEMPOTENT
- Example:
    - Before

    | **Customer_ID** | **Customer_Name**  | **City**     |
    |-----------------|--------------------|--------------|
    | 1               | John Doe           | New York     |
    | 2               | Jane Smith         | Chicago      |
    
    - After

    | **Customer_ID** | **Customer_Name**  | **City**     |
    |-----------------|--------------------|--------------|
    | 1               | John Doe           | Los Angeles  |
    | 2               | Jane Smith         | Chicago      |


### SCD type 2 - idempotent
- You care about what the value was from "start_date" to "end_date"
- Current values usually have either an end_date that is:
    - NULL
    - Far into the future like 9999-12-31
- Hard to use since there's more than 1 row per dimension, you need to be careful about filtering on time
- This is the only type of SCD that is purely IDEMPOTENT

| **Customer_ID** | **Customer_Name**  | **City**         | **Effective_Start_Date**  | **Effective_End_Date**  | **Current_Flag** |
|-----------------|--------------------|------------------|---------------------------|-------------------------|------------------|
| 1               | John Doe           | New York         | 2023-01-01                | 2023-06-30              | 0                |
| 1               | John Doe           | Los Angeles      | 2023-07-01                | 2023-12-31              | 0                |
| 1               | John Doe           | San Francisco    | 2024-01-01                | NULL                    | 1                |
| 2               | Jane Smith         | Chicago          | 2023-02-01                | NULL                    | 1                |

### SCD type 3 - non-idempotent
- Only care about "original" and "current" values
- Benefits: You only have 1 row per dimension
- Drawbacks: Lose the history in between original and current

- Example
    - Before

    | **Customer_ID** | **Customer_Name**  | **Current_City** | **Previous_City** |
    |-----------------|--------------------|------------------|-------------------|
    | 1               | John Doe           | New York         | NULL              |
    | 2               | Jane Smith         | Chicago          | NULL              |
    - After

    | **Customer_ID** | **Customer_Name**  | **Current_City** | **Previous_City** |
    |-----------------|--------------------|------------------|-------------------|
    | 1               | John Doe           | Los Angeles      | New York          |
    | 2               | Jane Smith         | Chicago          | NULL              |

> U should only care about type 0 and 2

## SCD2 Loading

- Load the entire history in one query: Inefficient but nimble
- Incrementally load the data after the previous SCD is generated
    - Has the same "depends_on_past" constraint
    - Efficient but cumbersome
