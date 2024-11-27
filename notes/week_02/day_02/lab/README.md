# The blurry line between fact and dimension

> Facts like depends on an event, dimension more like the attribute

## Is it a fact or a dimension

- Did a user log in today
    - The log in event would be a fact that informs the `dim_is_active` dimension
    - VS the state `dim_is_activated` which is something that is state-driven, not activity driven

- You can aggregate facts and turn them into dimensions
    - Is this person a _high engager_? A _low engager_?
        - Think of `scoring_class` from Week 1
    - `CASE WHEN` to bucketize aggregated facts can be very useful to reduce the cardinality (Cardinality đề cập đến số lượng giá trị khác nhau trong một cột)

> 5 to 10 diff values is sweet to bucketize (for normalize distribution)

## Properties of Facts vs Dimensions

- Dimensions
    - Usually show up in `GROUP BY` when doing analytics
    - Can be "high cardinality" or "low cardinality" depending
    - Generally come from a snapshot of state

- Facts
    - Usually aggregated when doing analytics by things like `SUM`, `AVG`, `COUNT`
    - Almost always higher volume than dimensions, although some fact sources are low-volume, think "rare events"
    - Generally come from events and logs

- Airbnb example
    - Is there price of a night on Airbnb a fact or a dimension?
    - The host can set the price which sounds like an event
    - It can easily be `SUM`, `AVG`, `COUNT` like regular facts
    - Prices on Airbnb are doubles, therefore extremely high cardinality
    - The fact in this case would  be the host changing the setting that impacted the price - that is a **state**
    - Think a fact has to be logged, a dimension comes from the state of things
    - Price being derived from settings is a **dimension**

## Dimensions based on Fact

**Boolean (Facts) / Existence-based (Dimensions)**

- `dim_is_active`, `dim_bought_something`, ...: There are usually on the daily/hour grain too
- `dim_has_ever_booked`, `dim_ever_active`, `dim_ever_labeled_fake`
    - These "ever" dimensions look to see if there has "ever" been a log and once it flips one way, it never goes back
    - Interesting, simple and powerful features for machine learning
        - An Airbnb host with active listings who has never been booked: Looks sketchier and sketchier over time
- "Days since" dimensions
    - Very common in Retention analytical patterns
    - Look up J curves for more details on this

## Categorical Fact/Dimensions

- Scoring class in Week 1
    - A dimension is derived from fact data
- Often calculated  with CASE WHEN logic and "bucketizing"
    - Airbnb superhost

## Should you use dimensions or facts to analyze users

- Is the `dim_is_actived` state or `dim_is_active` logs a better metric
    - It depends
- It's the difference between "signups" and "growth" in some perspectives

## The Extremely efficient Date List data structure

- Extremely efficient way to manage user growth
- Imagine a cumulated schema like `users_cumulated`
    - User_id
    - Date
    - Dates_active - an array of all the recent days that a user was active
- You can turn that into a structure like this
    
    | user_id | date         | datelist_int      |
    |---------|--------------|-------------------|
    | 1       | 2023-01-01   | 100000010000001   |
    
    The 1s in the datelist_int represent the activity for 2023-01-01 - bit_position (zero indexed) -> 2023-01-01; 2022-12-24; 2022-12-17


