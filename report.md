## Intro
In this document, I answer the questions which are part of the assignment.

## Specific DataFrame Queries

I first loaded the data using:

```py
bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json("/datasets/yelp/review.json")
us = spark.read.json("/datasets/yelp/user.json")
```

I then ran the following queries:

```py
def q1(bs):
    """
    Analyze business.json to find the total number of reviews for all businesses.
    The output should be in the form of a Spark DataFrame with one value representing the count.
    """

    return bs.agg({'review_count': 'sum'})

def q2(bs):
    """
    Analyze business.json to find all businesses that have received 5 stars and that have
    been reviewed by 1000 or more users. The output should be in the form of DataFrame of
    (name, stars, review count) 
    """

    return bs.filter((bs.stars == 5) & (bs.review_count >= 1000)).select("name", "stars", "review_count")

def q3(us):
    """
    Analyze user.json to find the influencers who have written more than 1000 reviews. The
    output should be in the form of DataFrame of user id.
    """
    
    return us.filter(us.review_count > 1000).select("user_id")


def q4(bs, rs, a3):
    """
    Analyze review.json, business.json, and a view created from your answer to Q3 to
    find the businesses that have been reviewed by more than 5 influencer users.
    """

    # Join business, reviews and corresponding influencers using inner join
    bs_rs = bs.join(rs, "business_id").join(a3, "user_id")

    # For each business, count number of unique influencers
    bs_inf_count = bs_rs.groupby("business_id").agg(countDistinct("user_id").alias("countd"))

    
    return bs_inf_count.filter(bs_inf_count.countd > 5)

def q5(rs, us):
    """
    Analyze review.json and user.json to find an ordered list of users based on the average star counts they have given in all their reviews.
    """
    
    rs_us = rs.join(us, "user_id").groupby("user_id").mean("stars").sort("avg(stars)")
```

## Authenticity Study

### Data exploration
First, here is the code used for this phase for reference:

```py
def eda(bs, rs):
    """
    What is the percentage of reviews containing a variant of the word "authentic"?
    How many reviews contain the string "legitimate" grouped by restaurant categories?
    How many reviews contain the string "legitimate" grouped by businesses type (type of cuisine)?
    Is there a difference in the amount of authenticity language used in the different areas? (e.g., by state, north/south, urban/rural)
    """

    # ---- What is the percentage of reviews containing a variant of the word "authentic"?
    # Get reviews for only restaurants
    rest = bs[bs.categories.contains('Restaurants')]
    rest_rs = rest.join(rs, "business_id")

    # Get reviews with a form of the word authentic
    rest_rs_auth = rest_rs.filter(rest_rs.text.rlike('authentic'))

    # Get the percentage of such revirews
    auth_perc = (rest_rs_auth.count()/rest_rs.count())*100
    print(f">> Percentage of reviews with a form of authentic: {auth_perc} %\n")

    # ---- How many reviews contain the string "legitimate" grouped by restaurant categories/businesses type (type of cuisine)?
    # Get reviews with word legitimate
    rest_rs_legit = rest_rs.filter(rest_rs.text.rlike('legitimate'))

    # Get count of categories from ALL reviews
    cat_count_all = rest_rs.withColumn('category', f.explode(f.split(f.col('categories'), ', '))).groupBy('category').agg({"category": "count"}).withColumnRenamed("count(category)", "count_all")

    # Get count of categories from reviews which include word legitimate
    cat_count_legit = rest_rs_legit.withColumn('category', f.explode(f.split(f.col('categories'), ', '))).groupBy('category').agg({"category": "count"}).withColumnRenamed("count(category)", "count_legit")

    # Normalize by the count in the actual category
    cat_count_legit_all = cat_count_legit.join(cat_count_all, "category")
    cat_count_legit_all = cat_count_legit_all.withColumn("normalized", cat_count_legit_all.count_legit/cat_count_legit_all.count_all)

    # Show sorted according to the count legit
    print(">> Count of the categories of reviews which include word 'legit':")
    cat_count_legit_all.sort("count_legit", ascending=False).toPandas().to_csv('data/category_count.csv')
    print("Done!\n")

    # ---- Is there a difference in the amount of authenticity language used in the different areas?
    print(">> Count of the reviews, which are using authenticity lang, per state and per city:")
    rest_rs = rest_rs.withColumn("isThereAuth", rest_rs.text.rlike('(legitimate)|(authentic)'))
    rest_rs_cube = rest_rs.cube("state", "city", "isThereAuth").count().orderBy("count", ascending=False)
    rest_rs_cube.toPandas().to_csv('data/location_count.csv')
    print("Done!\n")
```

I started by selecting relevant businesses, i.e. `Restaurants`. Note that
despite this, there remained still business which have both category `Restaurant` and for example `Dentist`. Then I assigned each business to its corresponding reviews. I started by analyzing how many of these reviews includes some form of word 'authenticity', surprisingly it was
only around `2.5 %`. I then continued by analysing reviews which include word
"legitimate". More specifically, I looked at categories which are associated
with these reviews. Here are top categories found:

```
+--------------------+-----------+---------+--------------------+               
|            category|count_legit|count_all|          normalized|
+--------------------+-----------+---------+--------------------+
|                Food|        625|  1133172|5.515491028722912E-4|
|           Nightlife|        574|  1009498|5.685994424951807E-4|
|                Bars|        554|   974747|5.683526084204414E-4|
|      American (New)|        360|   729264|4.936483907062463E-4|
|American (Traditi...|        323|   733103|4.405929316889987E-4|
|  Breakfast & Brunch|        309|   646334|4.780809921805135E-4|
|          Sandwiches|        238|   475626|5.003931660590464E-4|
|               Pizza|        217|   394428|5.501637814759601E-4|
|             Italian|        209|   392125|5.329933057060886E-4|
|             Mexican|        207|   401693|5.153189127019888E-4|
+--------------------+-----------+---------+--------------------+
```

It probably makes sense that at the top we see `Nightlife` and `Bars` since
people are interested if the given bar is for example safe to go, therefore
reviewers mention this. Interestingly, `Pizza` and `Sandwiches` categories are
at the top compare to categories like `Tacos`, `Fish and Chips` or `Poke` which
are rather in-frequent. I would say from the given data that this might be
rather a result of the fact that there are many pizza places compare to fish and
chips places. This can be seen from the `count_all` column. If we would only look at categories describing cuisine, then here is the top :

```
+--------------------+-----------+---------+--------------------+               
|            category|count_legit|count_all|          normalized|
+--------------------+-----------+---------+--------------------+
|      American (New)|        360|   729264|4.936483907062463E-4|
|American (Traditi...|        323|   733103|4.405929316889987E-4|
|             Italian|        209|   392125|5.329933057060886E-4|
|             Mexican|        207|   401693|5.153189127019888E-4|
|            Japanese|        166|   309510|5.363316209492424E-4|
|             Chinese|        136|   261527|5.200227892339988E-4|
+--------------------+-----------+---------+--------------------+
```

We can see that most legitimate cuisines seem to be the ones where we would
expect the bias except from the `Mexican` or `Chinese`. Therefore, this might be an example which would contradict the findings in the [article](https://ny.eater.com/2019/1/18/18183973/authenticity-yelp-reviews-white-supremacy-trap).  
