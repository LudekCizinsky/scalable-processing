# Intro
In this document, I answer the questions which are part of the assignment.

# Specific DataFrame Queries

I first loaded the data using:

```py
bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json("/datasets/yelp/review.json")
us = spark.read.json("/datasets/yelp/user.json")
```

I then ran the queries specified below.

## Q1

```py
def q1(bs):
    """
    Analyze business.json to find the total number of reviews for all businesses.
    The output should be in the form of a Spark DataFrame with one value representing the count.
    """

    return bs.agg({'review_count': 'sum'})
```

The return object is spark dataframe, I then ran `collect()` method and
obtained:

```
[Row(sum(review_count)=6459906)]
```

## Q2

```py
def q2(bs):
    """
    Analyze business.json to find all businesses that have received 5 stars and that have
    been reviewed by 1000 or more users. The output should be in the form of DataFrame of
    (name, stars, review count) 
    """

    return bs.filter((bs.stars == 5) & (bs.review_count >= 1000)).select("name", "stars", "review_count")
```

## Q3
```py
def q3(us):
    """
    Analyze user.json to find the influencers who have written more than 1000 reviews. The
    output should be in the form of DataFrame of user id.
    """
    
    return us.filter(us.review_count > 1000).select("user_id")
```

## Q4

```py
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
```

## Q5

```
def q5(rs, us):
    """
    Analyze review.json and user.json to find an ordered list of users based on the average star counts they have given in all their reviews.
    """
    
    rs_us = rs.join(us, "user_id").groupby("user_id").mean("stars").sort("avg(stars)")

    return rs_us
```

# Authenticity Study

## Data exploration
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
despite this, there remained still business which have both category `Restaurant` and for example `Dentist`, since these were rarer, I decided not to spend more time filtering these out. Then I assigned each business to its corresponding reviews. I started by analyzing how many of these reviews includes some form of word 'authenticity', surprisingly it was
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

Then I tried to explore the problem geographic wise. I looked all possible
combinations of the values of columns `country`, `state` and `isThereAuthLang`
(indicating presence of `authenticity` language within a review). Interestingly,
a state with the biggest number of reviews is Nevada, it then naturally has also
the biggest number of reviews which include and do NOT include authenticity
language, it then naturally has also the biggest number of reviews which include
and do NOT include authenticity language. It was then followed by another
`southern` state - `Arizona` - in terms of the number of reviews containing
authenticity language. However, third state is in `Canada`, namely `Ontario`.
The fact that the first two states are from the south might indicate some bias
given the history of southern states.

## Hypothesis testing

Below, I discuss my approaches towards hypothesis testing. I decided to try two
approaches:

1. Naive approach
2. Advanced approach

The aim was to first experiment with a naive approach and then try more advance
one if there will be time. In the below sections, I explained the methodology
and discuss the results.

### Naive approach
#### Methodology

To test my hypothesis, I started with the following assumption:

> I define authentic review as a review which contains any form of either of the following words: legitimate, authentic.

Further, I defined a review to be negative, if it contains any of the following
words:

> dirty, cheap, rude

Similarly, I then defined positive review, if it contains any of the following
words (or their form):

> nice, fresh, eleg

In order to confirm the hypothesis, I then assume that:

> Most common categories of authentic and negative reviews will be of non-western cuisines such as Mexican or Chinese. 
Similarly, most common categories of authentic and positive reviews will be of western cuisines. 

It is important to note, however, that this approach does not take into account
the fact that for example user might be using word authentic independently of the word
cheap. Consider this sentence:

> The Chinese restaurant was great with its authentic cheap food.

where the word authentic is used in the connection with cheap, whereas here is
the opposite example:

> The Chinese restaurant was great, especially its authentic old venue. The food was also relatively cheap given the provided value.

Therefore, the results should be interpretted with this fact in mind. Another
limitation of this methodology is that the list of positive and negative words
is not exhaustive.

#### Results

Given the above settings, I then computed normalized category counts for
positive and negative reviews. Let's start with the top 3 results for negative
reviews:

```
>>> Eastern European    : 3.37
>>> Honduran            : 2.47
>>> Slovakian           : 2.04
>>> Shanghainese        : 1.76
>>> Salvadoran          : 0.94
>>> Burmese             : 0.91
>>> German              : 0.89
>>> Austrian            : 0.88
```

The score you see was computed by dividing the number of authentic and negative
reviews from given category by the number of reviews in that category. This was
to account for the fact that different categories might have way more reviews
than others. This score was then mutiplied by 100 for a better readability. We
can see that for example cusines from central america (honudran, salvadoran) seem to be the most common one among
negative authentic reviews. However, we can also see that we have German and
Austrian cuisine in the top. Therefore, this would contradict the hypothesis. 
Similarly, we can now look at positive authentic reviews:

```
>>> Honduran            : 5.94
>>> Nicaraguan          : 5.82
>>> Shanghainese        : 5.66
>>> Uzbek               : 3.44
>>> Eastern European    : 3.37
>>> Sicilian            : 3.09
>>> Puerto Rican        : 2.92
>>> Egyptian            : 2.84
```

From the above results, we can see that for example honduran (central america) is also among the top
and as such this seems to suggest that there is no systematic bias towards
cusines from poorer countries. I would like to emphasize that perhaps a possible explanation why I obtained such
results is also a fact that there is simply very small number of for example
reviews of Honudran cuisine and as such even having few negative or posive
reviews might still yield a great number compare to Italian cuisine where there
are a lot of reviews and as such we would also need a significcant amount of
examples of either positive or negative reviews. Finally, to conclude, given the
above explained methodology and results, the conclusion is that there is no
systematic bias where people would make use of the authenticity to refer to
different type of experience (positive, negative) based on the type of cuisine.


# Bonus question: parquet vs json

Loading data from `parquet` is way faster. I believe the reason for this is that
it is already in column format which means that computer can quikcly read the
entire column from disk instead of going row by row. I tested this on the
reviews dataset, and there are in total around 6 to 7 million rows compare to
just a few columns. And since column data is stored close to each other on the
disk, the loading might be signifficantly sped up. Once the data was loaded,
I tried to use filter function, but have not been able to see any difference.
This is because no matter how we load it, the dataset is represented the same in
the memory.
