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

I started by selecting relevant businesses, i.e. `Restaurants`. Then I assigned
each business to its corresponding reviews. I started by analyzing how many of
these reviews includes some form of word 'authenticity', surprisingly it was
only around `2.5 %`. I then continued by analysing reviews which include word
"legitimate". More specifically, I looked at categories which are associated
with these reviews. Here is top 10 categories found:

```
≥≥≥ Category: Food                                : 625
≥≥≥ Category: Nightlife                           : 574
≥≥≥ Category: Bars                                : 554
≥≥≥ Category: American (New)                      : 360
≥≥≥ Category: American (Traditional)              : 323
≥≥≥ Category: Breakfast & Brunch                  : 309
≥≥≥ Category: Sandwiches                          : 238
≥≥≥ Category: Pizza                               : 217
≥≥≥ Category: Italian                             : 209
≥≥≥ Category: Mexican                             : 207
```

It probably makes sense that at the top we see `Nightlife` and `Bars` since people are interested if the given bar is for example safe to go, therefore reviewers mention this. Interestingly, `Pizza` and `Sandwiches` categories are at the top compare to categories like `Tacos`, 'Fish and Chips' or `Poke` which are rather in-frequent. I would say from the given data that this might be rather a result of the fact that there are many pizza places compare to fish and chips places. Therefore, I decided to normalize the counts by number of reviews in the actual category:

```


```

If we would only look at categories describing cuisine, then here is the top :

```
≥≥≥ Category: American (New)                      : 360
≥≥≥ Category: American (Traditional)              : 323
≥≥≥ Category: Italian                             : 209
≥≥≥ Category: Mexican                             : 207
≥≥≥ Category: Japanese                            : 166
≥≥≥ Category: Chinese                             : 136
≥≥≥ Category: Asian Fusion                        : 130
≥≥≥ Category: Thai                                : 68
≥≥≥ Category: Korean                              : 59
≥≥≥ Category: Canadian (New)                      : 54
```

We can see that most legitimate cuisines seem to be the ones where we would
expect the bias except from the `Mexican` or `Chinese`. Therefore, this might be an example which would contradict the findings in the [article](https://ny.eater.com/2019/1/18/18183973/authenticity-yelp-reviews-white-supremacy-trap). 
