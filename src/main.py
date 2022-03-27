# ---------------------- Spark setup
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf

conf=SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")


spark = SparkSession.builder \
                    .appName('ludek-cizinsky-assgn-02') \
                    .config(conf=conf) \
                    .getOrCreate()

# ---------------------- 3.1 Specific DataFrame Queries implementation
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

    return bs.filter((bs.stars == 5) & (bs.review_count >= 1000))["name", "stars", "review_count"]

def q3(us):
    """
    Analyze user.json to find the influencers who have written more than 1000 reviews. The
    output should be in the form of DataFrame of user id.
    """
    
    return us.filter(us.review_count > 1000)["user_id"]


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


def main():

    # ------------ 3.1 Specific DataFrame Queries
    # Load all the needed data
    bs = spark.read.json("/datasets/yelp/business.json")
    rs = spark.read.json("/datasets/yelp/review.json")
    us = spark.read.json("/datasets/yelp/user.json")

    # Run the queries
    a1 = q1(bs)
    a2 = q2(bs)
    a3 = q3(us)
    a4 = q4(bs, rs, a3)
    a5 = q5(rs, us)
