## About 
This small project is an assignment in the course `Large Scale Data Analysis`
taken at [IT University of Copenhagen](https://www.itu.dk/). Its goal is to
analyze [Yelp reviews](https://www.yelp.com/dataset/documentation/main) dataset using [spark](https://spark.apache.org/).
More specifically, the main goal was to test the following hypothesis:

> Can you identify a difference in the relationship between authenticity language (words
such as "authentic" or "legitimate" or their derived forms) and typically negative words
(like "dirty", "kitsch", "cheap", "rude", "simple" or similar), in restaurants serving south
american or south asian cuisine than in restaurants serving european cuisine? And to
what degree?

## Run the project
In order to run this project, you need to participate in the course mentioned above.
Start by cloning this repo locally and then `cd` into it. Now, you need to move the src
folder to the cluster, you can use the following commands 
(please make sure that you modify the variables in the square brackets accordingly):

```bash
zip -r src.zip src
scp -P 8022 src.zip [itu_username]@130.226.142.166;~/[path_to_folder]
```

Now you need to ssh into the `ambari` cluster:

```bash
ssh [itu_username]@130.226.142.166 -p 8022
```

Note that this method will only work if you provided your `public key` to the corresponding course manager. You should now be in your home directory: `~/`. Next, unzip the copied zip file and remove it as you no longer need it:

```bash
cd [path_to_folder]
unzip src.zip
rm -r src.zip
```

Finally, you can run the code as follows:

```bash
cd src/
pyspark < main.py
```

If you have any questions or find any bugs, please reach me at `luci@itu.dk`.

