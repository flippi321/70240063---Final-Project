import os
import json

# Partition user files
#   Users are split between Beijing and Hong Kong
def partition_user(input_dir, output_dir):
    with open(f"{input_dir}/user.dat", "r") as infile:
        beijing_users = []
        hongkong_users = []
        for line in infile:
            user = json.loads(line)
            if user["region"] == "Beijing":
                beijing_users.append(user)
            elif user["region"] == "Hong Kong":
                hongkong_users.append(user)
    
    with open(f"{output_dir}/user_beijing.json", "w") as out_bj:
        json.dump(beijing_users, out_bj)
    with open(f"{output_dir}/user_hongkong.json", "w") as out_hk:
        json.dump(hongkong_users, out_hk)

    return True

# Partition article files
#   Articles are split between Science and Technology
def partition_article(input_dir, output_dir):
    with open(f"{input_dir}/article.dat", "r") as infile:
        science_articles = []
        technology_articles = []
        for line in infile:
            article = json.loads(line)
            if article["category"] == "science":
                science_articles.append(article)
            elif article["category"] == "technology":
                technology_articles.append(article)
    
    with open(f"{output_dir}/article_science.json", "w") as out_sc:
        json.dump(science_articles, out_sc)
    with open(f"{output_dir}/article_technology.json", "w") as out_tech:
        json.dump(technology_articles, out_tech)

    return True

# Partition the read files
#   This is split between Beijing and Hong Kong (depending on which user who read the article)
def partition_read(input_dir, output_dir):
    # Load users to match region
    with open(f"{output_dir}/user_beijing.json", "r") as infile_bj, \
         open(f"{output_dir}/user_hongkong.json", "r") as infile_hk:
        beijing_uids = {user["uid"] for user in json.load(infile_bj)}
        hongkong_uids = {user["uid"] for user in json.load(infile_hk)}

    with open(f"{input_dir}/read.dat", "r") as infile:
        beijing_reads = []
        hongkong_reads = []
        for line in infile:
            read = json.loads(line)
            if read["uid"] in beijing_uids:
                beijing_reads.append(read)
            elif read["uid"] in hongkong_uids:
                hongkong_reads.append(read)
    
    with open(f"{output_dir}/read_beijing.json", "w") as out_bj:
        json.dump(beijing_reads, out_bj)
    with open(f"{output_dir}/read_hongkong.json", "w") as out_hk:
        json.dump(hongkong_reads, out_hk)

    return True

def partition_all(input_dir, output_dir):
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Partition the files
    user = partition_user(input_dir, output_dir)
    article = partition_article(input_dir, output_dir)
    read = partition_read(input_dir, output_dir)

    # If any of the partitioning fails, return False
    return user and article and read
partition_all(input_dir="data/database/dat_files", output_dir="data/database/partitioned")

# TODO REMOVE
"""
Now we can run

mongoimport --host localhost --port 27017 --db db_beijing --collection User --file data/database/partitioned/user_beijing.json --jsonArray
mongoimport --host localhost --port 27017 --db db_beijing --collection Article --file data/database/partitioned/article_science.json --jsonArray
mongoimport --host localhost --port 27017 --db db_beijing --collection Read --file data/database/partitioned/read_beijing.json --jsonArray

mongoimport --host localhost --port 27018 --db db_hongkong --collection User --file data/database/partitioned/user_hongkong.json --jsonArray
mongoimport --host localhost --port 27018 --db db_hongkong --collection Article --file data/database/partitioned/article_technology.json --jsonArray
mongoimport --host localhost --port 27018 --db db_hongkong --collection Read --file data/database/partitioned/read_hongkong.json --jsonArray

"""