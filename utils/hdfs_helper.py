import os
import json
import subprocess
try:
    from pydoop import hdfs
except:
    subprocess.run("pip3 install pydoop")
    from pydoop import hdfs


def get_hdfs_partition_path(article_metadata):
    """
    Determines the HDFS path for an article based on its region and category.
    """
    if article_metadata["region"] == "Beijing" and article_metadata["category"] == "science":
        return f"/data/dbms1/science/article_{article_metadata['id']}"
    elif article_metadata["region"] == "Hong Kong" and article_metadata["category"] == "technology":
        return f"/data/dbms2/technology/article_{article_metadata['id']}"
    else:
        raise ValueError(f"Unknown partition for article: {article_metadata}")


def upload_article_media_to_hdfs(article_id, article_path, hdfs_base_path):
    """
    Uploads media files (text, images, videos) for a specific article to HDFS.
    """
    for root, dirs, files in os.walk(article_path):
        for file in files:
            local_path = os.path.join(root, file)
            hdfs_file_path = os.path.join(hdfs_base_path, file)
            hdfs.put(local_path, hdfs_file_path)
            print(f"Uploaded {local_path} to {hdfs_file_path}")


def process_and_upload_articles(article_dir, science_partition_file, technology_partition_file):
    """
    Processes articles and uploads their media files to HDFS based on partitioning.
    """
    # Load partitioning results
    with open(science_partition_file, "r") as sc_file, \
         open(technology_partition_file, "r") as tech_file:
        science_articles = {article["id"]: article for article in json.load(sc_file)}
        technology_articles = {article["id"]: article for article in json.load(tech_file)}

    # Iterate through each article directory and upload media to HDFS
    for article in os.listdir(article_dir):
        article_path = os.path.join(article_dir, article)
        if os.path.isdir(article_path):
            article_id = int(article.replace("article", ""))
            if article_id in science_articles:
                hdfs_base_path = get_hdfs_partition_path(science_articles[article_id])
            elif article_id in technology_articles:
                hdfs_base_path = get_hdfs_partition_path(technology_articles[article_id])
            else:
                print(f"Skipping article {article_id} (not in any partition)")
                continue

            # Upload the media files for the article
            upload_article_media_to_hdfs(article_id, article_path, hdfs_base_path)

    print("All media files uploaded to HDFS successfully.")


def add_hdfs_paths_to_articles(data, hdfs_base_path_func):
    """
    Adds HDFS paths to article records for text, images, and videos.
    """
    for article in data:
        article_id = article["id"]
        hdfs_base_path = hdfs_base_path_func(article)
        article["text_path"] = f"hdfs://localhost:9000{hdfs_base_path}/text_a{article_id}.txt"
        article["image_paths"] = [
            f"hdfs://localhost:9000{hdfs_base_path}/image_a{article_id}_{i}.jpg"
            for i in range(1, 6)
        ]
        if article.get("has_video", False):
            article["video_path"] = f"hdfs://localhost:9000{hdfs_base_path}/video_a{article_id}.mp4"
    return data
