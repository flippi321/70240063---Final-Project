import json
import random
import numpy as np
from PIL import Image
from shutil import copyfile
import os
from enum import Enum

def generate_data(
        num_users=10000, 
        num_articles=10000, 
        num_reads=1000000, 
        input_dir='data/raw', 
        data_output_dir='data/database/articles', 
        dat_files_output_dir='data/database/dat_files', 
        gb_size=10):
    
    uid_region = {}
    aid_lang = {}

    USERS_NUM = num_users
    ARTICLES_NUM = num_articles
    READS_NUM = num_reads

    uid_region = {}
    aid_lang = {}
    
    # Depending on dataset size, choose chance of getting photo
    if([10, 50, 100].count(gb_size) == -1):
        print("Invalid dataset size. Please choose 10, 50, or 100 GB.")
        return False
    
    image_probability = {
        10: 0.05,
        50: 0.4,
        100: 0.6,
    }[gb_size]

    # Beijing:60%   Hong Kong:40%
    # en:20%    zh:80%
    # 20 depts
    # 3 roles
    # 50 tags
    # 0~99 credits

    def gen_an_user (i):
        timeBegin = 1506328859000
        user = {}
        user["timestamp"] = str(timeBegin + i)
        user["id"] = 'u'+str(i)
        user["uid"] = str(i)
        user["name"] = "user%d" % i
        user["gender"] = "male" if random.random() > 0.33 else "female"
        user["email"] = "email%d" % i
        user["phone"] = "phone%d" % i
        user["dept"]  = "dept%d" % int(random.random() * 20)
        user["grade"] = "grade%d" % int(random.random() * 4 + 1)
        user["language"] = "en" if random.random() > 0.8 else "zh"
        user["region"] = "Beijing" if random.random() > 0.4 else "Hong Kong"
        user["role"] = "role%d" % int(random.random() * 3)
        user["preferTags"] = "tags%d" % int(random.random() * 50)
        user["obtainedCredits"] = str(int(random.random() * 100))

        uid_region[user["uid"]] = user["region"]
        return user

    # science:45%   technology:55%
    # en:50%    zh:50%
    # 50 tags
    # 2000 authors
    def gen_an_article (i):
        timeBegin = 1506000000000
        article = {}
        article["id"] = 'a'+str(i)
        article["timestamp"] = str(timeBegin + i)
        article["aid"] = str(i)
        article["title"] = "title%d" % i
        article["category"] = "science" if random.random() > 0.55 else "technology"
        article["abstract"] = "abstract of article %d" % i
        article["articleTags"] = "tags%d" % int(random.random() * 50)
        article["authors"]  = "author%d" % int(random.random() * 2000)
        article["language"] = "en" if random.random() > 0.5 else "zh"

        # create text
        article["text"] = "text_a"+str(i)+'.txt'
        path = f'{data_output_dir}/article'+str(i)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True) 

        categories = ['business', 'entertainment', 'sport', 'tech']
        random_category = categories[random.randint(0,3)]
        files = os.listdir(f'{input_dir}/bbc_news_texts/' + random_category +'/')
        size = len(files)
        random_news = files[random.randint(0,size-1)]
        copyfile(f'{input_dir}/bbc_news_texts/' + random_category +'/' +random_news, path+"/text_a"+str(i)+'.txt')


        # create images
        image_num = random.randint(1,3)
        image_str = ""
        for j in range(image_num):
            image_str+= 'image_a'+str(i)+'_'+str(j)+'.jpg,'
        article["image"] = image_str
    
        for j in range(image_num):
            copyfile(f'{input_dir}/image/' + str(random.randint(0,599))+'.jpg',path+'/image_a'+str(i)+'_'+str(j)+'.jpg')

        # create video
        if random.random() < image_probability:
            #has one video
            article["video"] = "video_a"+str(i)+'_video.flv'
            if random.random()<0.5:
                copyfile(f'{input_dir}/video/video1.flv',path+"/video_a"+str(i)+'_video.flv')
            else:
                copyfile(f'{input_dir}/video/video2.flv',path+"/video_a"+str(i)+'_video.flv')
        else:
            article["video"] = ""

        aid_lang[article["aid"]] = article["language"]
        return article

    # user in Beijing read/agree/comment/share an english article with the probability 0.6/0.2/0.2/0.1
    # user in Hong Kong read/agree/comment/share an Chinese article with the probability 0.8/0.2/0.2/0.1
    p = {}
    p["Beijing"+"en"] = [0.6,0.2,0.2,0.1]
    p["Beijing"+"zh"] = [1,0.3,0.3,0.2]
    p["Hong Kong"+"en"] = [1,0.3,0.3,0.2]
    p["Hong Kong"+"zh"] = [0.8,0.2,0.2,0.1]
    def gen_an_read (i):
        timeBegin = 1506332297000
        read = {}
        read["timestamp"] = str(timeBegin + i*10000)
        read["id"] = 'r'+str(i)
        read["uid"] = str(int(random.random() * num_users))
        read["aid"] = str(int(random.random() * num_articles))
        
        region = uid_region[read["uid"]]
        lang = aid_lang[read["aid"]]
        ps = p[region + lang]

        if (random.random() > ps[0]):
            # read["readOrNot"] = "0";
            return gen_an_read (i)
        else:
            # read["readOrNot"] = "1"
            read["readTimeLength"] = str(int(random.random() * 100))
            # read["readSequence"] = str(int(random.random() * 4))
            read["agreeOrNot"] = "1" if random.random() < ps[1] else "0"
            read["commentOrNot"] = "1" if random.random() < ps[2] else "0"
            read["shareOrNot"] = "1" if random.random() < ps[3] else "0"
            read["commentDetail"] = "comments to this article: (" + read["uid"] + "," + read["aid"] + ")" 
        return read

    if not os.path.exists(f'/{data_output_dir}'):
        os.makedirs(f'./{data_output_dir}', exist_ok=True)

    if not os.path.exists(f'/{dat_files_output_dir}'):
        os.makedirs(f'./{dat_files_output_dir}', exist_ok=True)

    with open(f"{dat_files_output_dir}/user.dat", "w+") as f:
        for i in range (USERS_NUM):
            json.dump(gen_an_user(i), f)
            f.write("\n")

    with open(f"{dat_files_output_dir}/article.dat", "w+") as f:
        for i in range(ARTICLES_NUM):
            json.dump(gen_an_article(i), f)
            f.write("\n")


    with open(f"{dat_files_output_dir}/read.dat", "w+") as f:
        for i in range(READS_NUM):
            json.dump(gen_an_read(i), f)
            f.write("\n")

    return True
