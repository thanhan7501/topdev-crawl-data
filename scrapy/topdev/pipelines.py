# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
from itemadapter import ItemAdapter
import logging
import json
from datetime import datetime, timezone
from scrapy.exceptions import NotConfigured


class JsonWriterPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('JSONWRITERPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            raise NotConfigured
        return cls()
    
    def open_spider(self, spider):
        file_name = spider.settings.get("COLLECTION_NAME")
        self.file = open(f"{file_name}.jsonl", "w", encoding="utf-8")

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        print(item)
        self.file.write(line)
        return item


class MongoPipeline:
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            raise NotConfigured
        
        return cls(
            mongo_uri=crawler.settings.get("MONGODB_URI"),
            mongo_db=crawler.settings.get("MONGODB_DB"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command("ping")
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        current_time = datetime.now().astimezone(timezone.utc)
        update_item = {
            "$set": {
                **item,
                "updated_at": current_time
            },
            "$setOnInsert": {
                "inserted_at": current_time,
            },
        }
        self.db[spider.settings.get("COLLECTION_NAME")].update_one(
            {"_id": item["_id"]}, update_item, upsert=True
        )
        return item
