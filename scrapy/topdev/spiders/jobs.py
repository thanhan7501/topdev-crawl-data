import scrapy
import json


class JobsSpider(scrapy.Spider):
    name = "jobs"
    allowed_domains = ["topdev.vn"]
    api_url = """https://api.topdev.vn/td/v2/jobs?
                  fields[job]=id,slug,title,salary,company,extra_skills,skills_str,
                  skills_arr,skills_ids,job_types_str,job_levels_str,job_levels_arr,job_levels_ids,
                  addresses,status_display,detail_url,job_url,salary,published,refreshed,applied,
                  candidate,requirements_arr,packages,benefits,content,features,is_free,is_basic,
                  is_basic_plus,is_distinction&fields[company]=slug,tagline,addresses,skills_arr,
                  industries_arr,industries_str,image_cover,image_galleries,benefits&locale=vi_VN
                  &ordering=jobs_new&page="""
    custom_settings = {
        'COLLECTION_NAME' : 'jobs'
    }
    
    def start_requests(self):
        current_page = 1
        first_url = self.api_url + str(current_page)
        yield scrapy.Request(url=first_url, callback=self.parse_next_page, meta={'current_page': current_page})
    
    def parse_next_page(self, response):
        data = json.loads(response.body)
        jobs = data.get("data")
        meta = data.get("meta")
        for job in jobs:
            job['_id'] = job['id']
            job.pop('id', None)
            yield job

        current_page = response.meta['current_page']

        if current_page < meta["last_page"]:
            current_page += 1
            next_url = self.api_url + str(current_page)
            yield scrapy.Request(url=next_url, callback=self.parse_next_page, meta={'current_page': current_page})
