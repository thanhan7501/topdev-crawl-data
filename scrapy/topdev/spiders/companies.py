import scrapy
import json
from typing import List, Dict


class CompaniesSpider(scrapy.Spider):
    name = "companies"
    allowed_domains = ["topdev.vn"]
    api_url = """https://api.topdev.vn/td/v2/companies?fields[company]=tagline,addresses,skills_arr,
                industries_arr,industries_str,image_cover,image_galleries,company_size,nationalities_str,
                skills_ids,skills_str,num_job_openings,benefits,num_employees&locale=vi_VN&page="""    
    custom_settings = {
        'COLLECTION_NAME' : 'companies'
    }
    
    def start_requests(self):
        current_page = 1
        first_url = self.api_url + str(current_page)
        yield scrapy.Request(url=first_url, callback=self.parse_next_page, meta={'current_page': current_page})
    
    def parse_next_page(self, response: Dict):
        data: Dict = json.loads(response.body)
        companies: List[Dict[str, str]] = data.get("data")
        meta: Dict = data.get("meta")
        for company in companies:
            company['_id'] = company['id']
            company.pop('id', None)
            yield company

        current_page = response.meta['current_page']

        if current_page < meta["last_page"]:
            current_page += 1
            next_url = self.api_url + str(current_page)
            yield scrapy.Request(url=next_url, callback=self.parse_next_page, meta={'current_page': current_page})
