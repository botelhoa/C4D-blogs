
import os
import sys
import csv
import time
import requests
import click
import logging

import pandas as pd

from dataclasses import dataclass, fields, asdict
from typing import Union
from rush.limiters import periodic
from rush.stores import dictionary
from rush.throttle import Throttle
from rush.quota import Quota
from rush.contrib import decorator


test_throttle = Throttle(
    rate=Quota.per_hour(1000), 
    limiter=periodic.PeriodicLimiter(
       store=dictionary.DictionaryStore()
        )
    )

throttle_decorator = decorator.ThrottleDecorator(throttle=test_throttle)


DATA_PATH = os.path.join(os.getcwd().split("fundraising")[0], "fundraising/data")



@dataclass
class DonationItem:
    transaction_id: str
    committee_id: str
    contributor_id: Union[None, str]
    contributor_name: Union[None, str]
    contributor_city: Union[None, str]
    contributor_employer: Union[None, str]
    contributor_occupation: Union[None, str]
    contributor_state: Union[None, str]
    contributor_zip: Union[None, str]
    is_individual: bool
    contribution_receipt_amount: float
    contribution_receipt_date: str
    contributor_aggregate_ytd: float
    donor_committee_name: Union[None, str]
    election_type: str
    entity_type: str
    entity_type_desc: str
    memo_text: Union[None, str]
    report_type: str
    report_year: str
    receipt_type_full: str



class FECCollector:
    def __init__(self, start: str="2023-01-01", stop: str="2023-12-31") -> None:
        self.key = os.environ["API_KEY"]
        self.base_url = "https://api.open.fec.gov/v1/"
        self.start = start
        self.stop = stop
        
        
    @throttle_decorator
    def get_url(self, url: str, retry_count: int = 0, retry_limit: int=3):
        
        try:
            response = requests.get(url, timeout=5)
            response_failure = None
            
        except requests.RequestException as e:
            response = None
            response_failure = e

        while response is not None and response.status_code == 429:
            time.sleep(3)
            response = requests.get(url, timeout=5)

        if response is None or response.status_code >= 500:
            
            if response:
                failure_reason = f'http failure {response.status_code}'
            else:
                failure_reason = f'response {response}; network failure {response_failure}'
            
            retry_count += 1
            if retry_count <= retry_limit:
                response = self.get_url(url, retry_count=retry_count)
            else:
                logging.error(f'response failed ({failure_reason}); giving up on retry {retry_count}')
                exit()

        return response
    
    def unpack_response(self, response) -> tuple:
        
        if not response or not response.ok:
            exit(1)
        try:
            return (response.json()['pagination'], response.json()['results'])

        except Exception:
            logging.error(f'Unexpected parsing failure: {response.status_code} {response.text}')
            exit()
        
    
    def query_candidates(self, query: str, office: str='P') -> list:
        
        endpoint = "candidates/"
        search_url = f"?office={office}&q={query}&per_page=100&sort_hide_null=false&sort_nulls_last=false&page=1&sort_null_only=false&api_key={self.key}&sort=name"
        
        _, result = self.unpack_response(self.get_url(self.base_url+endpoint+search_url))

        return [i for i in result if i["candidate_status"] == 'C']

        
            
    def query_committees(self, candidate_id: str) -> list:
        
        endpoint = f"candidate/{candidate_id}/committees/"
        search_url = f"?per_page=100&sort_nulls_last=false&page=1&sort_hide_null=false&sort_null_only=false&api_key={self.key}&sort=name"
        
        _, result = self.unpack_response(self.get_url(self.base_url+endpoint+search_url))

        return result

        
    
    def query_fundraising(self, committee_id: str, writer) -> list:

        self.writer = writer

        endpoint = "schedules/schedule_a/"
        
        #results = []
        total_count = None
        remaining_records = None
        last_index = None
        last_contribution_receipt_date = None


        while remaining_records != 0:

            per_page = 100 
            if remaining_records != None and remaining_records < 100: 
                per_page = remaining_records

            search_url = f"?min_date={self.start}&max_date={self.stop}&committee_id={committee_id}&per_page={per_page}&sort=-contribution_receipt_date&sort_hide_null=false&sort_null_only=false&api_key={self.key}" # contributor_name=winred&contributor_name=actblue&
            
            
            if last_index:
                search_url = search_url + "&last_index=" + last_index

            if last_contribution_receipt_date:
                
                if last_contribution_receipt_date != "NULL":
                    search_url = search_url + "&last_contribution_receipt_date=" + last_contribution_receipt_date
                else:
                    search_url = search_url.split("sort_null_only=")[0] + "true" + search_url.split("sort_null_only=")[1][5:]

            
            combined_url = self.base_url+endpoint+search_url
            try:
                pagination, result = self.unpack_response(self.get_url(combined_url)) 
            except Exception as error:
                print(error)
                breakpoint()

            if len(result) > 0:

                last_index = pagination["last_indexes"]["last_index"]
                last_contribution_receipt_date = pagination["last_indexes"]["last_contribution_receipt_date"]
                
                if not total_count:
                    total_count = pagination["count"]

                if remaining_records == None:
                    remaining_records = total_count

                #results.extend([self.unpack_fundraising(i) for i in result])
                self.write(result)
            else:
                break

            remaining_records = remaining_records - len(result)

            
        #return results


    def unpack_fundraising(self, data: dict) -> DonationItem:

        return asdict(DonationItem(
            transaction_id = data["transaction_id"],
            committee_id = data["committee_id"],
            contributor_id = data["contributor_id"],
            contribution_receipt_amount = data["contribution_receipt_amount"],
            contribution_receipt_date = data["contribution_receipt_date"],
            contributor_aggregate_ytd = data["contributor_aggregate_ytd"],
            contributor_name = data["contributor_name"], 
            contributor_city = data["contributor_city"],
            contributor_employer = data["contributor_employer"], 
            contributor_occupation = data["contributor_occupation"], 
            contributor_state = data["contributor_state"],
            contributor_zip = data["contributor_zip"], 
            donor_committee_name = data["donor_committee_name"],
            election_type = data["election_type_full"], 
            entity_type = data["entity_type"], 
            entity_type_desc = data["entity_type_desc"], 
            is_individual = data['is_individual'],
            memo_text = data["memo_text"],
            report_type = data["report_type"],
            report_year = data["report_year"],
            receipt_type_full = data['receipt_type_full'],
        ))

    def write(self, results: list) -> None:
        for result in results:
            self.writer.writerow(self.unpack_fundraising(result))


@click.command()
@click.option('-candidate_path', '-c', type=click.Path(exists=True, dir_okay=False), help="Local path to file with list of candidates")
@click.option('-key_path', '-k', type=click.Path(exists=True, dir_okay=False), help="Local path to FEC API key")
def cli(candidate_path, key_path):
    """
    collect -c "candidates.txt" -k "fec_api_key.txt"
    """

    #logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    start = time.time()

    with open(key_path) as f:
        os.environ["API_KEY"] = f.readlines()[0]

    candidates = []
    with open(candidate_path) as f:
        for line in f.readlines():
            candidates.append(line.strip())


    fc = FECCollector(start="2023-01-01", stop="2023-08-01")

    logging.info(f"Collecting FEC data for the following candidates: {candidates}")

    logging.info("Starting candidate data collection.")

    if os.path.isfile(f"{DATA_PATH}/candidates.csv") and pd.read_csv(f"{DATA_PATH}/candidates.csv").empty == False:
        candidates = pd.read_csv(f"{DATA_PATH}/candidates.csv", index_col=0)
    else:  
        results = []
        for candidate in candidates:
            results.extend(fc.query_candidates(candidate))

        candidates = pd.DataFrame(results)
        candidates.to_csv(f"{DATA_PATH}/candidates.csv")

    logging.info("Finished candidate data collection.")


    logging.info("Starting comittee data collection.")

    if os.path.isfile(f"{DATA_PATH}/committees.csv") and pd.read_csv(f"{DATA_PATH}/committees.csv").empty == False:
        committees = pd.read_csv(f"{DATA_PATH}/committees.csv", index_col=0)
    else:
        results = []
        for candidate_id in candidates["candidate_id"]:
            results.extend(fc.query_committees(candidate_id))
            
        committees = pd.DataFrame(results)
        committees.to_csv(f"{DATA_PATH}/committees.csv")

    logging.info("Finished comittee data collection.")

    logging.info("Starting donation data collection.")

    with open(f"{DATA_PATH}/donations.csv", 'w+') as f: # Move to witihn function
        
        fieldnames = [field.name for field in fields(DonationItem)]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for committee_id in committees["committee_id"]:
            logging.info(f"Collecting data for {committee_id}")
            results = fc.query_fundraising(committee_id, writer)

            # for result in results:
            #     writer.writerow(result)


    stop = time.time()
    logging.info(f"Finished donation data collection. Total runtime: {stop-start} seconds")






