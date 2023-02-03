from httpx import Client

from os import getenv
from datetime import datetime
from json import loads
import time


class HttpxClient(Client):

    def __init__(self, api_key) -> None:
        super().__init__()
        self.api_key = api_key
        self.parameters = {
            "key": api_key,
            "format": "json"
        }


    def get(self, url, parameters={}):
        self.parameters |= parameters
        return super().get(url, params=self.parameters)

    
    def request_data(self, endpoint, parameters={}):
        '''
        makes request to API gateway with specific endpoint and send parameters

        Params:

        endpoint (str) -> endpoint name to hit
        parameters (dict) -> any extra parameters to use in the request
        '''
        
        # Set URL from gateway and desired endpoint
        url = f"{getenv('GATEWAY')}/{endpoint}"

        request_complete = True
        
        # Loop calling request until we receive a proper response
        while request_complete: 
            # Rate limit defined as One request per second
            time.sleep(1)

            
            data = self.get(url, parameters=parameters)
            print(data.status_code)
            # send request and handle different response codes
            if data.status_code <= 203:
                try: 
                    response = loads(data.text)
                    request_complete = True
                except Exception as e:
                    response = {"status": "FAILED", "error_message": e, "error_date": datetime.now()}
                    request_complete = True
            elif data.status_code == 429:
                response = {"status": "FAILED", "error_message": "Too many requests. Rate limit hit.", "error_date": datetime.now()}
                print("Retrying Request...")
                time.sleep(5)
                continue

            elif data.status_code == 400:
                response = {"status":"FAILED", "error_message":"Unauthorized request. Invalid API Key.", "error_date": datetime.now()}
                request_complete = True
                
            return response