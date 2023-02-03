from mongo import DbClient
from client import HttpxClient


from os import getenv
from dotenv import load_dotenv




def update_tz_data():
    '''
    Updates timezone database with new data from timezonedb
    '''

    # Create httpx client to make requests
    client =  HttpxClient(getenv("API_KEY"))

    all_tz = client.request_data("list-time-zone")

    db_client = DbClient()
    # if request response is OK then we can Delete the old entries and repopulate the database
    if all_tz and all_tz.get("status") == "OK":
        all_tz = all_tz.get("zones")
        db_client.delete(getenv("TIMEZONES"))
        db_client.insert(getenv("TIMEZONES"), all_tz)    
    # If the request FAILED then log the errors
    else:
        db_client.insert(getenv("ERROR_LOG"),[all_tz])   

def update_zone_data():
    '''
    updates zone database 
    '''

    # Retrieve Time zone data from DB
    db_client = DbClient()
    all_tz_data = db_client.get(getenv("TIMEZONES"))
    
    # Open HTTPX client to make requests
    with HttpxClient(getenv("API_KEY")) as client:
        if all_tz_data:
            for item in all_tz_data:
                # Make Zone specific requests required by API
                parameters = {
                    "by": "zone",
                    "zone": item.get("zoneName"),
                }
                zone = client.request_data("get-time-zone", parameters=parameters)
                print(zone)
                # If the Request FAILED then log error
                if zone.get("status") == "FAILED":
                    db_client.insert(getenv("ERROR_LOG"),[zone])   
                # Else log in Zone Details table
                else:
                    db_client.insert(getenv("ZONE_DETAILS"),[zone],check_exists=True)    



def main():
    load_dotenv()
    update_tz_data()
    update_zone_data()



if __name__ == "__main__":
    main()
