

def generate_ar_aging():

    for account in accounts:
        url = f"{base_url}/customer/{account}/reports/10062054/filter/10137065/run"
        
        try:
            response = requests.post(url, auth=(username, password))

            if response.status_code == 200:
                success = handle_report_response(response.text, account, 'ar_aging')
                if success:
                    print(f"Report started and DB updated for account {account}")
                else:
                    print(f"Failed to handle response for account {account}")
            else:
                print(f"API call failed for account {account} - Status: {response.status_code}")

        except Exception as e:
            logging.error(f"Exception for account {account}: {str(e)}")
        time.sleep(10)

def generate_charges_on_hold():

    for account in accounts:
        url = f"{base_url}/customer/{account}/reports/10062055/filter/10137067/run"
        
        try:
            response = requests.post(url, auth=(username, password))

            if response.status_code == 200:
                success = handle_report_response(response.text, account, 'charges_on_hold')
                if success:
                    print(f"Report started and DB updated for account {account}")
                else:
                    print(f"Failed to handle response for account {account}")
            else:
                print(f"API call failed for account {account} - Status: {response.status_code}")

        except Exception as e:
            logging.error(f"Exception for account {account}: {str(e)}")
        time.sleep(10)

def generate_claim_stage_breakdown():

    for account in accounts:
        url = f"{base_url}/customer/{account}/reports/10062056/filter/10137069/run"
        
        try:
            response = requests.post(url, auth=(username, password))

            if response.status_code == 200:
                success = handle_report_response(response.text, account, 'claim_stage_breakdown')
                if success:
                    print(f"Report started and DB updated for account {account}")
                else:
                    print(f"Failed to handle response for account {account}")
            else:
                print(f"API call failed for account {account} - Status: {response.status_code}")

        except Exception as e:
            logging.error(f"Exception for account {account}: {str(e)}")
        time.sleep(10)

   

#generate_ar_aging()
time.sleep(10)
generate_charges_on_hold()