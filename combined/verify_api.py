import requests
import xml.etree.ElementTree as ET
from config_loader import ConfigLoader
import sys
import argparse

def test_credentials(base_url, username, password, account=None, report_id=None):
    print(f"\nTesting Credentials for: {username}")
    
    # If no account provided, try to discover accounts by listing them
    if not account:
        url = f"{base_url}/customer"
        print(f"  Attempting to discover accounts via GET /customer...")
        try:
            response = requests.get(url, auth=(username, password), timeout=15)
            if response.status_code == 200:
                print(f"  [SUCCESS] Credentials are valid! Received 200 OK from /customer.")
                try:
                    root = ET.fromstring(response.content)
                    # Try to find some Customer elements
                    customers = root.findall('.//Customer')
                    if not customers:
                        # Some versions might use different tags, just print the root
                        print(f"  [INFO] Connected successfully, but no <Customer> elements found in response.")
                        print(f"  Response tag: <{root.tag}>")
                    else:
                        print(f"  [SUCCESS] Found {len(customers)} accessible account(s):")
                        for i, cust in enumerate(customers[:5]): # Show first 5
                            cust_id = cust.find('CustomerID')
                            name = cust.find('Name')
                            id_text = cust_id.text if cust_id is not None else "Unknown"
                            name_text = name.text if name is not None else "Unknown"
                            print(f"    - {id_text} ({name_text})")
                        if len(customers) > 5:
                            print(f"    ... and {len(customers) - 5} more.")
                except ET.ParseError:
                    print(f"  [INFO] Connected successfully, but response is not XML.")
                return True
            elif response.status_code == 401:
                print(f"  [FAILURE] 401 Unauthorized: Invalid username or password.")
                return False
            else:
                print(f"  [INFO] GET /customer returned {response.status_code}. Trying fallback...")
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Connection to /customer failed: {e}")

    # Fallback or specific test if account/report provided
    test_account = account or "10031998" # Default to a known one if we're just guessing
    test_report = report_id or "10062054"
    
    url = f"{base_url}/customer/{test_account}/reports/results/{test_report}"
    print(f"  Testing specific endpoint: /customer/{test_account}/reports/results/{test_report}")
    
    try:
        response = requests.post(url, auth=(username, password), timeout=15)
        if response.status_code == 200:
            print(f"  [SUCCESS] Credentials valid (200 OK).")
            return True
        elif response.status_code == 401:
            print(f"  [FAILURE] 401 Unauthorized: Invalid username or password.")
        elif response.status_code == 404:
            if not account:
                print(f"  [INFO] Could not verify without a valid Account ID (received 404).")
                print(f"  [HINT] Please provide a valid --account ID to test further.")
            else:
                print(f"  [FAILURE] 404 Not Found: Check if Account ID {test_account} is correct.")
        else:
            print(f"  [FAILURE] Received status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Request failed: {e}")
    
    return False

def main():
    parser = argparse.ArgumentParser(description='Verify Collaboratemd API credentials.')
    parser.add_argument('--username', help='API Username')
    parser.add_argument('--password', help='API Password')
    parser.add_argument('--account', help='Customer Account ID (optional)')
    parser.add_argument('--report-id', help='Report ID (optional)')
    parser.add_argument('--base-url', help='API Base URL', default='https://webapi.collaboratemd.com/v1')
    
    args = parser.parse_args()

    if args.username and args.password:
        success = test_credentials(args.base_url, args.username, args.password, args.account, args.report_id)
        if not success:
            sys.exit(1)
    else:
        # Config-based verification
        config_path = 'config/config.py'
        try:
            config_loader = ConfigLoader(config_path)
        except Exception as e:
            print(f"Failed to load config from {config_path}: {e}")
            sys.exit(1)

        instances = config_loader.get_instances()
        for instance_key, config in instances.items():
            accounts = config.get('accounts', [])
            report_configs = config.get('report_configs', [])
            
            acc = accounts[0] if accounts else None
            rep = report_configs[0]['report_id'] if report_configs else None
            
            test_credentials(config['api_base_url'], config['username'], config['password'], acc, rep)
            print("-" * 40)

if __name__ == "__main__":
    main()
