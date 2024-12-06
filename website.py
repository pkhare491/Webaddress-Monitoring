import pyodbc
import pandas as pd
import requests
from requests.exceptions import RequestException
import threading

# Database connection string for trusted connection
conn_str = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=DCRODBPRDDB6001;"
    r"DATABASE=CompanyInfo;"
    r"Trusted_Connection=yes;"
)

# Expanded search terms for identifying "for sale" domains
FOR_SALE_KEYWORDS = ["domain for sale", "available for purchase", "domain available", "buy this domain"]

# Function to check if the website's domain is for sale
def check_website(url):
    try:
        if not isinstance(url, str) or url.strip() == "":
            return "Invalid URL"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url

        # Adding a user-agent header to make the request appear more like a real browser
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)

        # Check content for domain-for-sale indicators
        content_lower = response.text.lower()
        if any(keyword in content_lower for keyword in FOR_SALE_KEYWORDS):
            return "Domain is for sale"
        elif response.status_code == 200:
            return "Website is opening normally"
        elif response.status_code == 503:
            return "Website is in maintenance"
        else:
            return f"Website returned status code {response.status_code}"
    except requests.ConnectionError:
        return "Domain is not reachable (Connection error)"
    except requests.Timeout:
        return "Domain timed out"
    except RequestException as e:
        return f"An error occurred: {e}"

# Function to process each row and update with the status
def process_row(row, status_dict):
    status_dict[row['CompanyId']] = check_website(row['WebAddress'])

# Main function to fetch data, check websites, and save results
def main():
    # Connect to the database and execute the SQL query
    with pyodbc.connect(conn_str) as conn:
        query = """
            select w.CompanyId, c.WebAddress, a.Name
            from WorkQ w
            Join GlobalReference g on w.CompanyId = g.CompanyId
            Join CompanyOperation c on w.CompanyId = c.CompanyId
            Join Account a on w.AssignedDAId = a.UserId
            where g.TypeStatus = 'true'
            and c.WebAddress is not null
            and w.ReviewedDate > '10-01-2024'
            and w.DocumentType = '202'
            and w.EventStatus = 2
            and w.EventType = 1
        """
        df = pd.read_sql(query, conn)

    print(f"Checking {len(df)} websites...")

    # Dictionary to store results
    status_dict = {}

    # Using threading to check websites in parallel
    threads = []
    for _, row in df.iterrows():
        thread = threading.Thread(target=process_row, args=(row, status_dict))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Convert status_dict to a DataFrame
    result_df = pd.DataFrame(list(status_dict.items()), columns=['CompanyId', 'Status'])

    # Filter only the domains marked "for sale"
    for_sale_df = result_df[result_df['Status'] == "Domain is for sale"]

    # Merge "for sale" statuses back to the original DataFrame
    df = df.merge(for_sale_df, on='CompanyId', how='inner')

    # Save results to Excel automatically
    output_path = r'C:\Users\pkhare\Downloads\Website Monitoring\Data from July - For Sale Only.xlsx'
    df.to_excel(output_path, index=False)
    print(f"Results saved to {output_path}")

if __name__ == "__main__":
    main()
