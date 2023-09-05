import requests
import psycopg2

conn = psycopg2.connect(host="localhost", dbname='postgres',
                        user="postgres", password='1234', port=5432)

cur = conn.cursor()

supporters_url = "https://www.few-far.co/api/techtest/v1/supporters"
donations_url = "https://www.few-far.co/api/techtest/v1/donations"
donations_exports_url = "https://www.few-far.co/api/techtest/v1/donations_exports"

supporters_response = requests.get(supporters_url)
donations_response = requests.get(donations_url)
exports_response = requests.post(donations_exports_url)

if supporters_response.status_code == 200 and donations_response.status_code == 200:
    supporters_data = supporters_response.json()
    donations_data = donations_response.json()

    supporter_donations = {}

    cur.execute("""DROP TABLE IF EXISTS supporters""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS supporters (
                id UUID PRIMARY KEY,
                name VARCHAR(255),
                donations INT);"""

    )
    # Calculate total donations for each supporter
    for donation in donations_data["data"]:
        supporter_id = donation["supporter_id"]
        donation_amount = donation["amount"]
        supporter_donations.setdefault(supporter_id, 0)
        supporter_donations[supporter_id] += donation_amount

    # Print or store the results
    for supporter in supporters_data["data"]:
        supporter_id = supporter["id"]
        supporter_name = supporter["name"]
        total_donation = supporter_donations.get(supporter_id, 0)
        # print(
        # f"Supporter: {supporter_name}, Total Donation: {total_donation / 100} GBP")

        cur.execute(
            """INSERT INTO supporters (id, name, donations) VALUES
            (%s, %s, %s)
        """,
            (supporter_id, supporter_name, total_donation))

        conn.commit()

else:
    print("Failed to retrieve data from the API.")

print(exports_response.status_code)
print(exports_response.text)

if exports_response.status_code == 201:
    export_data = exports_response.json()
    export_id = export_data["id"]

    # Check the status of the export to see if it's ready
    export_status_url = f"https://www.few-far.co/api/techtest/v1/donations_exports/{export_id}"
    export_status_response = requests.get(export_status_url)

    if export_status_response.status_code == 200:
        export_status_data = export_status_response.json()
        if export_status_data["status"] == "ready":
            # The export is ready, retrieve and process the export data
            export_data = export_status_data["data"]
            # Process the export data as needed
        else:
            print("Export is still pending.")
    else:
        print("Failed to retrieve export status.")
else:
    print("Failed to create donations export.")


cur.close()
conn.close()
