import requests
import psycopg2
import time

conn = psycopg2.connect(host="localhost", dbname='postgres',
                        user="postgres", password='1234', port=5432)

cur = conn.cursor()

# APIs
supporters_url = "https://www.few-far.co/api/techtest/v1/supporters"
donations_url = "https://www.few-far.co/api/techtest/v1/donations"
donations_exports_url = "https://www.few-far.co/api/techtest/v1/donations_exports"

# API responses
supporters_response = requests.get(supporters_url)
donations_response = requests.get(donations_url)
exports_response = requests.post(donations_exports_url)


# Supporters PSQL block
if supporters_response.status_code == 200 and donations_response.status_code == 200:
    supporters_data = supporters_response.json()
    donations_data = donations_response.json()

    supporter_donations = {}
    # Create supporters table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS supporters (
                id UUID PRIMARY KEY,
                created_at DATE,
                name VARCHAR(255),
                address_1 VARCHAR(255),
                address_2 VARCHAR(255),
                city VARCHAR(255),
                postcode VARCHAR(255)
                );"""
    )

    cur.execute("""DELETE FROM supporters""")

    # Store supporters data in postgres table
    for supporter in supporters_data["data"]:
        supporter_id = supporter["id"]
        created_at = supporter["created_at"]
        supporter_name = supporter["name"]
        address_1 = supporter["address_1"]
        address_2 = supporter["address_2"]
        city = supporter["city"]
        postcode = supporter["postcode"]

        cur.execute(
            """INSERT INTO supporters (id, created_at, name, address_1, address_2, city, postcode) VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        """,
            (supporter_id, created_at, supporter_name, address_1, address_2, city, postcode))

        conn.commit()

else:
    print("Failed to retrieve data from the API.")

# Check to see if the exports API is responding
print("EXPORT RESPONSE STATUS CODE ", exports_response.status_code)
print("EXPORT RESPONSE STATUS TEXT ", exports_response.text)

# EXPORT POST BLOCK
if exports_response.status_code == 201:
    export_data = exports_response.json()
    export_id = export_data["id"]

    # Check the status of the export to see if it's ready
    export_status_url = f"https://www.few-far.co/api/techtest/v1/donations_exports/{export_id}"
    export_status_response = requests.get(export_status_url)

    print("EXPORT RESPONSE STATUS URL", export_status_url)

    if export_status_response.status_code == 200:
        export_status_data = export_status_response.json()

        print("Export status:", export_status_data["status"])

        # Wait for the export satus to be ready
        while export_status_data["status"] != "ready":
            time.sleep(10)

            export_status_data = requests.get(export_status_url).json()

            export_data_url = export_status_data["url"]
            export_data_response = requests.get(export_data_url)

            # If url returns 200 export the data and store it in a postgres table
            if export_data_response.status_code == 200:
                print("Export status:", export_status_data["status"])
                export_data = export_data_response.json()

                cur.execute("""
                CREATE TABLE IF NOT EXISTS exports (
                    id UUID PRIMARY KEY,
                    created_at DATE,
                    supporter_id UUID,
                    amount INT
                    );
                """)

                cur.execute("""DELETE FROM exports""")

            for export_entry in export_data["data"]:
                export_id = export_entry["id"]
                created_at = export_entry["created_at"]
                supporter_id = export_entry["supporter_id"]
                amount = export_entry["amount"]

                cur.execute(
                    """INSERT INTO exports (id, created_at, supporter_id, amount) VALUES
                    (%s, %s, %s, %s)
                    """,
                    (export_id, created_at, supporter_id, amount)
                )

            conn.commit()
        else:
            print("Export is pending.")
    else:
        print("Failed to retrieve export status.")
else:
    print("Failed to create donations export.")

cur.execute("""DROP TABLE IF EXISTS supporters_with_total_donations""")

# cur.execute("""
#     CREATE TABLE IF NOT EXISTS supporters_with_total_donations AS
#     SELECT
#         s.id AS supporter_id,
#         s.created_at AS supporter_created_at,
#         s.name AS supporter_name,
#         s.address_1 AS supporter_address_1,
#         s.address_2 AS supporter_address_2,
#         s.city AS supporter_city,
#         s.postcode AS supporter_postcode,
#         ROUND(COALESCE(SUM(e.amount) / 100.0, 0), 2) AS total_donations_gbp
#     FROM supporters s
#     LEFT JOIN exports e ON s.id = e.supporter_id
#     GROUP BY s.id, s.created_at, s.name, s.address_1, s.address_2, s.city, s.postcode;
# """)
# conn.commit()

cur.close()
conn.close()
