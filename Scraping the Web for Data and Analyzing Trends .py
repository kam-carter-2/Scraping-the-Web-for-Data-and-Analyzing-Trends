import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Step 1: Scrape Data
url = "https://www.worldometers.info/co2-emissions/co2-emissions-by-country/"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

table = soup.find("table")
rows = table.find_all("tr")

data = []
for row in rows[1:]:
    cols = row.find_all("td")
    if len(cols) >= 6:
        country = cols[1].text.strip()
        emissions = cols[2].text.strip().replace(",", "")
        year = 2022  # Assuming the year of data
        try:
            emissions = float(emissions)
        except ValueError:
            emissions = None
        data.append([country, year, emissions])

# Step 2: Save to CSV
df = pd.DataFrame(data, columns=["country", "year", "emission_value"])
df.to_csv("emissions_raw.csv", index=False)

# Step 3: Save to SQLite DB (with 2 tables)
conn = sqlite3.connect("emissions.db")
cursor = conn.cursor()

# Drop tables if re-running
cursor.execute("DROP TABLE IF EXISTS countries")
cursor.execute("DROP TABLE IF EXISTS emissions")

cursor.execute("""
CREATE TABLE countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE emissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_id INTEGER,
    year INTEGER,
    emission_value REAL,
    FOREIGN KEY (country_id) REFERENCES countries(id)
)
""")

# Insert data into tables
for country, year, value in data:
    cursor.execute("INSERT OR IGNORE INTO countries (name) VALUES (?)", (country,))
    cursor.execute("SELECT id FROM countries WHERE name = ?", (country,))
    country_id = cursor.fetchone()[0]
    cursor.execute("INSERT INTO emissions (country_id, year, emission_value) VALUES (?, ?, ?)",
                   (country_id, year, value))

conn.commit()

# Step 4: Clean Data (e.g., replace NULLs with 0)
cursor.execute("UPDATE emissions SET emission_value = 0 WHERE emission_value IS NULL")
conn.commit()

# Step 5: Export Cleaned Data to CSV
query = """
SELECT countries.name AS country, emissions.year, emissions.emission_value
FROM emissions
JOIN countries ON emissions.country_id = countries.id
"""
df_cleaned = pd.read_sql_query(query, conn)
df_cleaned.to_csv("emissions_cleaned.csv", index=False)

# Step 6: Descriptive Statistics
print("\n--- Descriptive Statistics ---")
print(df_cleaned.describe())

# Step 7: Visualizations

# Top 10 emitters
top_emitters = df_cleaned.sort_values("emission_value", ascending=False).head(10)
plt.figure(figsize=(10, 6))
plt.bar(top_emitters["country"], top_emitters["emission_value"])
plt.title("Top 10 CO₂ Emitters (2022)")
plt.ylabel("Emissions (Metric Tons)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("top_emitters_bar.png")
plt.show()

# Line graph of global emissions over time
# If you have multiple years, uncomment this and adapt accordingly:
# emissions_over_time = df_cleaned.groupby("year")["emission_value"].sum()
# plt.plot(emissions_over_time.index, emissions_over_time.values)
# plt.title("Global CO₂ Emissions Over Time")
# plt.xlabel("Year")
# plt.ylabel("Emissions")
# plt.show()

# Pie chart (top 5 countries)
top5 = top_emitters.head(5)
plt.figure(figsize=(6, 6))
plt.pie(top5["emission_value"], labels=top5["country"], autopct="%1.1f%%")
plt.title("CO₂ Share Among Top 5 Emitters")
plt.savefig("top5_pie.png")
plt.show()

conn.close()
