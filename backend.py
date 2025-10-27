# %%

#Step 1->Api data Extraction 
import requests

all_products = []

for page in range(1, 121):
    url = f"https://world.openfoodfacts.org/api/v2/search?categories=chocolates&fields=code,product_name,brands,nutriments&page_size=100&page={page}"
    response = requests.get(url)
    data = response.json()
    all_products.extend(data["products"])

    print(f"Page {page} done, total products: {len(all_products)}")

# %%                                                                                                                                                                                                                                                                                                                                                                                                 
print(len(all_products))
# %%
data.keys()
# %%
data['products']
# %%

import pandas as pd

df = pd.DataFrame(data['products'])

df


# %%
df['nutriments'] #json_normalize
# %%
#  expand the nested 'nutriments' column into separate columns and convert key columns to numeric
import pandas as pd

# use the full fetched list if available.
if 'all_products' in globals():
    df = pd.DataFrame(all_products)
else:
    df = pd.DataFrame(data['products'])

# Normalize nutriments (handles missing entries)
nutr_df = pd.json_normalize(df.get('nutriments', pd.Series([]))).rename(columns=lambda c: c.replace('.', '_'))

# concat and drop original nested column
df = pd.concat([df.reset_index(drop=True), nutr_df.reset_index(drop=True)], axis=1)
if 'nutriments' in df.columns:
    df = df.drop(columns=['nutriments'])

# Convert expected numeric nutriment columns to numeric types 
numeric_cols = [
    'energy-kcal_value','energy-kj_value','carbohydrates_value','sugars_value',
    'fat_value','saturated-fat_value','proteins_value','fiber_value',
    'salt_value','sodium_value','fruits-vegetables-nuts-estimate-from-ingredients_100g',
    'nutrition-score-fr','nova-group'
]
# try both raw names and with 'nutriments_' prefix produced by json_normalize
numeric_variants = set(numeric_cols + [f"nutriments_{c}" for c in numeric_cols])

for col in list(df.columns):
    if col in numeric_variants:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# quick sanity check
print("Shape:", df.shape)
print("Numeric columns converted (sample):", [c for c in df.columns if c in numeric_variants][:10])

# save intermediate for next steps
df.to_parquet("chocofacts_step_expanded.parquet", index=False)

# %%
# Step 3 — Feature Engineering
import pandas as pd

# Load previous cleaned data
df = pd.read_parquet("chocofacts_step_expanded.parquet")


# 1 Sugar to Carbohydrate Ratio ---
df["sugar_to_carb_ratio"] = df.apply(
    lambda r: (r["sugars_value"] / r["carbohydrates_value"])
    if pd.notna(r.get("sugars_value")) and pd.notna(r.get("carbohydrates_value")) and r["carbohydrates_value"] > 0
    else None,
    axis=1
)

# 2 Calorie Category (based on energy per 100g)
def calorie_category(kcal):
    if pd.isna(kcal):
        return "Unknown"
    if kcal < 250:
        return "Low Calorie"
    elif kcal < 450:
        return "Moderate Calorie"
    else:
        return "High Calorie"

df["calorie_category"] = df["energy-kcal_value"].apply(calorie_category)

# 3 Sugar Category ---
def sugar_category(sugar):
    if pd.isna(sugar):
        return "Unknown"
    if sugar < 10:
        return "Low Sugar"
    elif sugar < 30:
        return "Moderate Sugar"
    else:
        return "High Sugar"

df["sugar_category"] = df["sugars_value"].apply(sugar_category)

# 4 Ultra Processed (based on NOVA group) ---
df["is_ultra_processed"] = df["nova-group"].apply(
    lambda x: "Yes" if x == 4 else ("No" if pd.notna(x) else "Unknown")
)

#  Save the feature engineered data 
df.to_parquet("chocofacts_features.parquet", index=False)
print("Feature Engineering done!")
print("New columns added: ['sugar_to_carb_ratio', 'calorie_category', 'sugar_category', 'is_ultra_processed']")

# %%
# mysql connection
import pymysql
import pandas as pd
conn = pymysql.connect(
    host='127.0.0.1',  # localhost
    user='root',       
    password='',       
      
    port=3306
)

print("Connected successfully!")

# %%
#creating cursor
my_cursor=conn.cursor()

# %%
print(my_cursor)

# %%
#createing database
my_cursor.execute("CREATE DATABASE IF NOT EXISTS choco_crunch")
# %%
conn=pymysql.connect(
        host ="localhost",
        user="root",
        password="",
        database="choco_crunch"
    )
# %%
my_cursor=conn.cursor()
# %%
# table1 creation
my_cursor = conn.cursor()
my_cursor.execute("USE choco_crunch;")
my_cursor.execute("""
CREATE TABLE IF NOT EXISTS product_info (
    code VARCHAR(50) PRIMARY KEY,
    product_name TEXT,
    brands TEXT
)
""")
# %%
#table 2 creation
my_cursor.execute("""
CREATE TABLE IF NOT EXISTS nutrient_info(
       code VARCHAR(50) PRIMARY KEY,
    energy_kcal FLOAT,
    energy_kj FLOAT,
    carbohydrates FLOAT,
    sugars FLOAT,
    fat FLOAT,
    saturated_fat FLOAT,
    proteins FLOAT,
    fiber FLOAT,
    salt FLOAT,
    sodium FLOAT,
    nova_group INT,
    nutrition_score_fr INT,
    fruits_veg_nuts_estimate FLOAT,
    FOREIGN KEY (code) REFERENCES product_info(code)            )
                  """)
# %% table 3 creation
my_cursor.execute("""
    CREATE TABLE IF NOT EXISTS derived_metrics (
    code VARCHAR(50) PRIMARY KEY,
    sugar_to_carb_ratio FLOAT,
    calorie_category VARCHAR(50),
    sugar_category VARCHAR(50),
    is_ultra_processed VARCHAR(10),
    FOREIGN KEY (code) REFERENCES product_info(code)
)""")



# %%

import pandas as pd
import pymysql

# Load the final DataFrame with all features

df = pd.read_parquet("chocofacts_features.parquet")

# Establish connection to the MySQL database
conn = pymysql.connect(
    host="localhost",
    user="root",
    
    password="", 
    database="choco_crunch"
)
my_cursor = conn.cursor()
print("Successfully connected to the 'choco_crunch' database.")

# Loop and Insert Data into Three Tables 
insert_count = 0
for _, row in df.iterrows():
    # Retrieve and clean the primary key (code)
    code = str(row.get("code", "")).strip()
    if not code:
        continue  # for skiping rows without a valid product code

    try:
        # 1. Insert into product_info ---
        product_sql = """
        INSERT INTO product_info (code, product_name, brands)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE product_name=VALUES(product_name), brands=VALUES(brands);
        """
        # Convert NaN values to Python None for SQL
        product_name = row.get("product_name") if pd.notna(row.get("product_name")) else None
        brands = row.get("brands") if pd.notna(row.get("brands")) else None
        my_cursor.execute(product_sql, (code, product_name, brands))

        # 2. Insert into nutrient_info ---
        nutrient_sql = """
        INSERT INTO nutrient_info (code, energy_kcal, energy_kj, carbohydrates, sugars, fat, saturated_fat, proteins, fiber, salt, sodium, nova_group, nutrition_score_fr, fruits_veg_nuts_estimate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE energy_kcal=VALUES(energy_kcal), sugars=VALUES(sugars);
        """
        # Collect values using the exact column names from the data preparation step
        nutrients = [
            row.get('energy-kcal_value'), row.get('energy-kj_value'), row.get('carbohydrates_value'),
            row.get('sugars_value'), row.get('fat_value'), row.get('saturated-fat_value'),
            row.get('proteins_value'), row.get('fiber_value'), row.get('salt_value'),
            row.get('sodium_value'), row.get('nova-group'), row.get('nutrition-score-fr'),
            row.get('fruits-vegetables-nuts-estimate-from-ingredients_100g')
        ]
        # Replace pandas NaN/NaT with Python's None for SQL compatibility
        nutrients_safe = [None if pd.isna(val) else val for val in nutrients]
        my_cursor.execute(nutrient_sql, (code, *nutrients_safe))

        #  3. Insert into derived_metrics 
        derived_sql = """
        INSERT INTO derived_metrics (code, sugar_to_carb_ratio, calorie_category, sugar_category, is_ultra_processed)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE calorie_category=VALUES(calorie_category), sugar_category=VALUES(sugar_category);
        """
        derived_metrics = [
            row.get('sugar_to_carb_ratio'), row.get('calorie_category'),
            row.get('sugar_category'), row.get('is_ultra_processed')
        ]
        derived_safe = [None if pd.isna(val) else val for val in derived_metrics]
        my_cursor.execute(derived_sql, (code, *derived_safe))

        insert_count += 1
    except pymysql.Error as e:
        print(f"Skipping row with code {code} due to error: {e}")

# Commit all changes to the database
conn.commit()
print(f"Data insertion complete. Total rows processed: {insert_count}")

# Verification Queries 
my_cursor.execute("SELECT COUNT(*) FROM product_info;")
print(f"Total records in 'product_info': {my_cursor.fetchone()[0]}")

my_cursor.execute("SELECT COUNT(*) FROM nutrient_info;")
print(f"Total records in 'nutrient_info': {my_cursor.fetchone()[0]}")

my_cursor.execute("SELECT COUNT(*) FROM derived_metrics;")
print(f"Total records in 'derived_metrics': {my_cursor.fetchone()[0]}")


# %%

# 1 Count products per brand (show 'Unknown' where brand missing)
my_cursor.execute("""
SELECT 
    CASE 
        WHEN brands IS NULL OR brands = '' THEN 'Unknown Brand' 
        ELSE brands 
    END AS brand_name,
    COUNT(*) AS product_count
FROM product_info
GROUP BY brand_name
ORDER BY product_count DESC;
""")
results = my_cursor.fetchall()
for row in results:
    print(row)

# %%
# 2. Count unique products per brand 
my_cursor.execute("""
SELECT 
    CASE 
        WHEN brands IS NULL OR brands = '' THEN 'Unknown Brand' 
        ELSE brands 
    END AS brand_name,
    COUNT(DISTINCT code) AS unique_products
FROM product_info
GROUP BY brand_name
ORDER BY unique_products DESC;
""")
results = my_cursor.fetchall()
for row in results:
    print(row)

# %%
# %%
# 3. Top 5 brands by product count 
my_cursor.execute("""
SELECT 
    CASE 
        WHEN brands IS NULL OR brands = '' THEN 'Unknown Brand'
        ELSE brands
    END AS brand_name,
    COUNT(*) AS total_products
FROM product_info
GROUP BY brand_name
ORDER BY total_products DESC
LIMIT 5;
""")

results = my_cursor.fetchall()
for row in results:
    print(row)
# %%
# 4. Products with missing product name
my_cursor.execute("""
SELECT 
    code, 
    CASE 
        WHEN brands IS NULL OR brands = '' THEN 'Unknown Brand'
        ELSE brands
    END AS brand_name
FROM product_info
WHERE product_name IS NULL OR product_name = '';
""")

results = my_cursor.fetchall()
for row in results:
    print(row)

# %%
# 5. Number of unique brands
my_cursor.execute("""
SELECT COUNT(DISTINCT 
    CASE 
        WHEN brands IS NULL OR brands = '' THEN NULL
        ELSE brands
    END
) AS total_unique_brands
FROM product_info;
""")

results = my_cursor.fetchall()
for row in results:
    print("Total unique brands:", row[0])

# %%
# 6. Products with code starting with '3'
my_cursor.execute("""
SELECT 
    code, 
    CASE 
        WHEN product_name IS NULL OR product_name = '' THEN 'Unknown Product'
        ELSE product_name
    END AS product_name,
    CASE 
        WHEN brands IS NULL OR brands = '' THEN 'Unknown Brand'
        ELSE brands
    END AS brand_name
FROM product_info
WHERE code LIKE '3%';
""")

results = my_cursor.fetchall()
for row in results:
    print(row)

# %%
# 7. Top 10 products with highest calories
my_cursor.execute("""
SELECT 
    p.product_name,
    CASE 
        WHEN p.brands IS NULL OR p.brands = '' THEN 'Unknown Brand'
        ELSE p.brands
    END AS brand_name,
    n.energy_kcal
FROM nutrient_info n
JOIN product_info p ON n.code = p.code
WHERE n.energy_kcal IS NOT NULL
ORDER BY n.energy_kcal DESC
LIMIT 10;
""")

results = my_cursor.fetchall()
for row in results:
    print(row)

# %%
# 8. Average sugar per NOVA group
my_cursor.execute("""
SELECT 
    nova_group,
    ROUND(AVG(sugars), 2) AS avg_sugar
FROM nutrient_info
WHERE sugars IS NOT NULL
GROUP BY nova_group
ORDER BY nova_group;
""")

results = my_cursor.fetchall()
for row in results:
    print("NOVA Group:", row[0], "| Average Sugar:", row[1])

# %%
# 9. Count products with fat_value > 20g
my_cursor.execute("""
SELECT COUNT(*) AS high_fat_products
FROM nutrient_info
WHERE fat > 20;
""")

results = my_cursor.fetchall()
for row in results:
    print("High-fat products:", row[0])

# %%
# 10. Average carbohydrates of all chocolates
my_cursor.execute("""
SELECT ROUND(AVG(carbohydrates), 2) AS avg_carbohydrates
FROM nutrient_info
WHERE carbohydrates IS NOT NULL;
""")

results = my_cursor.fetchall()
for row in results:
    print("Average carbohydrates (g/100g):", row[0])

# %%
# 11. Products with sodium_value > 1g
my_cursor.execute("""
SELECT 
    p.product_name,
    CASE 
        WHEN p.brands IS NULL OR p.brands = '' THEN 'Unknown Brand'
        ELSE p.brands
    END AS brand_name,
    n.sodium
FROM nutrient_info n
JOIN product_info p ON n.code = p.code
WHERE n.sodium > 1;
""")

results = my_cursor.fetchall()
for row in results:
    print(row)

# %%
# 12. Products containing fruits/vegetables/nuts
my_cursor.execute("""
SELECT COUNT(*) AS products_with_fvn
FROM nutrient_info
WHERE fruits_veg_nuts_estimate > 0;
""")

results = my_cursor.fetchall()
for row in results:
    print("Products with fruits/vegetables/nuts:", row[0])

# %%
# 13. Products with energy-kcal_value > 500
my_cursor.execute("""
SELECT 
    p.product_name,
    CASE 
        WHEN p.brands IS NULL OR p.brands = '' THEN 'Unknown Brand'
        ELSE p.brands
    END AS brand_name,
    n.energy_kcal
FROM nutrient_info n
JOIN product_info p ON n.code = p.code
WHERE n.energy_kcal > 500;
""")
results = my_cursor.fetchall()
for row in results:
    print(row)
# %%
# 14. Count products per calorie_category
my_cursor.execute("""
SELECT calorie_category, COUNT(*) AS total_products
FROM derived_metrics
GROUP BY calorie_category
ORDER BY total_products DESC;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 15. Count of High Sugar products
my_cursor.execute("""
SELECT COUNT(*) AS high_sugar_products
FROM derived_metrics
WHERE sugar_category = 'High Sugar';
""")
print("High Sugar products:", my_cursor.fetchone()[0])
# %%# 16. Average sugar_to_carb_ratio for High Calorie products
my_cursor.execute("""
SELECT ROUND(AVG(sugar_to_carb_ratio), 3)
FROM derived_metrics
WHERE calorie_category = 'High Calorie';
""")
print("Average sugar_to_carb_ratio (High Calorie):", my_cursor.fetchone()[0])

# %%
# 17. Products that are both High Calorie and High Sugar
my_cursor.execute("""
SELECT p.product_name, p.brands
FROM derived_metrics d
JOIN product_info p ON d.code = p.code
WHERE d.calorie_category = 'High Calorie' AND d.sugar_category = 'High Sugar';
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 18. Number of ultra-processed products
my_cursor.execute("""
SELECT COUNT(*) AS ultra_processed_count
FROM derived_metrics
WHERE is_ultra_processed = 'Yes';
""")
print("Ultra-processed products:", my_cursor.fetchone()[0])
# %%

# 19 Products with sugar_to_carb_ratio > 0.7
my_cursor.execute("""
SELECT p.product_name, p.brands, d.sugar_to_carb_ratio
FROM derived_metrics d
JOIN product_info p ON d.code = p.code
WHERE d.sugar_to_carb_ratio > 0.7;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 20 Average sugar_to_carb_ratio per calorie_category
my_cursor.execute("""
SELECT calorie_category, ROUND(AVG(sugar_to_carb_ratio), 3) AS avg_ratio
FROM derived_metrics
GROUP BY calorie_category
ORDER BY avg_ratio DESC;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 21. Top 5 brands with most High Calorie products
my_cursor.execute("""
SELECT p.brands, COUNT(*) AS high_calorie_count
FROM derived_metrics d
JOIN product_info p ON d.code = p.code
WHERE d.calorie_category = 'High Calorie'
GROUP BY p.brands
ORDER BY high_calorie_count DESC
LIMIT 5;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 22. Average energy-kcal_value for each calorie_category
my_cursor.execute("""
SELECT d.calorie_category, ROUND(AVG(n.energy_kcal), 2) AS avg_calories
FROM derived_metrics d
JOIN nutrient_info n ON d.code = n.code
WHERE n.energy_kcal IS NOT NULL
GROUP BY d.calorie_category;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 23. Count of ultra-processed products per brand
my_cursor.execute("""
SELECT p.brands, COUNT(*) AS ultra_processed_count
FROM derived_metrics d
JOIN product_info p ON d.code = p.code
WHERE d.is_ultra_processed = 'Yes'
GROUP BY p.brands
ORDER BY ultra_processed_count DESC;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 24. Products with High Sugar and High Calorie (with brand)
my_cursor.execute("""
SELECT p.product_name, p.brands, d.calorie_category, d.sugar_category
FROM derived_metrics d
JOIN product_info p ON d.code = p.code
WHERE d.calorie_category = 'High Calorie' AND d.sugar_category = 'High Sugar';
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# 25. Average sugar content per brand for ultra-processed products
my_cursor.execute("""
SELECT p.brands, ROUND(AVG(n.sugars), 2) AS avg_sugar
FROM product_info p
JOIN nutrient_info n ON p.code = n.code
JOIN derived_metrics d ON p.code = d.code
WHERE d.is_ultra_processed = 'Yes'
GROUP BY p.brands
ORDER BY avg_sugar DESC
LIMIT 10;
""")
for row in my_cursor.fetchall():
    print(row)



# %%
# 26. Number of products with fruits/vegetables/nuts per calorie_category
my_cursor.execute("""
SELECT d.calorie_category, COUNT(*) AS products_with_fvn
FROM nutrient_info n
JOIN derived_metrics d ON n.code = d.code
WHERE n.fruits_veg_nuts_estimate > 0
GROUP BY d.calorie_category;
""")
for row in my_cursor.fetchall():
    print(row)
# %%

# 27/ Top 5 products by sugar_to_carb_ratio with their calorie and sugar category
my_cursor.execute("""
SELECT p.product_name, p.brands, ROUND(d.sugar_to_carb_ratio, 3), d.calorie_category, d.sugar_category
FROM derived_metrics d
JOIN product_info p ON d.code = p.code
WHERE d.sugar_to_carb_ratio IS NOT NULL
ORDER BY d.sugar_to_carb_ratio DESC
LIMIT 5;
""")
for row in my_cursor.fetchall():
    print(row)
# %%
# %%
# Step 6 — Exploratory Data Analysis (EDA)

from matplotlib import pyplot as plt
import pandas as pd

# Load the feature-engineered dataset
df = pd.read_parquet("chocofacts_features.parquet")

#  1. Summary statistics 
print("Summary Statistics:")
print(df.describe(include="all"))

#  2 .Missing Values 
missing = df.isnull().sum().sort_values(ascending=False)
print("\nMissing Values per Column:")
print(missing.head(15))

plt.figure(figsize=(10, 4))
plt.bar(missing.index[:15], missing.values[:15], color='chocolate')
plt.xticks(rotation=90)
plt.title("Top 15 Columns with Missing Values")
plt.xlabel("Columns")
plt.ylabel("Missing Count")
plt.show()

# 3 Calorie Category Distribution 
plt.figure(figsize=(6, 4))
df["calorie_category"].value_counts().plot(kind="bar", color=['#F4A261', '#E76F51', '#2A9D8F'])
plt.title("Distribution of Calorie Categories")
plt.xlabel("Calorie Category")
plt.ylabel("Number of Products")
plt.show()

#  4 Sugar Category Distribution 
plt.figure(figsize=(6, 4))
df["sugar_category"].value_counts().plot(kind="bar", color=['#264653', '#E9C46A', '#E76F51'])
plt.title("Distribution of Sugar Categories")
plt.xlabel("Sugar Category")
plt.ylabel("Number of Products")
plt.show()

#  5 Calories vs Sugars 
plt.figure(figsize=(7, 5))
plt.scatter(df["energy-kcal_value"], df["sugars_value"], alpha=0.6, color='brown')
plt.title("Calories vs Sugars (per 100g)")
plt.xlabel("Energy (kcal)")
plt.ylabel("Sugars (g)")
plt.show()

#  6. Top 10 Brands by Product Count 
top_brands = df["brands"].value_counts().head(10)
plt.figure(figsize=(8, 4))
plt.barh(top_brands.index[::-1], top_brands.values[::-1], color='peru')
plt.title("Top 10 Brands by Product Count")
plt.xlabel("Number of Products")
plt.ylabel("Brand")
plt.show()

#  7 Sugar-to-Carb Ratio Distribution 
plt.figure(figsize=(6, 4))
plt.hist(df["sugar_to_carb_ratio"].dropna(), bins=25, color='sienna', edgecolor='black')
plt.title("Distribution of Sugar-to-Carbohydrate Ratio")
plt.xlabel("Sugar-to-Carb Ratio")
plt.ylabel("Frequency")
plt.show()

#  8 Ultra-Processed vs Non-Processed Products ---
plt.figure(figsize=(5, 4))
df["is_ultra_processed"].value_counts().plot(kind="bar", color=['#E76F51', '#2A9D8F', '#E9C46A'])
plt.title("Ultra-Processed vs Non-Processed Products")
plt.xlabel("Category")
plt.ylabel("Count")
plt.show()

#  9 Average Calories by Sugar Category ---
avg_cal = df.groupby("sugar_category")["energy-kcal_value"].mean().reindex(["Low Sugar", "Moderate Sugar", "High Sugar"])
plt.figure(figsize=(6, 4))
plt.bar(avg_cal.index, avg_cal.values, color='chocolate')
plt.title("Average Calories by Sugar Category")
plt.xlabel("Sugar Category")
plt.ylabel("Average Energy (kcal/100g)")
plt.show()

# 10 Correlation Between Nutrients ---
num_cols = [
    "energy-kcal_value",
    "carbohydrates_value",
    "sugars_value",
    "fat_value",
    "saturated-fat_value",
    "proteins_value",
    "fiber_value",
    "salt_value"
]
corr = df[num_cols].corr()

plt.figure(figsize=(8, 6))
plt.imshow(corr, cmap="YlOrBr", interpolation="nearest")
plt.colorbar(label="Correlation")
plt.xticks(range(len(num_cols)), num_cols, rotation=45, ha="right")
plt.yticks(range(len(num_cols)), num_cols)
plt.title("Correlation Between Key Nutrients")
plt.show()

print("  EDA completed successfully!")

