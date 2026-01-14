
# STEP 6 — Streamlit Dashboard 

import streamlit as st
import pandas as pd
from matplotlib import pyplot as plt

st.set_page_config(page_title="ChocoCrunch Analytics", layout="wide")
st.title("ChocoCrunch Analytics Dashboard")

# Load the feature-engineered dataset
try:
    df = pd.read_parquet("chocofacts_features.parquet")
    st.success("Feature-engineered dataset loaded successfully.")
except Exception as e:
    st.error(f"Error loading dataset: {e}")
    df = pd.DataFrame()

# 1. Summary Statistics
st.subheader("1. Summary Statistics")
if not df.empty:
    st.write(df.describe(include='all'))
else:
    st.warning("Dataset not found. Please run the backend first.")

# 2. Missing Values per Column
st.subheader("2. Missing Values per Column")
if not df.empty:
    missing = df.isnull().sum().sort_values(ascending=False)
    st.bar_chart(missing.head(15))
else:
    st.warning("No data available to show missing values.")

# 3. Calorie Category Distribution
st.subheader("3. Calorie Category Distribution")
if "calorie_category" in df.columns:
    cal_counts = df["calorie_category"].value_counts()
    st.bar_chart(cal_counts)
else:
    st.warning("Column 'calorie_category' not found in dataset.")

# 4. Sugar Category Distribution
st.subheader("4. Sugar Category Distribution")
if "sugar_category" in df.columns:
    sug_counts = df["sugar_category"].value_counts()
    st.bar_chart(sug_counts)
else:
    st.warning("Column 'sugar_category' not found in dataset.")

# 5. Calories vs Sugars (per 100g)
st.subheader("5. Calories vs Sugars (per 100g)")
if not df.empty and "energy-kcal_value" in df.columns and "sugars_value" in df.columns:
    st.scatter_chart(df[["energy-kcal_value", "sugars_value"]])
else:
    st.warning("Columns for calories or sugars not found.")

# 6. Top 10 Brands by Product Count
st.subheader("6. Top 10 Brands by Product Count")
if "brands" in df.columns:
    top_brands = df["brands"].value_counts().head(10)
    st.bar_chart(top_brands)
else:
    st.warning("Column 'brands' not found in dataset.")

# 7. Distribution of Sugar-to-Carbohydrate Ratio
st.subheader("7. Distribution of Sugar-to-Carbohydrate Ratio")
if "sugar_to_carb_ratio" in df.columns:
    plt.figure(figsize=(6,4))
    plt.hist(df["sugar_to_carb_ratio"].dropna(), bins=25, edgecolor='black')
    plt.xlabel("Sugar-to-Carbohydrate Ratio")
    plt.ylabel("Frequency")
    st.pyplot(plt)
else:
    st.warning("Column 'sugar_to_carb_ratio' not found in dataset.")

# 8. Ultra-Processed vs Non-Processed Products
st.subheader("8. Ultra-Processed vs Non-Processed Products")
if "is_ultra_processed" in df.columns:
    proc_counts = df["is_ultra_processed"].value_counts()
    st.bar_chart(proc_counts)
else:
    st.warning("Column 'is_ultra_processed' not found in dataset.")

# 9. Average Calories by Sugar Category
st.subheader("9. Average Calories by Sugar Category")
if "energy-kcal_value" in df.columns and "sugar_category" in df.columns:
    avg_cal = df.groupby("sugar_category")["energy-kcal_value"].mean()
    st.bar_chart(avg_cal)
else:
    st.warning("Required columns not found for average calories.")

# 10. Correlation Between Key Nutrients
st.subheader("10. Correlation Between Key Nutrients")
num_cols = [
    "energy-kcal_value", "carbohydrates_value", "sugars_value",
    "fat_value", "saturated-fat_value", "proteins_value",
    "fiber_value", "salt_value"
]
if all(col in df.columns for col in num_cols):
    corr = df[num_cols].corr()
    st.dataframe(corr)
else:
    st.warning("Not all nutrient columns found for correlation matrix.")

