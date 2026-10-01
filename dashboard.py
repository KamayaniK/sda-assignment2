import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Food Delivery Streaming Analytics",
    layout="wide"
)

# Load consumed Kafka data
df = pd.read_csv("consumed_orders.csv")

# Keep one record per order for analysis
orders = df[df["event_type"] == "ORDER_DELIVERED"].copy()

# Convert numeric columns
orders["time_taken_min"] = pd.to_numeric(orders["time_taken_min"], errors="coerce")
orders["delivery_person_rating"] = pd.to_numeric(
    orders["delivery_person_rating"], errors="coerce"
)

# -----------------------------
# Dashboard Title
# -----------------------------
st.title("🍔 Food Delivery Streaming Analytics")
st.subheader("Real-Time Order & Delivery Performance Dashboard")

st.write(
    "Analysis of food-delivery events consumed from Apache Kafka "
    "using the food-delivery-orders topic."
)

# -----------------------------
# KPI Cards
# -----------------------------
total_orders = orders["order_id"].nunique()
avg_delivery_time = orders["time_taken_min"].mean()
avg_rating = orders["delivery_person_rating"].mean()
delivered_orders = len(orders)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Orders", total_orders)
col2.metric("Avg Delivery Time", f"{avg_delivery_time:.1f} min")
col3.metric("Avg Delivery Rating", f"{avg_rating:.2f}")
col4.metric("Delivered Orders", delivered_orders)

st.divider()

# -----------------------------
# Chart 1: Delivery Time by City
# -----------------------------
st.subheader("1. Average Delivery Time by City")

city_data = (
    orders.groupby("city")["time_taken_min"]
    .mean()
    .sort_values(ascending=False)
)

st.bar_chart(city_data)

# -----------------------------
# Chart 2: Delivery Time by Traffic
# -----------------------------
st.subheader("2. Average Delivery Time by Traffic Density")

traffic_data = (
    orders.groupby("road_traffic_density")["time_taken_min"]
    .mean()
    .sort_values(ascending=False)
)

st.bar_chart(traffic_data)

# -----------------------------
# Chart 3: Delivery Time by Weather
# -----------------------------
st.subheader("3. Average Delivery Time by Weather Condition")

weather_data = (
    orders.groupby("weather_conditions")["time_taken_min"]
    .mean()
    .sort_values(ascending=False)
)

st.bar_chart(weather_data)

# -----------------------------
# Chart 4: Delivery Time by Vehicle
# -----------------------------
st.subheader("4. Average Delivery Time by Vehicle Type")

vehicle_data = (
    orders.groupby("vehicle_type")["time_taken_min"]
    .mean()
    .sort_values(ascending=False)
)

st.bar_chart(vehicle_data)

# -----------------------------
# Business Insights
# -----------------------------
st.divider()
st.subheader("Business Insights")

highest_traffic = traffic_data.index[0]
highest_traffic_time = traffic_data.iloc[0]

fastest_traffic = traffic_data.index[-1]
fastest_traffic_time = traffic_data.iloc[-1]

st.write(
    f"• **Traffic impact:** Orders under **{highest_traffic}** traffic "
    f"had the highest average delivery time of approximately "
    f"**{highest_traffic_time:.1f} minutes**."
)

st.write(
    f"• **Traffic comparison:** **{fastest_traffic}** traffic had the "
    f"lowest average delivery time at approximately "
    f"**{fastest_traffic_time:.1f} minutes**."
)

highest_city = city_data.index[0]

st.write(
    f"• **City-level performance:** **{highest_city}** recorded the "
    f"highest average delivery time among the cities in the consumed data."
)

highest_weather = weather_data.index[0]

st.write(
    f"• **Weather impact:** **{highest_weather}** conditions were "
    f"associated with the highest average delivery time in the dataset."
)

st.divider()

st.caption(
    "Data source: Zomato food-delivery dataset | "
    "Streaming source: Apache Kafka | "
    "Events consumed: 300"
)