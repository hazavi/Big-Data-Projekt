# Import necessary libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dagster import asset, Definitions, Output, AssetMaterialization, job

# Step 1: Data Extraction
@asset(description="Extracts data from a CSV source file.")
def extract_data():
    data_source = os.path.join(os.path.dirname(__file__), 'world_bank_dataset.csv')
    print("Extracting data from CSV source...")
    df = pd.read_csv(data_source)
    print("Data extraction complete.")
    # Materialization log for tracking
    yield AssetMaterialization.file(data_source, description="Source data file")
    yield Output(df)

# Step 2: Data Transformation
@asset(description="Transforms data by converting GDP and Population values to trillions and millions.")
def transform_data(extract_data):
    print("Transforming data...")
    extract_data['GDP (trillions USD)'] = extract_data['GDP (USD)'] / 1e12  # Convert to trillions
    extract_data['Population (millions)'] = extract_data['Population'] / 1e6  # Convert to millions
    
    transformed_df = extract_data[['Country', 'Year', 'GDP (trillions USD)', 'Population (millions)',
                                     'Life Expectancy', 'Unemployment Rate (%)', 'CO2 Emissions (metric tons per capita)',
                                     'Access to Electricity (%)']]
    print("Data transformation complete.")
    yield Output(transformed_df)

# Step 3: Load/Save Transformed Data
@asset(description="Saves the transformed data to a new CSV file.")
def load_data(transform_data):
    destination = './load_data/transformed_data.csv'
    print("Loading data to destination CSV...")
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    transform_data.to_csv(destination, index=False)
    print(f"Data successfully loaded and saved to: {destination}")
    yield AssetMaterialization.file(destination, description="Transformed data file")
    yield Output(destination)

# Step 4: Plotting CO2 Emissions
@asset(description="Generates and displays a line plot of CO₂ emissions by country over time.")
def plot_co2_emissions(transform_data):
    print("Generating CO₂ emissions plot...")
    sns.set(style="whitegrid")
    plt.figure(figsize=(14, 8))

    # Specify the countries to plot
    countries_to_plot = ['USA', 'China', 'India', 'Germany', 'Brazil']  # Adjust these to your desired countries

    # Filter the DataFrame to include only the selected countries
    filtered_data = transform_data[transform_data['Country'].isin(countries_to_plot)]

    # Plot CO₂ emissions for each specified country
    colors = sns.color_palette("husl", len(countries_to_plot))

    for idx, country in enumerate(countries_to_plot):
        country_data = filtered_data[filtered_data['Country'] == country]
        plt.plot(
            country_data['Year'], 
            country_data['CO2 Emissions (metric tons per capita)'], 
            label=country,
            color=colors[idx], 
            linewidth=2, 
            marker='o', 
            markersize=5
        )
    
    plt.title("CO2 Emissions Over Time by Selected Countries", fontsize=16, fontweight='bold')
    plt.xlabel("Year", fontsize=12)
    plt.ylabel("CO2 Emissions (metric tons per capita)", fontsize=12)
    plt.legend(title="Country", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
    print("Plot generation complete.")

# Define all assets and dependencies
all_assets = [extract_data, transform_data, load_data, plot_co2_emissions]

# Set up Definitions for the Dagster pipeline
defs = Definitions(assets=all_assets)

# Main guard to run the pipeline
if __name__ == "__main__":
    # Execute the pipeline steps
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)
    plot_co2_emissions(transformed_data)
