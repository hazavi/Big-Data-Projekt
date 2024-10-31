# Importér nødvendige biblioteker
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ProcessPoolExecutor
import os

def extract_data(source):
    print("Extracting data...")
    df = pd.read_csv(source)
    print("Data extracted.")
    return df

def transform_data(df):
    print("Transforming data...")
    df['GDP (trillions USD)'] = df['GDP (USD)'] / 1e12  # Konverter til billioner
    df['Population (millions)'] = df['Population'] / 1e6  # Konverter til millioner
    transformed_df = df[['Country', 'Year', 'GDP (trillions USD)', 'Population (millions)',
                         'Life Expectancy', 'Unemployment Rate (%)', 'CO2 Emissions (metric tons per capita)',
                         'Access to Electricity (%)']]
    print("Data transformed.")
    return transformed_df

def load_data(df, destination):
    print("Loading data...")
    os.makedirs(os.path.dirname(destination), exist_ok=True)  # Sørg for mappen findes
    df.to_csv(destination, index=False)
    print(f"Data loaded and saved to: {destination}")

def plot_co2_emissions(df):
    print("Generating plot...")
    sns.set(style="whitegrid")
    plt.figure(figsize=(14, 8))

    countries = df['Country'].unique()
    colors = sns.color_palette("husl", len(countries))

    for idx, country in enumerate(countries):
        country_data = df[df['Country'] == country]
        plt.plot(
            country_data['Year'], 
            country_data['CO2 Emissions (metric tons per capita)'], 
            label=country,
            color=colors[idx], 
            linewidth=2, 
            marker='o', 
            markersize=5
        )
    
    plt.title("CO2 Emissions Over Time by Country", fontsize=16, fontweight='bold')
    plt.xlabel("Year", fontsize=12)
    plt.ylabel("CO2 Emissions (metric tons per capita)", fontsize=12)
    plt.legend(title="Country", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
    print("Plot generated.")

def elt_pipeline(data_source, destination):
    # Start multiprocessering for hvert trin
    with ProcessPoolExecutor() as executor:
        # Extract
        future_extract = executor.submit(extract_data, data_source)
        df = future_extract.result()
        
        # Transform
        future_transform = executor.submit(transform_data, df)
        transformed_df = future_transform.result()
        
        # Load
        future_load = executor.submit(load_data, transformed_df, destination)
        future_load.result()
        
        # Plot
        executor.submit(plot_co2_emissions, transformed_df)

if __name__ == "__main__":
    # Definer data kilde og destination
    data_source = 'world_bank_dataset.csv'
    destination = './load_data/processed_data.csv'
    
    # Kør ELT-pipeline
    elt_pipeline(data_source, destination)
