## Dataset
Dataset jeg har brugt i dette projekt er [World Bank](https://www.kaggle.com/datasets/bhadramohit/world-bank-dataset) fra ``Kaggle``

Dette datasæt viser vigtige økonomiske, sociale og miljømæssige tal for 20 lande i fra 2010 til 2019.

## Installationsguide

1. **Installer Rust og Dagster**

   Start med at installere [Rust](https://rustup.rs/), og derefter installere Dagster via `pip`. 
   ```bash
   winget install rustup
   winget install cargo
   pip install dagster dagster-webserver
   ```
   Se: [Dagster Docs](https://docs.dagster.io/getting-started/install)

2. **Opret et Nyt Projekt**

   Når Dagster er installeret, kan du oprette et nyt projekt.
   ```bash
   pip install dagster
   dagster project scaffold --name my-dagster-project
   ```
   Erstat `my-dagster-project` med det ønskede navn på dit projekt.

3. **Installere project dependencies**

   Skift til projektmappen:
   ```bash
   cd mit_dagster_projekt
   ```
   Installere package og Python dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. **Kør og Starte Dagster UI**

   Start Dagster Ui:
   ```bash
   dagster dev
   ```
   eller
   ```bash
   python -m dagster dev
   ```   
   Dette starter en lokal server. Som standard kører den på `http://localhost:3000`.

5. **Opret Din Første Pipeline**

   Du kan oprette en simpel pipeline ved at definere aktiver i mappen `mit_dagster_projekt`. Åbn filen `assets.py` og tilføj dine pipeline-definitioner, som vist i kodeeksemplet nedenfor.


## Eksempel på Pipeline Kode

Her er et grundlæggende eksempel på, hvordan du kan definere en pipeline i dit projekt:

```python
from dagster import asset, job

@asset
def extract_data():
    # Din dataudtræk-logik
    pass

@asset
def transform_data(extract_data):
    # Din datatransformations-logik
    pass

@asset
def load_data(transform_data):
    # Din dataloading-logik
    pass

@job
def my_pipeline():
    load_data(transform_data(extract_data()))
```

## ETL Data-pipeline for World Bank

### 1. Import af Biblioteker
```python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dagster import asset, Definitions, Output, AssetMaterialization, job
```
- **pandas**: for data manipulation.
- **matplotlib og seaborn**: Til at lave visualiseringer (som grafer).
- **os**: Et modul til at interagere med operativsystemet, som bruges til at håndtere filstier.
- **dagster**: Til at definere datapipeline trin.

### 2. Extract
```python
@asset(description="Extractor data fra en CSV kildefil.")
def extract_data():
    data_source = os.path.join(os.path.dirname(__file__), 'world_bank_dataset.csv')
    print("Henter data fra CSV kilde...")
    df = pd.read_csv(data_source)
    print("Dataudtræk færdig.")
    yield AssetMaterialization.file(data_source, description="Kilde datafil")
    yield Output(df)
```
- Denne funktion henter data fra en CSV-fil **`world_bank_dataset.csv`**.
- Den læser data ind i en DataFrame (`df`) ved hjælp af **`pandas`**.
- Den skriver beskeder for at vise, hvornår dataudtrækket starter og slutter.
- Den bruger **`yield`** til at sende to resultater: en log af kildefilen og den udtrukne data selv.

### 3. Transform
```python
@asset(description="Transformer data ved at konvertere BNP og befolkningsværdier til billioner og millioner.")
def transform_data(extract_data):
    print("Transformerer data...")
    extract_data['GDP (trillions USD)'] = extract_data['GDP (USD)'] / 1e12  # Konverter til billioner
    extract_data['Population (millions)'] = extract_data['Population'] / 1e6  # Konverter til millioner
    
    transformed_df = extract_data[['Country', 'Year', 'GDP (trillions USD)', 'Population (millions)',
                                     'Life Expectancy', 'Unemployment Rate (%)', 'CO2 Emissions (metric tons per capita)',
                                     'Access to Electricity (%)']]
    print("Datatransformation færdig.")
    yield Output(transformed_df)
```
- Denne funktion tager den udtrukne data og transformer den.
- Den konverterer BNP-værdier fra USD til billioner og befolkningsværdier til millioner.
- Den vælger specifikke kolonner at beholde i den transformerede DataFrame og skriver beskeder om processen.
- Den sender den transformerede data ud ved hjælp af **`yield`**.

### 4. Load/Save Transformed Data
```python
@asset(description="Gemmer de transformerede data i en ny CSV-fil.")
def load_data(transform_data):
    destination = './load_data/processed_data.csv'
    print("Indlæser data til destination CSV...")
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    transform_data.to_csv(destination, index=False)
    print(f"Data er succesfuldt indlæst og gemt i: {destination}")
    yield AssetMaterialization.file(destination, description="Transformeret datafil")
    yield Output(destination)
```
- Denne funktion gemmer de transformerede data i en ny CSV-fil.
- Den opretter et bibliotek kaldet **`load_data`**, hvis det ikke allerede findes, og skriver de transformerede data til **`processed_data.csv`**.
- Den skriver beskeder om indlæsningen af data og bekræfter filstien.
- Den yield'er en log af den nye fil og filstien selv.

### 5. Plotting af CO2-Emissioner
```python
@asset(description="Genererer og viser et linjediagram over CO₂-emissioner efter land over tid.")
def plot_co2_emissions(transform_data):
    print("Genererer CO₂-emissionsplot...")
    sns.set(style="whitegrid")
    plt.figure(figsize=(14, 8))

    countries_to_plot = ['USA', 'China', 'India', 'Germany', 'Brazil']
    filtered_data = transform_data[transform_data['Country'].isin(countries_to_plot)]

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
    
    plt.title("CO2-emissioner over tid for udvalgte lande", fontsize=16, fontweight='bold')
    plt.xlabel("År", fontsize=12)
    plt.ylabel("CO2-emissioner (metrisk tons per indbygger)", fontsize=12)
    plt.legend(title="Land", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
    print("Plotgenerering færdig.")
```
- Denne funktion laver et linjediagram, der viser CO₂-emissioner for udvalgte lande over tid.
- Den filtrerer de transformerede data for kun at inkludere de angivne lande (USA, Kina, Indien, Tyskland, Brasilien).
- Den bruger **`seaborn`** og **`matplotlib`** til at lave og vise diagrammet.
- Den skriver beskeder


### 6. Hovedbeskyttelse
```python
# Main guard to run the pipeline
if __name__ == "__main__":
    # Execute the pipeline steps
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)
    plot_co2_emissions(transformed_data)
```
- **Main guard** tjekker, om scriptet kører direkte (ikke importeres som et modul).
- Den udfører hver funktion i rækkefølge: henter data, transformerer det, indlæser det og plotter udledningerne.


