"""
Escriba el codigo que ejecute la accion solicitada.
"""


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import pandas as pd 
    import zipfile
    import glob
    import os
    
    os.makedirs('files/output', exist_ok=True)
    dataframes = []
    zip_files = glob.glob('files/input/*.csv.zip')
    
    for zip_path in zip_files:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_filename = zip_ref.namelist()[0]
            df = pd.read_csv(zip_ref.open(csv_filename))
            dataframes.append(df)
    full_data = pd.concat(dataframes, ignore_index=True)
    client_data = full_data[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    client_data['job'] = client_data['job'].str.replace('.', '', regex=False)
    client_data['job'] = client_data['job'].str.replace('-', '_', regex=False)
    client_data['education'] = client_data['education'].str.replace('.', '_', regex=False)
    client_data['education'] = client_data['education'].replace('unknown', pd.NA)
    client_data['credit_default'] = (client_data['credit_default'] == 'yes').astype(int)
    client_data['mortgage'] = (client_data['mortgage'] == 'yes').astype(int)
    client_data.to_csv('files/output/client.csv', index=False)
    campaign_data = full_data[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'month', 'day']].copy()
    campaign_data['previous_outcome'] = (campaign_data['previous_outcome'] == 'success').astype(int)
    campaign_data['campaign_outcome'] = (campaign_data['campaign_outcome'] == 'yes').astype(int)
    month_mapping = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    campaign_data['month_num'] = campaign_data['month'].map(month_mapping)
    campaign_data['last_contact_day'] = pd.to_datetime(
        '2022-' + campaign_data['month_num'].astype(str).str.zfill(2) + '-' + 
        campaign_data['day'].astype(str).str.zfill(2)
    ).dt.strftime('%Y-%m-%d')
    
    campaign_data = campaign_data.rename(columns={'last_contact_day': 'last_contact_date'})
    campaign_final = campaign_data[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']]
    campaign_final.to_csv('files/output/campaign.csv', index=False)
    economics_data = full_data[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    economics_data.to_csv('files/output/economics.csv', index=False)
    return


if __name__ == "__main__":
    clean_campaign_data()
