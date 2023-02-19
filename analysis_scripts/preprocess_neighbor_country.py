import pandas as pd

measurement_path = 'results/measurement_{}'.format(49832986)

def define_neighbor_country(df):
    for i in range(len(df)):
        if df.iloc[i]['Scenario'] == 'SameContinent':
            if df.iloc[i]['probe_country'] == df.iloc[i]['Country_y']:
                df.at[i, 'Scenario'] = 'SameCountry'
            elif df.iloc[i]['Country_y'] == 'DE':
                if df.iloc[i]['probe_country'] in ['FR','CH','BE', 'NL', 'IT', 'AT', 'CZ', 'PL', 'DM']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'GB':
                if df.iloc[i]['probe_country'] in ['FR','BE', 'NL', 'IE']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'HK':
                if df.iloc[i]['probe_country'] in ['CN', 'MO', 'TW', 'VN', 'PH']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'SG':
                if df.iloc[i]['probe_country'] in ['MY', 'ID','VN']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'JP':
                if df.iloc[i]['probe_country'] in ['KR', 'CN', 'TW', 'RU', 'KP', 'HK']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'KR':
                if df.iloc[i]['probe_country'] in ['JP', 'CN', 'TW', 'RU', 'KP', 'HK']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'IN':
                if df.iloc[i]['probe_country'] in ['CN', 'PK', 'AF', 'BD', 'BT', 'NP', 'KH', 'LK']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'AU':
                if df.iloc[i]['probe_country'] in ['ID', 'TL', 'PG', 'SB', 'NU', 'VC', 'NZ']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
            elif df.iloc[i]['Country_y'] == 'BR':
                if df.iloc[i]['probe_country'] in ['AR', 'BO', 'CO', 'GY', 'PY', 'PE', 'SR', 'UY','VE']:
                    df.at[i, 'Scenario'] = 'NeighborCountry'
    return None


def main():
    df_results = pd.read_csv(f"{measurement_path}/measurement_results.csv")
    define_neighbor_country(df_results)
    df_results.to_csv(f"{measurement_path}/measurement_results_pnc.csv", index=False)


if __name__ == '__main__':
    main()