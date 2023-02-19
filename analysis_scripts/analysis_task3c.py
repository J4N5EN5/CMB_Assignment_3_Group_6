import pandas as pd
import matplotlib.pyplot as plt

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


def data_preprocessing(df):
    define_neighbor_country(df)
    df.drop([], axis=1, inplace=True)
    df['probe_continent'].fillna('NA', inplace=True)
    return df



def plot_print_rtt_by_continent(probe_type,title, df):
    fig, ax = plt.subplots()
    df.boxplot(column=['avg'], by=['probe_continent'], ax=ax, showfliers=False)

    # add a red line at the value of 30
    ax.axhline(y=30, color='r')

    # set the plot title and axis labels
    title = 'average rtt when ' + probe_type + ' probe in ' + title + ' destination'
    ax.set_title(title)
    ax.set_xlabel('Continent')
    ax.set_ylabel('average rtt (ms)')
    # show the plot
    filename = title.replace(' ', '_') + '.pdf'
    plt.savefig(filename)

    plt.show()


    grouped = df.groupby('probe_continent')
    print(title)
    # calculate the percentage of 'avg' values larger than 30 for each continent
    for name, group in grouped:
        total = group['avg'].count()
        above_30 = group[group['avg'] > 30]['avg'].count()
        percent = (above_30 / total) * 100
        print(name + ':  ' + str(round(percent,2)))



def pipeline_question3(df):
    # select rows where 'Scenario' is 'Samecountry'

    df_cellular = df.loc[df['connection_type'] == 'cellular']

    same_cty_df = df_cellular.loc[df['Scenario'] == 'SameCountry'].dropna(subset=['avg'])
    neigh_cty_df = df_cellular.loc[df['Scenario'] == 'NeighborCountry'].dropna(subset=['avg'])
    neigh_ctn_df = df_cellular.loc[df['Scenario'] == 'NeighborContinent'].dropna(subset=['avg'])

    plot_print_rtt_by_continent('cellular', 'the same country as', same_cty_df)
    plot_print_rtt_by_continent('cellular','the neighbor country with', neigh_cty_df)
    plot_print_rtt_by_continent('cellular','the neighbor continent with', neigh_ctn_df)

    df_wifi = df.loc[df['connection_type'] == 'wifi']

    same_cty_df = df_wifi.loc[df['Scenario'] == 'SameCountry'].dropna(subset=['avg'])
    neigh_cty_df = df_wifi.loc[df['Scenario'] == 'NeighborCountry'].dropna(subset=['avg'])
    neigh_ctn_df = df_wifi.loc[df['Scenario'] == 'NeighborContinent'].dropna(subset=['avg'])

    plot_print_rtt_by_continent('wifi','the same country as', same_cty_df)
    plot_print_rtt_by_continent('wifi','the neighbor country with', neigh_cty_df)
    plot_print_rtt_by_continent('wifi','the neighbor continent with', neigh_ctn_df)


    return None


def main():
    plt.clf()
    df_results = pd.read_csv(f"{measurement_path}/measurement_results.csv")
    df_results_preprocessed = data_preprocessing(df_results)

    pipeline_question3(df_results_preprocessed)


if __name__ == '__main__':
    main()
