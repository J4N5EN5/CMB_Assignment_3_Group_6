import pandas as pd
from ast import literal_eval
import numpy as np
from matplotlib import pyplot as plt
import statsmodels.api as sm

measurement_path = 'measurement_{}'.format(49924586)


def data_preprocessing(df):
    df['Continent_y'] = np.where(df['Country_y'] == 'US', 'NA', df['Continent_y'])
    df['Continent_y'] = np.where(df['Country_y'] == 'CA', 'NA', df['Continent_y'])
    df.drop(['af', 'proto', 'step', 'type', 'fw', 'mver', 'dst_name', 'stored_timestamp', 'size', 'lts',
             'src_addr', 'from', 'min', 'max', 'avg', 'dst_addr', 'msm_id', 'index', 'msm_name', 'Continent_x',
             'Country_x', 'City_x'], axis=1, inplace=True)
    df['result'] = df['result'].apply(lambda x: [d.values() for d in literal_eval(x)])
    df['result'] = [sum([list(x) for x in y], []) for y in df['result']]
    number_of_irregular_probes = len(df[df['sent'] != 3])
    number_of_irregular_probes += len(df[df['rcvd'] != 3])
    irregular_probes = pd.concat([df[df['sent'] != 3], df[df['rcvd'] != 3]], axis=0)
    df = df[df['sent'] == 3]
    df = df[df['rcvd'] == 3]
    df['mean'] = df['result'].apply(np.median)
    df['std'] = df['result'].apply(np.std)

    return df, number_of_irregular_probes, irregular_probes


def pipeline_question1(df):
    dfg = df.groupby(['prb_id', 'AU', 'Scenario', 'connection_type']).agg(Mean=('mean', np.median), Std=('std', np.std),
                                                                          Country=('Country_y', lambda z: z.iloc[0]),
                                                                          Continent=(
                                                                              'Continent_y', lambda z: z.iloc[0]),
                                                                          TS=('timestamp', lambda z: z.iloc[0]),
                                                                          Provider=('Provider',
                                                                                    lambda z: z.iloc[0])).reset_index()

    continents = ['NA', 'EU', 'OC', 'AS', 'SA']
    scenarios = ['SameCountry']
    df = df.sort_values(by='timestamp')

    for continent in continents:
        for scenario in scenarios:
            fig, ax = plt.subplots(figsize=(8, 9))
            g_temp = df[df['Scenario'] == scenario]

            g_temp = g_temp[g_temp['Continent_y'] == continent]
            g_temp = g_temp.groupby(['connection_type'])

            for group, group_df in g_temp:
                if group_df.empty:
                    continue
                group_df['timestamp'] = pd.to_datetime(group_df['timestamp'], unit='s')
                group_df.set_index('timestamp', inplace=True, drop=False)
                group_df.index = pd.to_datetime(group_df.index)
                df_hourly = group_df.resample('4H').mean()

                plt.plot(df_hourly.index, df_hourly['mean'], label=group)

            ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
            plt.title(' Comparison of Access Technologies Over Time')
            plt.suptitle("Continent: {}".format(continent))
            plt.ylabel('RTT')
            plt.legend()
            plt.savefig('plots/TimeComp_{}_{}.pdf'.format(continent, scenario))
            plt.close(fig)

    scenarios = ['SameCountry', 'NeighborCountry', 'NeighborContinent', 'OtherContinent']

    for continent in continents:
        for scenario in scenarios:
            fig, ax = plt.subplots(figsize=(8, 6))
            g_temp = dfg[dfg['Scenario'] == scenario]

            g_temp = g_temp[g_temp['Continent'] == continent]
            g_temp = g_temp.groupby(['connection_type'])

            for group, group_df in g_temp:
                if group_df.empty:
                    continue
                ecdf = sm.distributions.ECDF(group_df['Mean'])
                x = np.linspace(min(group_df['Mean']), max(group_df['Mean']))
                y = ecdf(x)
                ax.step(x, y, label=[continent, scenario, group])

            ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')

            plt.title('CDF - Comparison of Access Technologies Latencies')
            plt.suptitle("Continent: {}".format(continent))
            plt.xlabel('RTT (ms)')
            plt.ylabel('Cumulative Probability')
            plt.legend()
            plt.savefig('plots/ContinentComp_{}_{}.pdf'.format(continent, scenario))
            plt.close(fig)


def pipeline_question2(df):
    dfg = df.groupby(['prb_id', 'AU', 'Scenario', 'connection_type']).agg(Mean=('mean', np.median), Std=('std', np.std),
                                                                          Country=('Country_y', lambda z: z.iloc[0]),
                                                                          Continent=(
                                                                              'Continent_y', lambda z: z.iloc[0]),
                                                                          TS=('timestamp', lambda z: z.iloc[0]),
                                                                          Provider=('Provider',

                                                                                    lambda z: z.iloc[0])).reset_index()
    return None


def pipeline_question3(base_df, df, irregularities, irregular_probes):
    dfg = df.groupby(['prb_id', 'AU', 'Scenario', 'connection_type']).agg(Mean=('mean', np.median), Std=('std', np.std),
                                                                          Country=('Country_y', lambda z: z.iloc[0]),
                                                                          Continent=(
                                                                              'Continent_y', lambda z: z.iloc[0]),
                                                                          TS=('timestamp', lambda z: z.iloc[0]),
                                                                          Provider=('Provider',
                                                                                    lambda z: z.iloc[0])).reset_index()
    complete_counts = base_df.groupby(['Country_y'])
    country_counts = df['Country_y'].value_counts()
    for group, group_df in complete_counts:
        if group_df.empty:
            continue
        rows_in_group = group_df.shape[0]
        country_counts[group] = country_counts[group] / rows_in_group
    plt.pie(country_counts, labels=country_counts.index, autopct='%1.1f%%')
    # irregular_probes.plot(kind='pie', y='Country_y', labels=df['Country_y'], autopct='%1.1f%%')
    plt.axis('equal')
    plt.title('Normalized Share of Irregularities by Country')
    plt.savefig("plots/irregularities_by_country.pdf")
    plt.clf()

    complete_counts = base_df.groupby(['Provider'])
    provider_counts = df['Provider'].value_counts()
    for group, group_df in complete_counts:
        if group_df.empty:
            continue
        rows_in_group = group_df.shape[0]
        provider_counts[group] = provider_counts[group] / rows_in_group
    plt.pie(provider_counts, labels=provider_counts.index, autopct='%1.1f%%')
    # irregular_probes.plot(kind='pie', y='Country_y', labels=df['Country_y'], autopct='%1.1f%%')
    plt.axis('equal')
    plt.title('Normalized Share of Irregularities by Provider')
    plt.savefig("plots/irregularities_by_Provider.pdf")
    plt.clf()

    grp_provider = df.groupby(['Provider', 'connection_type']).agg({'result': lambda x: sum(list(x), [])})
    grp_provider['Mean'] = grp_provider['result'].apply(lambda x: np.mean(x))
    grp_provider['Std'] = grp_provider['result'].apply(lambda x: np.std(x))

    ax = grp_provider.plot(kind='bar', y='Mean', yerr='Std', capsize=3, figsize=(10, 6))

    ax.set_ylabel('Mean Value')
    ax.set_title('Mean and Standard Deviation of Latency by Provider and Connection Type')
    ax.set_xlabel('Provider and Connection Type')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("plots/provider_mean_std.pdf")
    plt.clf()

    return None


def main():
    df_results = pd.read_csv(
        "/home/jan/Desktop/CMB_git/assignment_3/results/measurement_49924586/measurement_results.csv",
        keep_default_na=False, na_values=[''])

    df_results_preprocessed, num_weird, irregular_probes = data_preprocessing(df_results)
    pipeline_question1(df_results_preprocessed)
    pipeline_question2(df_results_preprocessed)
    pipeline_question3(df_results, df_results_preprocessed, num_weird, irregular_probes)


if __name__ == '__main__':
    main()
