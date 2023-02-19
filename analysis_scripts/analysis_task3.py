import pandas as pd

measurement_path = 'results/measurement_{}'.format(49832986)


def data_preprocessing(df):
    df.drop([], axis=1, inplace=True)
    return df


def pipeline_question1(df):
    return None


def pipeline_question2(df):
    return None


def define_neighbor_country(df):
    for i in range(len(df)):
        if df.iloc[i]['Scenario'] == 'SameContinent':
            if df.iloc[i]['probe_country'] == df.iloc[i]['Country_y']:
                df.at[i, 'Scenario'] = 'SameCountry'
            elif df.iloc[i]['Country_y'] == 'DE':
                if df.iloc[i]['probe_country'] in ['FR','CH','BL', 'NL']


    return None

def pipeline_question3(df):
    print(df.shape)
    print(df.head(5))
    print(df.columns)
    return None


def main():
    df_results = pd.read_csv(f"{measurement_path}/measurement_results.csv")
    df_results_preprocessed = data_preprocessing(df_results)
    pipeline_question1(df_results_preprocessed)
    pipeline_question2(df_results_preprocessed)
    pipeline_question3(df_results_preprocessed)


if __name__ == '__main__':
    main()
