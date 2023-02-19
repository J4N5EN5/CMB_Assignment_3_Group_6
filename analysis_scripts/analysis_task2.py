import pandas as pd
from ast import literal_eval
import numpy as np


measurement_path = 'measurement_{}'.format(49924586)


def data_preprocessing(df):
    df.drop(['af', 'proto', 'step', 'type', 'fw', 'mver', 'dst_name', 'stored_timestamp', 'size', 'lts',
             'src_addr', 'from', 'min', 'max', 'avg', 'dst_addr', 'msm_id', 'index', 'msm_name', 'Continent_x',
             'Country_x', 'City_x'], axis=1, inplace=True)
    df['result'] = df['result'].apply(lambda x: [d.values() for d in literal_eval(x)])
    df['result'] = [sum([list(x) for x in y], []) for y in df['result']]
    number_of_irregular_probes = len(df[df['sent'] != 3])
    df = df[df['sent'] == 3]
    return df, number_of_irregular_probes


def pipeline_question1(df):
    df.groupby(['prb_id', 'AU', 'Scenario', 'connection_type']).agg(Mean=('result', np.mean), Std=('result', np.std)).reset_index()
    return None


def pipeline_question2(df):
    return None


def pipeline_question3(df):
    return None


def main():
    df_results = pd.read_csv(f"{measurement_path}/measurement_results.csv")
    df_results_preprocessed = data_preprocessing(df_results)
    pipeline_question1(df_results_preprocessed)
    pipeline_question2(df_results_preprocessed)
    pipeline_question3(df_results_preprocessed)


if __name__ == '__main__':
    main()

# Apply ast.literal_eval and list comprehension to extract 'rtt' values
df['new_column'] = df['result'].apply(lambda x: [d['rtt'] for d in ast.literal_eval(x)])
