import pandas as pd

measurement_path = 'measurement_{}'.format(49924586)


def data_preprocessing(df):
    df.drop(['af', 'proto', 'step', 'type', 'fw', 'mver', 'dst_name', 'stored_timestamp', 'size', 'lts', 'src_addr',
             'from', 'result', 'Latitude', 'Longitude', 'msm_name'], axis=1, inplace=True)
    return df


def pipeline_question1(df):
    df.groupby(['prb_id', 'msm_id']).agg({'avg': 'mean', 'type': 'first'}).reset_index()
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
