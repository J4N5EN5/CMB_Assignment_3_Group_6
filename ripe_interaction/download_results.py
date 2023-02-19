import requests
import json
import pandas as pd
import yaml
import os
from tqdm.auto import tqdm
import numpy as np


def read_parameters_from_yml_file(filename):
    """
    Function reads parameters from yml file.
    @params
        filename: name of the file to be read
    @return
        parameters
    """
    with open(f"{filename}") as f:
        return yaml.safe_load(f)


def download_file_to_io_wrapper(url, file):
    """
    Function downloads file from url to io wrapper.
    @params
        url: url to download file from
        file: io wrapper
    """
    with requests.get(url, stream=True) as r:

        # check header to get content length, in bytes
        total_length = int(r.headers.get("Content-Length"))

        # implement progress bar via tqdm
        with tqdm.wrapattr(r.raw, "read", total=total_length, desc="") as raw:
            # save the output to a file
            file.write(raw)


def get_ping_data(api_key):
    """
    Function filters pings out of all measurements and returns only valid ones tagged by 'thousand'.
    @params
        api_key: api_key from ripe atlas with 'List your measurements' permission.
    @return
        pandas dataframe with ping data
    """

    ret = requests.get(f"https://atlas.ripe.net/api/v2/measurements/my/?key={api_key}")
    measurements = ret.json()['results']
    pings_list = []

    for measurement in measurements:
        if "ping" in measurement['tags'] and "thousand" in measurement['tags']:
            result_url = measurement['result']
            start_time = measurement['start_time']
            ping_url = f"{result_url}?start={start_time}"

            ping_response = requests.get(ping_url)
            if ping_response.status_code == 200:
                pings_list.extend(ping_response.json())

    return pd.DataFrame(pings_list)


def dump_dataframe_to_csv(df, path):
    """
    Function dumps pandas dataframe to csv file.
    @params
        df: pandas dataframe
        filename: name of the file to be saved
    """

    df.to_csv("{}/measurement_results.csv".format(path), index=False)


# df_ping = get_ping_data(zhihangs_key)
# dump_dataframe_to_csv(df_ping, "zhihangs_ping")
def read_dataframe_from_csv(filename):
    """
    Function reads pandas dataframe from csv file.
    @params
        filename: name of the file to be read
    @return
        pandas dataframe
    """
    return pd.read_csv(f"{filename}")


# df.drop(['af', 'proto', 'step', 'type', 'fw', 'mver', 'dst_name', 'stored_timestamp', 'size', 'lts',
#         'src_addr', 'from', 'result'], axis=1, inplace=True)


def create_df_from_json_file(filename):
    """
    Function creates pandas dataframe from json file.
    @params
        filename: name of the file to be read
    @return
        pandas dataframe
    """
    with open(f"{filename}", "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)


def get_path_to_store(df_data):
    min_measurement_id = df_data['msm_id'].min()
    max_measurement_id = df_data['msm_id'].max()
    path = 'measurement_{}'.format(min_measurement_id)
    if not os.path.exists(path):
        raise Exception("There exists no corresponding measurement directory. \n \
                        Please check your available data for the measuremetnts in the range {}-{}."
                        .format(min_measurement_id, max_measurement_id))
    else:
        return path


def add_datacenter_information(df_data, path_to_store):
    with open("{}/dc_to_msm_id_mapping.yml".format(path_to_store), "r") as f:
        data = yaml.safe_load(f)
    df_datacenter = pd.read_csv("{}/datacenter_list.csv".format(path_to_store))
    df_datacenter = df_datacenter.replace({"index": data})
    df_datacenter.rename(columns={'index': 'msm_id'}, inplace=True)
    df_data = pd.merge(df_data, df_datacenter, on='msm_id', how='left')
    df_data.drop(np.where(df_data['AU'].isna)[0], axis=0, inplace=True)
    return df_data


def main():
    parameters = read_parameters_from_yml_file("config.yml")
    df_data = get_ping_data(parameters['api_keys']['jan'])
    path_to_store = get_path_to_store(df_data)
    df_data = add_datacenter_information(df_data, path_to_store)
    print(df_data.columns)
    dump_dataframe_to_csv(df_data, path_to_store)


if __name__ == "__main__":
    main()
