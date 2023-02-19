import requests
import json
import pandas as pd
import yaml
import os
from tqdm.auto import tqdm
import numpy as np


def get_ping_data(api_key):
    """
    Function downloads ping data from ripe atlas using api_key.
    :param api_key:
    :return:
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


def get_path_to_store(df_data):
    # Either create a new folder or use the existing one with the  name corresponding to the lowest measurement id
    min_measurement_id = df_data['msm_id'].min()
    path = 'results/measurement_{}'.format(min_measurement_id)
    if not os.path.exists(path):
        os.mkdir(path)
    return path


def add_datacenter_information(df_data, path_to_store):
    """
    Function adds datacenter information to the measurement results.
    :param df_data: Measurement results
    :param path_to_store: Path to store the results corresponding to the lowest measurement id
    :return: Dataframe with measurement results and datacenter information
    """

    with open("{}/dc_to_msm_id_mapping.yml".format(path_to_store), "r") as f:
        data = yaml.safe_load(f)
    df_datacenter = pd.read_csv("{}/datacenter_list.csv".format(path_to_store))
    df_mapping = pd.read_csv("ripe_interaction/mapping.csv", usecols=['AU', 'id', 'Scenario', 'probe_country', 'probe_continent',
                                                                      'Continent', 'Country', 'City', 'connection_type'])



    df_mapping.rename(columns={'id': 'prb_id'}, inplace=True)
    # We replace the temporary indices with the msm_ids
    df_datacenter = df_datacenter.replace({"index": data})
    df_datacenter.rename(columns={'index': 'msm_id'}, inplace=True)
    df_data = pd.merge(df_data, df_datacenter, on='msm_id', how='left')
    # Mappings can be identified by common probe ids and AU (datacenter identifier)
    # We map left to right, so we can drop all rows where e.g. probes from the
    # mapping did not participate in the measurement
    df_data = pd.merge(df_data, df_mapping, on=['prb_id', 'AU'], how='left')

    df_data.drop(np.where(df_data['AU'].isna())[0], axis=0, inplace=True)
    df_data.reset_index(inplace=True)
    return df_data


def main():
    with open('ripe_interaction/config.yml') as f:
        parameters = yaml.safe_load(f)

    df_data = get_ping_data(parameters['api_keys']['zhihang'])
    path_to_store = get_path_to_store(df_data)
    df_data = add_datacenter_information(df_data, path_to_store)
    df_data.drop(parameters['columns_to_drop'], axis=1, inplace=True)
    df_data.to_csv(f"{path_to_store}/measurement_results.csv", index=False)


if __name__ == "__main__":
    main()


