import pandas as pd
import json
import yaml
from geopy.distance import geodesic
import numpy as np
from enum import Enum


def filter_excluded_tags_and_inactive_probes(df_probes, excluded_tags):
    """
    :param df_probes: dataframe containing all probes form the ripe atlas
    :param excluded_tags: list of tags to be generally excluded
    :return: cleaned dataframe for further filtering
    """
    df_connected_probes = df_probes.copy()
    df_connected_probes = df_connected_probes[df_connected_probes['status_name'] == 'Connected']
    # Filter out all probes that contain at least one of the tags in excluded_tags
    df_filtered = df_connected_probes[~df_connected_probes['tags'].apply(lambda x: any(i in x for i in excluded_tags))]
    return df_filtered


def get_starlink_probes(df_probes):
    """
    :param df_probes: dataframe with initially cleaned probes
    :return: dataframe with starlink probes
    """
    # only keep probes that either contain a tag "starlink" or have asn_v4 == 14593
    df_starlink = df_probes[(df_probes['tags'].apply(
        lambda x: any(i in x for i in ['starlink']))) | (df_probes['asn_v4'] == 14593)]
    df_starlink.loc[:, 'connection_type'] = 'starlink'
    return df_starlink


def get_probes_by_tag_filtering(df_probes, required_tags, included_tags, excluded_tags, connection_type):
    """
    Filters probes with three different tag lists and adds a column to indicate the connection type
    :param df_probes: dataframe with initially cleaned probes
    :param required_tags: list of tags that must be included
    :param included_tags: list of tags from which at least one must be included
    :param excluded_tags: list of tags that must not be included
    :param connection_type: descriptive name for the connection type, e.g. wifi, cellular, etc.
    :return: dataframe with probes according to the connection type filters
    """
    # only keep probes that contain all tags in required_tags and at least one tag in included_tags
    df_filtered = df_probes[df_probes['tags'].apply(
        lambda x: all(i in x for i in required_tags) & any(i in x for i in included_tags))]
    # Filter out all probes that contain at least one of the tags in excluded_tags
    df_filtered = df_filtered[~(df_filtered['tags'].apply(lambda x: any(i in x for i in excluded_tags)))]
    df_filtered['connection_type'] = connection_type
    return df_filtered


def get_ethernet_probes_by_proximity(df_probes, df_probes_to_compare):
    """
    Find the closest probe to each probe in df_probes from df_probes_to_compare
    :param df_probes: dataframe with cellular or Wi-Fi probes
    :param df_probes_to_compare: dataframe with ethernet probes
    :return: dataframe containing the closest ethernet probe for each probe in df_probes
    """
    df_probes_by_proximity = pd.DataFrame()
    for probe in df_probes.itertuples(name='Probe'):
        lat, long = probe.latitude, probe.longitude
        # initialize with a very large distance
        dist_min = float("inf")
        probe_min = pd.Series(dtype=object)
        for probe_to_compare in df_probes_to_compare.itertuples(name='ProbeToCompare', index=False):
            lat_to_compare, long_to_compare = probe_to_compare.latitude, probe_to_compare.longitude
            dist = geodesic((lat, long), (lat_to_compare, long_to_compare)).km

            # only consider probes that are closer than the current minimum
            if dist_min > dist > 0:
                dist_min = dist
                probe_min = pd.Series(probe_to_compare, index=df_probes_to_compare.columns)

        # add the closest probe to the dataframe
        df_probes_by_proximity = pd.concat([df_probes_by_proximity, probe_min.to_frame().T], axis=0)
    df_probes_by_proximity['connection_type'] = 'ethernet'

    return df_probes_by_proximity


def convert_country_alpha2_to_continent(country_2_code):
    COUNTRY_ALPHA2_TO_CONTINENT = {
        'AB': 'AS', 'AD': 'EU', 'AE': 'AS', 'AF': 'AS', 'AG': 'NA', 'AI': 'NA', 'AL': 'EU', 'AM': 'AS', 'AO': 'AF',
        'AR': 'SA', 'AS': 'OC', 'AT': 'EU', 'AU': 'OC', 'AW': 'NA', 'AX': 'EU', 'AZ': 'AS', 'BA': 'EU', 'BB': 'NA',
        'BD': 'AS', 'BE': 'EU', 'BF': 'AF', 'BG': 'EU', 'BH': 'AS', 'BI': 'AF', 'BJ': 'AF', 'BL': 'NA', 'BM': 'NA',
        'BN': 'AS', 'BO': 'SA', 'BQ': 'NA', 'BR': 'SA', 'BS': 'NA', 'BT': 'AS', 'BV': 'AN', 'BW': 'AF', 'BY': 'EU',
        'BZ': 'NA', 'CA': 'NA', 'CC': 'AS', 'CD': 'AF', 'CF': 'AF', 'CG': 'AF', 'CH': 'EU', 'CI': 'AF', 'CK': 'OC',
        'CL': 'SA', 'CM': 'AF', 'CN': 'AS', 'CO': 'SA', 'CR': 'NA', 'CU': 'NA', 'CV': 'AF', 'CW': 'NA', 'CX': 'AS',
        'CY': 'AS', 'CZ': 'EU', 'DE': 'EU', 'DJ': 'AF', 'DK': 'EU', 'DM': 'NA', 'DO': 'NA', 'DZ': 'AF', 'EC': 'SA',
        'EE': 'EU', 'EG': 'AF', 'ER': 'AF', 'ES': 'EU', 'ET': 'AF', 'FI': 'EU', 'FJ': 'OC', 'FK': 'SA', 'FM': 'OC',
        'FO': 'EU', 'FR': 'EU', 'GA': 'AF', 'GB': 'EU', 'GD': 'NA', 'GE': 'AS', 'GF': 'SA', 'GG': 'EU', 'GH': 'AF',
        'GI': 'EU', 'GL': 'NA', 'GM': 'AF', 'GN': 'AF', 'GP': 'NA', 'GQ': 'AF', 'GR': 'EU', 'GS': 'SA', 'GT': 'NA',
        'GU': 'OC', 'GW': 'AF', 'GY': 'SA', 'HK': 'AS', 'HM': 'AN', 'HN': 'NA', 'HR': 'EU', 'HT': 'NA', 'HU': 'EU',
        'ID': 'AS', 'IE': 'EU', 'IL': 'AS', 'IM': 'EU', 'IN': 'AS', 'IO': 'AS', 'IQ': 'AS', 'IR': 'AS', 'IS': 'EU',
        'IT': 'EU', 'JE': 'EU', 'JM': 'NA', 'JO': 'AS', 'JP': 'AS', 'KE': 'AF', 'KG': 'AS', 'KH': 'AS', 'KI': 'OC',
        'KM': 'AF', 'KN': 'NA', 'KP': 'AS', 'KR': 'AS', 'KW': 'AS', 'KY': 'NA', 'KZ': 'AS', 'LA': 'AS', 'LB': 'AS',
        'LC': 'NA', 'LI': 'EU', 'LK': 'AS', 'LR': 'AF', 'LS': 'AF', 'LT': 'EU', 'LU': 'EU', 'LV': 'EU', 'LY': 'AF',
        'MA': 'AF', 'MC': 'EU', 'MD': 'EU', 'ME': 'EU', 'MF': 'NA', 'MG': 'AF', 'MH': 'OC', 'MK': 'EU', 'ML': 'AF',
        'MM': 'AS', 'MN': 'AS', 'MO': 'AS', 'MP': 'OC', 'MQ': 'NA', 'MR': 'AF', 'MS': 'NA', 'MT': 'EU', 'MU': 'AF',
        'MV': 'AS', 'MW': 'AF', 'MX': 'NA', 'MY': 'AS', 'MZ': 'AF', 'NA': 'AF', 'NC': 'OC', 'NE': 'AF', 'NF': 'OC',
        'NG': 'AF', 'NI': 'NA', 'NL': 'EU', 'NO': 'EU', 'NP': 'AS', 'NR': 'OC', 'NU': 'OC', 'NZ': 'OC', 'OM': 'AS',
        'OS': 'AS', 'PA': 'NA', 'PE': 'SA', 'PF': 'OC', 'PG': 'OC', 'PH': 'AS', 'PK': 'AS', 'PL': 'EU', 'PM': 'NA',
        'PR': 'NA', 'PS': 'AS', 'PT': 'EU', 'PW': 'OC', 'PY': 'SA', 'QA': 'AS', 'RE': 'AF', 'RO': 'EU', 'RS': 'EU',
        'RU': 'EU', 'RW': 'AF', 'SA': 'AS', 'SB': 'OC', 'SC': 'AF', 'SD': 'AF', 'SE': 'EU', 'SG': 'AS', 'SH': 'AF',
        'SI': 'EU', 'SJ': 'EU', 'SK': 'EU', 'SL': 'AF', 'SM': 'EU', 'SN': 'AF', 'SO': 'AF', 'SR': 'SA', 'SS': 'AF',
        'ST': 'AF', 'SV': 'NA', 'SY': 'AS', 'SZ': 'AF', 'TC': 'NA', 'TD': 'AF', 'TG': 'AF', 'TH': 'AS', 'TJ': 'AS',
        'TK': 'OC', 'TM': 'AS', 'TN': 'AF', 'TO': 'OC', 'TP': 'AS', 'TR': 'AS', 'TT': 'NA', 'TV': 'OC', 'TW': 'AS',
        'TZ': 'AF', 'UA': 'EU', 'UG': 'AF', 'US': 'NA', 'UY': 'SA', 'UZ': 'AS', 'VC': 'NA', 'VE': 'SA', 'VG': 'NA',
        'VI': 'NA', 'VN': 'AS', 'VU': 'OC', 'WF': 'OC', 'WS': 'OC', 'XK': 'EU', 'YE': 'AS', 'YT': 'AF', 'ZA': 'AF',
        'ZM': 'AF', 'ZW': 'AF',
    }
    if country_2_code not in COUNTRY_ALPHA2_TO_CONTINENT:
        raise KeyError

    return COUNTRY_ALPHA2_TO_CONTINENT[country_2_code]


def make_mapping(df_probe, dc_city, probe_country_here, probe_continent_here, scenario):
    """
    This function creates a mapping between probes and datacenters.
    :param df_probe: The single probe we want to map to datacenters
    :param dc_city: The datacenters we want to map to the probe
    :param scenario: Defines the location of the datacenters
    :return: A dataframe with the probe and the datacenter mapping
    """
    # Create a dataframe with the probe repeated for each datacenter
    df_probe_city = pd.concat([df_probe] * len(dc_city), ignore_index=True)
    probe_dc_city = pd.concat([df_probe_city, dc_city.reset_index(drop=True)], axis=1)

    probe_dc_city['probe_country'] = probe_country_here
    probe_dc_city['probe_continent'] = probe_continent_here

    probe_dc_city['Scenario'] = scenario
    return probe_dc_city


def get_closest_datacenter_to_probe(probe, dc_list):
    """
    This function returns the closest datacenter to a probe
    :param probe: Probe in question
    :param dc_list: Datacenters to compare to
    :return: closest datacenter
    """
    probe_lat, probe_long = probe.latitude, probe.longitude

    dc_closest = pd.DataFrame()
    # Set the initial distance to infinity
    min_dist = float("inf")

    for dc in dc_list:
        dc_lat, dc_long = dc['Latitude'].mean(), dc['Longitude'].mean()
        dist = geodesic((probe_lat, probe_long), (dc_lat, dc_long)).km

        # If the distance is smaller than the current minimum, update the minimum
        if dist < min_dist:
            min_dist = dist
            dc_closest = dc

    return dc_closest


def create_mapping(df_probes, df_datacenters):
    """
    This function is used to define which probes are mapped to which datacenters.
    :param df_probes: all probes we want to map to datacenters and thus use in our measurements
    :param df_datacenters: the datacenters we chose to use in our measurements
    :return: Dataframe containing the final list of mappings we want to use for measurements
    """
    probe_datacenter_mapping = pd.DataFrame()

    # Create dataframes containing the datacenters for each location we chose
    dc_frankfurt = df_datacenters[df_datacenters["City"] == 'Frankfurt am Main']
    dc_london = df_datacenters[df_datacenters["City"] == 'London']
    dc_useast = df_datacenters[(df_datacenters["City"] == 'Washington') | (df_datacenters["City"] == 'Ashburn')]
    dc_uswest = df_datacenters[(df_datacenters["City"] == 'San Jose') | (df_datacenters["City"] == 'Los Angeles')]

    dc_hongkong = df_datacenters[df_datacenters["Country"] == 'HK']
    dc_canada = df_datacenters[df_datacenters["Country"] == 'CA']
    dc_mumbai = df_datacenters[df_datacenters["Country"] == 'IN']
    dc_singapore = df_datacenters[df_datacenters["Country"] == "SG"]

    dc_southamerica = df_datacenters[df_datacenters["Continent"] == 'SA']
    dc_oceanien = df_datacenters[df_datacenters["Continent"] == "OC"]
    dc_asia = df_datacenters[df_datacenters["Continent"] == "AS"]

    # Iterate over all probes and decide which datacenters to map to them. To each probe we map 4 datacenters with
    # different locations.
    for probe in df_probes.itertuples(name='Probe', index=False):
        df_probe = pd.Series(probe, index=df_probes.columns).to_frame().T

        if probe.continent_code == "EU":
            probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'SameContinent')
            probe_dc_lon = make_mapping(df_probe, dc_london, probe.country_code, probe.continent_code, 'SameContinent')
            probe_dc_use = make_mapping(df_probe, dc_useast, probe.country_code, probe.continent_code, 'NeighborContinent')
            probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'OtherContinent')

            probe_datacenter_mapping = pd.concat(
                [probe_datacenter_mapping, probe_dc_fra, probe_dc_lon, probe_dc_use, probe_dc_hon], ignore_index=True)

        if probe.continent_code == "AS":
            if probe.country_code in ["HK", "SG"]:
                probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'SameContinent')
                probe_dc_sg = make_mapping(df_probe, dc_singapore, probe.country_code, probe.continent_code, 'SameContinent')
                probe_datacenter_mapping = pd.concat([probe_datacenter_mapping, probe_dc_hon, probe_dc_sg],
                                                     ignore_index=True)
            elif probe.country_code in dc_asia["Country"].unique():
                probe_dc_closest = make_mapping(df_probe, dc_asia[dc_asia["Country"] == probe.country_code], probe.country_code, probe.continent_code,
                                                'SameCountry')
                probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'NeighborCountry')
                probe_datacenter_mapping = pd.concat([probe_datacenter_mapping, probe_dc_closest, probe_dc_hon],
                                                     ignore_index=True)
            else:
                probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'SameContinent')
                probe_dc_sg = make_mapping(df_probe, dc_singapore, probe.country_code, probe.continent_code, 'SameContinent')
                probe_datacenter_mapping = pd.concat([probe_datacenter_mapping, probe_dc_hon, probe_dc_sg],
                                                     ignore_index=True)

            probe_dc_use = make_mapping(df_probe, dc_useast, probe.country_code, probe.continent_code, 'NeighborContinent')
            probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'OtherContinent')

            probe_datacenter_mapping = pd.concat([probe_datacenter_mapping, probe_dc_use, probe_dc_fra],
                                                 ignore_index=True)

        if probe.continent_code == "NA":
            if probe.country_code == "US":
                probe_dc_us = make_mapping(df_probe, get_closest_datacenter_to_probe(probe, [dc_useast, dc_uswest]), probe.country_code, probe.continent_code,
                                           'SameCountry')
                probe_dc_ca = make_mapping(df_probe, dc_canada, probe.country_code, probe.continent_code, 'NeighborCountry')
                probe_dc_sa = make_mapping(df_probe, dc_southamerica, probe.country_code, probe.continent_code, 'NeighborContinent')
                probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'NeighborContinent')

                probe_datacenter_mapping = pd.concat(
                    [probe_datacenter_mapping, probe_dc_us, probe_dc_ca, probe_dc_sa, probe_dc_fra], ignore_index=True)

            elif probe.country_code == "CA":
                probe_dc_ca = make_mapping(df_probe, dc_canada, probe.country_code, probe.continent_code, 'SameCountry')
                probe_dc_us = make_mapping(df_probe, get_closest_datacenter_to_probe(probe, [dc_useast, dc_uswest]), probe.country_code, probe.continent_code,
                                           'NeighborCountry')
                probe_dc_sa = make_mapping(df_probe, dc_southamerica, probe.country_code, probe.continent_code, 'NeighborContinent')
                probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'NeighborContinent')

                probe_datacenter_mapping = pd.concat(
                    [probe_datacenter_mapping, probe_dc_ca, probe_dc_us, probe_dc_sa, probe_dc_fra], ignore_index=True)

            else:
                probe_dc_use = make_mapping(df_probe, dc_useast, probe.country_code, probe.continent_code, 'SameCountry')
                probe_dc_usw = make_mapping(df_probe, dc_uswest, probe.country_code, probe.continent_code, 'NeighborCountry')
                probe_dc_sa = make_mapping(df_probe, dc_southamerica, probe.country_code, probe.continent_code, 'NeighborContinent')
                probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'NeighborContinent')

                probe_datacenter_mapping = pd.concat(
                    [probe_datacenter_mapping, probe_dc_use, probe_dc_usw, probe_dc_sa, probe_dc_fra],
                    ignore_index=True)

        if probe.continent_code == "SA":
            probe_dc_sa = make_mapping(df_probe, dc_southamerica, probe.country_code, probe.continent_code, 'SameContinent')
            probe_dc_use = make_mapping(df_probe, dc_useast, probe.country_code, probe.continent_code, 'NeighborContinent')
            probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'OtherContinent')
            probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'OtherContinent')

            probe_datacenter_mapping = pd.concat(
                [probe_datacenter_mapping, probe_dc_sa, probe_dc_use, probe_dc_fra, probe_dc_hon], ignore_index=True)

        if probe.continent_code == "OC":
            probe_dc_oce = make_mapping(df_probe, dc_oceanien, probe.country_code, probe.continent_code, 'SameContinent')
            probe_dc_use = make_mapping(df_probe, dc_useast, probe.country_code, probe.continent_code, 'NeighborContinent')
            probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'OtherContinent')
            probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'NeighborContinent')

            probe_datacenter_mapping = pd.concat(
                [probe_datacenter_mapping, probe_dc_oce, probe_dc_use, probe_dc_fra, probe_dc_hon], ignore_index=True)

        if probe.continent_code == "AF":
            probe_dc_fra = make_mapping(df_probe, dc_frankfurt, probe.country_code, probe.continent_code, 'NeighborContinent')
            probe_dc_mum = make_mapping(df_probe, dc_mumbai, probe.country_code, probe.continent_code, 'OtherContinent')
            probe_dc_use = make_mapping(df_probe, dc_useast, probe.country_code, probe.continent_code, 'OtherContinent')
            probe_dc_hon = make_mapping(df_probe, dc_hongkong, probe.country_code, probe.continent_code, 'OtherContinent')

            probe_datacenter_mapping = pd.concat(
                [probe_datacenter_mapping, probe_dc_fra, probe_dc_mum, probe_dc_use, probe_dc_hon], ignore_index=True)

    return probe_datacenter_mapping


#data_center_tag = pd.read_csv("Datacenters_for_tags_in_measurement.csv", keep_default_na=False)
# print(data_center_tag)

post_body = []
post_name = []
debug_information = []


def create_ping_json(datacenter_domain_name, probe_set):
    # I want to create several tags in the measurement
    # including data_center_name I created, Continent, provider name
    row = data_center_tag[data_center_tag["AU"] == datacenter_domain_name]
    dc_name = row["Datacenter_name"].iloc[0]
    dc_continet = row["Continent"].iloc[0]
    dc_provider = row["Provider"].iloc[0]
    if dc_provider == "Amazon EC2":
        dc_provider = "Amazon"

    dc_name = dc_name.replace('_', '')
    dc_name = dc_name.lower()
    dc_continet = dc_continet.lower()
    dc_provider = dc_provider.lower()

    # print(dc_name+dc_continet+dc_provider)

    probe_all_count = len(probe_set)

    ###### PARAMETER HERE #############
    # set here how many measurement can there be in one measurement
    limit_measurement = 1000
    ###### PARAMETER END #############
    ####actually doesn't really matter#####

    batch = -(probe_all_count // -limit_measurement)  # ceiling, a simple approach not using math

    for i in range(batch):
        start = i * limit_measurement
        end = min((i + 1) * limit_measurement - 1, probe_all_count - 1)
        probe_set_thisbatch = probe_set[start:end + 1]

        probe_name_thisbatch = ','.join([str(i) for i in probe_set_thisbatch])
        probe_count_thisbatch = len(probe_set_thisbatch)

        description_batch = "Ping-" + dc_name + "-" + dc_provider + "-" + str(i + 1) + "of" + str(batch)

        data = {
            "definitions": [
                {
                    "target": datacenter_domain_name,
                    "af": 4,
                    "packets": 3,
                    "size": 48,
                    "description": description_batch,
                    "interval": 14400,
                    "tags": ["ping", "thousand", dc_name, dc_continet, dc_provider],
                    "resolve_on_probe": False,
                    "skip_dns_check": False,
                    "include_probe_id": False,
                    "type": "ping"
                }
            ],
            "probes": [
                {
                    "value": probe_name_thisbatch,
                    "type": "probes",
                    "requested": probe_count_thisbatch
                }
            ],
            "is_oneoff": False,
            "bill_to": "janoesterle@posteo.de",
            "start_time": 1676178720,
            "stop_time": 1676783520
        }

        debug_info = "datacenter:" + dc_name + '' + dc_continet + ',' + dc_provider + ';'
        debug_info = debug_info + 'probe_count:' + str(probe_count_thisbatch) + ';'
        debug_info = debug_info + datacenter_domain_name + ';'
        debug_information.append(debug_info)

        # timestamp
        # Unix Timestamp	1675883520 GMT	Wed Feb 08 2023 19:12:00 GMT+0000
        # Unix Timestamp	1676491920 GMT	Wed Feb 15 2023 20:12:00 GMT+0000
        # print(["ping", dc_name, dc_continet, dc_provider])
        # print(type(["ping", dc_name, dc_continet, dc_provider]))
        # print(type(dc_name))

        post_body.append(data)
        post_name.append(description_batch)


def main():
    # Load the config file containing the parameters to create our measurements
    parameters = yaml.load(open('config.yml', 'r'), Loader=yaml.FullLoader)
    # Load the list of all probes dowlnoaded from RIPE Atlas
    with open(parameters['probe_json']) as file:
        data = json.loads(file.read())
    df_all_probes = pd.DataFrame(data['objects'])

    df_base_probes = filter_excluded_tags_and_inactive_probes(df_all_probes, parameters['general_excluded_tags'])
    del df_all_probes

    df_starlink_probes = get_starlink_probes(df_base_probes)
    df_wifi_probes = get_probes_by_tag_filtering(df_base_probes, parameters['wifi_required_tags'],
                                                 parameters['wifi_included_tags'], parameters['wifi_excluded_tags'],
                                                 'wifi')
    df_cellular_probes = get_probes_by_tag_filtering(df_base_probes, parameters['cellular_required_tags'],
                                                     parameters['cellular_included_tags'],
                                                     parameters['cellular_excluded_tags'], 'cellular')
    df_ethernet_probes = get_probes_by_tag_filtering(df_base_probes, parameters['ethernet_required_tags'],
                                                     parameters['ethernet_included_tags'],
                                                     parameters['ethernet_excluded_tags'], 'ethernet')

    df_wifi_proximate_ethernet_probes = get_ethernet_probes_by_proximity(df_wifi_probes, df_ethernet_probes)
    df_cellular_proximate_ethernet_probes = get_ethernet_probes_by_proximity(df_cellular_probes, df_ethernet_probes)
    df_ethernet_probes = pd.concat([df_wifi_proximate_ethernet_probes, df_cellular_proximate_ethernet_probes], axis=0)

    df_all_probes = pd.concat([df_cellular_probes, df_wifi_probes, df_starlink_probes, df_ethernet_probes])
    df_all_probes['continent_code'] = df_all_probes['country_code'].apply(convert_country_alpha2_to_continent)

    df_datacenters = pd.read_csv('datacenter_list.csv', na_values=[''], keep_default_na=False)
    mapping = create_mapping(df_all_probes, df_datacenters)

    mapping.to_csv("mapping.csv")

    if parameters['start_measurements']:
        for j in range(len(mapping)):
            create_ping_json(mapping.iloc[j]['AU'], mapping.iloc[j]['id'])


if __name__ == '__main__':
    main()
