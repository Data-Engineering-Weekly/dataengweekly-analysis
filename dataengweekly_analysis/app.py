import json
import os

import streamlit as st
import pandas as pd


def get_path(filename: str) -> str:
    return os.path.dirname(os.path.abspath(__file__)) + '/data/' + filename + ".json"


def load_data(filename) -> dict:
    json_data = open(get_path(filename), 'r')
    data = json_data.read()
    json_data.close()
    return data


def format_country_stats(data):
    json_dict = json.loads(data)
    country_stats = []
    for key in json_dict:
        country_stats.append({"Country": key, "Readers Percentage": float(json_dict[key])})
    return country_stats


def format_top3_links(data):
    json_dict = json.loads(data)
    top3_links = []
    for links in json_dict:
        top3_links.append({"Edition": links["edition"], "Title": links['title'], "URL": links['short_link']})
    return top3_links


def plot_top3_links():
    data = format_top3_links(load_data('top3_links'))
    st.markdown("## DEW Top 3 Most Viewed Links Per Edition")
    df = pd.DataFrame.from_dict(data)
    st.table(df)


def plot_country_stats():
    data = format_country_stats(load_data('country_metrics'))
    st.markdown("## DEW Viewers Distribution Percentage")
    df = pd.DataFrame.from_dict(data) \
        .sort_values(by=['Readers Percentage'], ascending=False)
    df['Readers Percentage'] = df['Readers Percentage'].map(lambda x: str(round(x, 2)) + '%')
    st.table(df.reset_index(drop=True))


if __name__ == '__main__':
    st.set_page_config(page_title='Data Engineering Weekly NGram Analytics', layout="centered")

    st.title("Data Engineering Weekly Analytics")

    st.sidebar.title("Select Analytics Type")

    geo_analytics_check = st.sidebar.checkbox('Geography Distribution of Readers', value=True, key=1)
    url_analytics_check = st.sidebar.checkbox('Top 3 Links', value=False, key=2)

    st.sidebar.info("Connect with us!!!")

    st.sidebar.markdown("""
    
        **Author:** Data Engineering Weekly

        Twitter: [@data_weekly](https://twitter.com/data_weekly)
        
    """)

    if geo_analytics_check:
        plot_country_stats()

    if url_analytics_check:
        plot_top3_links()
