import time

import pandas as pd
import psycopg2
import streamlit as st
from confluent_kafka import Consumer
import matplotlib.pyplot as plt
import numpy as np

from voting import consumer


@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # fetch total number of voters
    cur.execute("""
        SELECT count(*) AS voters_count
        FROM voters
    """)
    voters_count = cur.fetchone()[0]
    # fetch total number of candidates
    cur.execute("""
        SELECT count(*) AS candidates_count
        FROM candidates
    """)
    candidates_count = cur.fetchone()[0]
    return voters_count, candidates_count


def create_kafka_consumer(topic_name):
    consumer = Consumer(
        config={
            'bootstrap.servers': 'localhost:9092',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
    )

    consumer.subscribe([topic_name])
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout=1)
    data = []

    for message in messages.value().decode("utf-8"):
        for sub_message in message:
            data.append(sub_message.value)

    return data


def plot_colored_bar_chart(results):
    data_type = results['candidate_name']

    colors = plt.cm.virdidis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel("Candidate")
    plt.ylabel("Total Votes")
    plt.title("Vote Counts Per Candidate")
    plt.xticks(rotation=90)

    return plt


def plot_donut_chart(data):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title('Candidates Votes')

    return fig



def update_data(topic_name):
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # fetch voting statistics from postgres
    voters_count, candidates_count = fetch_voting_stats()

    # display the statistics
    st.markdown("""-----""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col1.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer(topic_name=topic_name)
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    # indentify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # display the leading candidate information
    st.markdown("""-----""")
    st.header("Leading Candidate")

    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'], width=200)
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))

    # display the statistics and visualisations
    st.markdown("""---""")
    st.header("Voting statistics")
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)

    # display the bar chart and donut chart
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)

    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)


def main():
    st.title("Realtime Election Voting Dashboard")
    topic_name = "aggregated_votes_per_candidate"
    update_data(topic_name)


if __name__ == "__main__":
    main()
