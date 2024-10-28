import streamlit as st
import pandas as pd
import json
import plotly.express as px

# Streamlit App Setup
st.title("Astrology Chat Data Processor")

# Step 1: Upload Files
raw_file = st.file_uploader("Upload raw_data.csv", type="csv")
completed_file = st.file_uploader("Upload chat_completed_data.csv", type="csv")
astro_file = st.file_uploader("Upload astro_type.csv", type="csv")

if raw_file and completed_file and astro_file:
    
    # Step 2: Extract JSON Data from raw_data.csv and Save to a DataFrame
    def extract_json(raw_df, json_column):
        json_data = []
        for item in raw_df[json_column]:
            try:
                data = json.loads(item)
                json_data.append(data)
            except (json.JSONDecodeError, TypeError):
                continue
        json_df = pd.json_normalize(json_data)
        combined_df = pd.concat([raw_df, json_df], axis=1)
        return combined_df

    # Step 3: Process Events to Calculate Unique Users
    class UniqueUsersProcessor:
        def __init__(self, raw_df, completed_df, astro_df):
            self.raw_df = raw_df
            self.completed_df = completed_df
            self.astro_df = astro_df

        def process_chat_intake_requests(self):
            intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')]
            intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            intake_events['date'] = intake_events['event_time'].dt.date
            intake_events['hour'] = intake_events['event_time'].dt.hour
            user_counts = intake_events.groupby(['astrologerId', 'date', 'hour'])['user_id'].nunique().reset_index()
            user_counts.rename(columns={'user_id': 'chat_intake_requests', 'astrologerId': '_id'}, inplace=True)
            return user_counts

        def process_chat_accepted_events(self):
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
            valid_user_ids = intake_events['user_id'].unique()
            accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] == 0) & (self.raw_df['clientId'].isin(valid_user_ids))]
            accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            accept_events['date'] = accept_events['event_time'].dt.date
            accept_events['hour'] = accept_events['event_time'].dt.hour
            accept_counts = accept_events.groupby(['user_id', 'date', 'hour'])['clientId'].nunique().reset_index()
            accept_counts.rename(columns={'clientId': 'chat_accepted', 'user_id': '_id'}, inplace=True)
            return accept_counts

        def process_chat_completed_events(self):
            completed_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'].isin(['FREE', 'PAID']))]
            completed_events['createdAt'] = pd.to_datetime(completed_events['createdAt'], utc=True)
            completed_events['date'] = completed_events['createdAt'].dt.date
            completed_events['hour'] = completed_events['createdAt'].dt.hour
            completed_counts = completed_events.groupby(['astrologerId', 'date', 'hour'])['userId'].nunique().reset_index()
            completed_counts.rename(columns={'userId': 'chat_completed', 'astrologerId': '_id'}, inplace=True)
            return completed_counts

        def process_paid_chat_completed_events(self):
            paid_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'] == 'PAID')]
            paid_events['createdAt'] = pd.to_datetime(paid_events['createdAt'], utc=True)
            paid_events['date'] = paid_events['createdAt'].dt.date
            paid_events['hour'] = paid_events['createdAt'].dt.hour
            paid_counts = paid_events.groupby(['astrologerId', 'date', 'hour'])['userId'].nunique().reset_index()
            paid_counts.rename(columns={'userId': 'paid_chats_completed', 'astrologerId': '_id'}, inplace=True)
            return paid_counts

        def merge_with_astro_data(self, final_data):
            merged_data = pd.merge(final_data, self.astro_df, on='_id', how='left')
            columns = ['_id', 'name', 'type', 'date', 'hour', 'chat_intake_requests', 'chat_accepted', 'chat_completed', 'paid_chats_completed']
            return merged_data[columns]

    # Read CSV files
    raw_df = pd.read_csv(raw_file)
    completed_df = pd.read_csv(completed_file)
    astro_df = pd.read_csv(astro_file)

    # Step 4: Process Data
    raw_df = extract_json(raw_df, 'other_data')
    processor = UniqueUsersProcessor(raw_df, completed_df, astro_df)
    
    # Process each event type
    intake_data = processor.process_chat_intake_requests()
    accepted_data = processor.process_chat_accepted_events()
    completed_data = processor.process_chat_completed_events()
    paid_completed_data = processor.process_paid_chat_completed_events()

    # Combine results
    final_results = intake_data
    final_results = pd.merge(final_results, accepted_data, on=['_id', 'date', 'hour'], how='outer')
    final_results = pd.merge(final_results, completed_data, on=['_id', 'date', 'hour'], how='outer')
    final_results = pd.merge(final_results, paid_completed_data, on=['_id', 'date', 'hour'], how='outer')

    # Merge with astro data and display final data
    merged_data = processor.merge_with_astro_data(final_results)
    
    # Display final output
    st.write("### Final Processed Data")
    st.dataframe(merged_data)

    # Option to download final data
    csv = merged_data.to_csv(index=False)
    st.download_button("Download Final Data as CSV", data=csv, file_name="combined_data_final_hour_wise.csv", mime="text/csv")

    # Step 5: Add Visualizations
    st.write("### Visualizations")
    
    # Plot for Chat Intake Requests per Hour
    fig_intake = px.line(merged_data, x='hour', y='chat_intake_requests', title="Chat Intake Requests per Hour",
                         color='name', markers=True)
    st.plotly_chart(fig_intake)

    # Plot for Chat Accepted per Hour
    fig_accepted = px.line(merged_data, x='hour', y='chat_accepted', title="Chat Accepted per Hour",
                           color='name', markers=True)
    st.plotly_chart(fig_accepted)

    # Plot for Chat Completed per Hour
    fig_completed = px.line(merged_data, x='hour', y='chat_completed', title="Chat Completed per Hour",
                            color='name', markers=True)
    st.plotly_chart(fig_completed)

    # Plot for Paid Chats Completed per Hour
    fig_paid_completed = px.line(merged_data, x='hour', y='paid_chats_completed', title="Paid Chats Completed per Hour",
                                 color='name', markers=True)
    st.plotly_chart(fig_paid_completed)

else:
    st.info("Please upload all required CSV files to proceed.")
