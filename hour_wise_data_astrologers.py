import streamlit as st
import pandas as pd
import json

st.title("Unique Users Processing and Data Merging")

# File uploads
raw_data_file = st.file_uploader("Upload Raw Data CSV", type="csv")
astro_data_file = st.file_uploader("Upload Astro Data CSV", type="csv")
json_column = "other_data"  # Specify the JSON column name
output_file = "combined_data_hour_wise.csv"  # Output file name

# Only proceed if both files are uploaded
if raw_data_file and astro_data_file:

    # Step 1: Extract JSON and save all data with extracted JSON columns
    def extract_json_and_save_all(file, json_column):
        df = pd.read_csv(file)
        json_data = []
        for item in df[json_column]:
            try:
                data = json.loads(item)
                json_data.append(data)
            except (json.JSONDecodeError, TypeError):
                continue
        json_df = pd.json_normalize(json_data)
        combined_df = pd.concat([df, json_df], axis=1)
        return combined_df

    combined_df = extract_json_and_save_all(raw_data_file, json_column)
    st.write("Combined data with extracted JSON columns:", combined_df.head())

    # Step 2, 3, 4: Process unique users for each event type (Free, Paid, Chat Intake Submit)
    class UniqueUsersProcessor:
        def __init__(self, raw_data_df):
            self.raw_data_df = raw_data_df

        def process_unique_users(self, event_type, status, event_name_col="event_name", paid_col="paid"):
            raw_data = self.raw_data_df
            if event_type == "chat_call_accept_free":
                filtered_data = raw_data[(raw_data['status'] == status) & (raw_data['type'] == 'FREE')]
                count_col = 'chat_completed'
            elif event_type == "chat_call_accept_paid":
                filtered_data = raw_data[(raw_data['status'] == status) & (raw_data['type'] == 'PAID')]
                count_col = 'paid_chats_completed'
            else:
                filtered_data = raw_data[(raw_data[event_name_col] == event_type) & (raw_data[paid_col] == 0)]
                count_col = 'chat_intake_requests'

            filtered_data['createdAt'] = pd.to_datetime(filtered_data['createdAt'], utc=True)
            filtered_data['date'] = filtered_data['createdAt'].dt.date
            filtered_data['hour'] = filtered_data['createdAt'].dt.hour
            user_client_accepts = filtered_data.groupby(['astrologerId', 'date', 'hour'])['userId'].nunique().reset_index()
            user_client_accepts.rename(columns={'userId': count_col, 'astrologerId': '_id'}, inplace=True)
            return user_client_accepts.sort_values(by=['_id', 'date', 'hour']).reset_index(drop=True)

    raw_data_df = pd.read_csv(raw_data_file)
    processor = UniqueUsersProcessor(raw_data_df)
    completed_free = processor.process_unique_users("chat_call_accept_free", "COMPLETED")
    completed_paid = processor.process_unique_users("chat_call_accept_paid", "COMPLETED")
    chat_intake_submit = processor.process_unique_users("chat_intake_submit", "SUBMITTED", event_name_col="event_name", paid_col="paid")

    # Merge results into one DataFrame
    final_results = pd.merge(completed_free, completed_paid, on=['_id', 'date', 'hour'], how='outer')
    final_results = pd.merge(final_results, chat_intake_submit, on=['_id', 'date', 'hour'], how='outer')
    st.write("Combined Free, Paid, and Chat Intake Data:", final_results.head())

    # Step 5: Merge unique users data with astro_data
    class DataMerger:
        def __init__(self, astro_data_file):
            self.astro_data_file = astro_data_file

        def merge_with_astro_data(self, unique_users_data):
            astro_data = pd.read_csv(self.astro_data_file)
            merged_data = pd.merge(unique_users_data, astro_data, on='_id', how='left')
            merged_data = merged_data[['_id', 'name', 'type', 'date', 'hour', 'chat_intake_requests', 'chat_completed', 'paid_chats_completed']]
            return merged_data

    merger = DataMerger(astro_data_file)
    final_merged_data = merger.merge_with_astro_data(final_results)
    final_merged_data.to_csv("combined_data_final_hour_wise.csv", index=False)
    st.write("Final Merged Data with Astro Data:", final_merged_data.head())
    st.success("Merged data saved to 'combined_data_final_hour_wise.csv'")

else:
    st.warning("Please upload both the Raw Data and Astro Data CSV files.")
