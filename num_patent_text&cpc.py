'''
input:
selected words for search in the abstract + title
cpc subclass, if you want to keep certain cpc then filter the patents based on selected words.
if not, you can leave "cpc_codes_input" empty.

sample result:
Number of patents in this topic
and
csv file with these columns:
'patent_id', 'patent_date', 'patent_abstract', 'patent_title', 'cpc_subclass', 'cpc_sequence'
'''


import pandas as pd
import time

# go to this address to download the dataset
# https://patentsview.org/download/data-download-tables
g_patent = 'g_patent.tsv'
g_cpc = 'g_cpc_current.tsv'

# keep specific columns in g_patent database
print('start reading g_patent file and choosing selected columns')
g_patent_df = pd.read_csv(g_patent, sep='\t', usecols=['patent_id', 'patent_date', 'patent_abstract', 'patent_title'], dtype=str)
g_patent_df['combined_text'] = g_patent_df['patent_title'].str.lower() + ' ' + g_patent_df['patent_abstract'].str.lower().fillna('')


# Function to filter g_patent database based on selected_word dictionary
def filter_g_patent(selected_word,certain_database):
    filtered_patents = certain_database[certain_database['combined_text'].str.contains('|'.join(selected_word), case=False)]

    return filtered_patents

# Function to filter g_cpc based on CPC codes
def filter_g_cpc_by_sequence(file_path, cpc_codes=None, sequence='0'):
    # Read the 'g_cpc' TSV file into a DataFrame and select only the required columns
    columns_to_keep = ['patent_id', 'cpc_subclass', 'cpc_sequence']
    filtered_g_cpc = pd.read_csv(file_path, sep='\t', usecols=columns_to_keep, dtype=str)

    # Filter the DataFrame to keep only rows where cpc_sequence is equal to the given sequence
    filtered_g_cpc = filtered_g_cpc[filtered_g_cpc['cpc_sequence'] == str(sequence)]

    if cpc_codes:
        # Filter the DataFrame to keep only rows with CPC codes in the provided list
        filtered_g_cpc = filtered_g_cpc[filtered_g_cpc['cpc_subclass'].isin(cpc_codes)]

    return filtered_g_cpc

# Function to perform left join based on a common key (patent_id)
def left_join(left_df, right_df, key):
    return pd.merge(left_df, right_df, on=key, how='left')

# Main function
def main():
    selected_word = {'dental implant', 'Dental implant fixture', 'Dental implant fix', 'Dental implant screw',
                     'dental implant abutment', 'dental implant connect', 'dental implant connector',
                     'Dental implant artificial teeth', 'Dental implant artificial tooth', 'Dental implant artificial cap'}
    cpc_codes = ['A61C']

    # Step 2: Filter g_cpc
    print('Step 2: Filtering g_cpc based on the provided CPC codes...')
    start_time = time.time()
    filtered_g_cpc = filter_g_cpc_by_sequence(g_cpc, cpc_codes, sequence='0')
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()

    # Step 3: Left join g_patent with filtered_g_cpc
    print('Step 3: Performing left join between g_patent and filtered_g_cpc...')
    start_time = time.time()
    joined_g_cpc = left_join(g_patent_df, filtered_g_cpc, 'patent_id')
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()

    # Step 4: Search the selected keywords in joined_g_cpc (previously filtered by CPC)
    print('Step 4: Searching selected keywords in the filtered data...')
    start_time = time.time()
    filtered_patents = filter_g_patent(selected_word,joined_g_cpc)
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()

    filtered_patents = filtered_patents.dropna(subset=['cpc_subclass'])


    # Step 5: Store the final_data in a CSV
    output_file = 'new_topic_all.csv'
    filtered_patents.to_csv(output_file, index=False)
    print(f"new_topic has been stored in '{output_file}'.")
    print()


    # Step 6: Print the number of rows in the new_topic file
    num_rows = filtered_patents.shape[0]
    print("Number of patents in this topic:", num_rows)

if __name__ == "__main__":
    main()
