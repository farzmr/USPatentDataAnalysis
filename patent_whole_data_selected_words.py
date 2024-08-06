'''
input:
selected words for search in the abstract + title
cpc_sequence=0

sample result:
patent_id	patent_type	patent_date	       patent_title	                 patent_abstract	       cpc_subclass	 cpc_class   assignee_sequence_x	        assignee_type	                                             assignee_name	                                            disambig_state	disambig_country	assignee_sequence_y
10504314	utility	    2019-12-10	    Blockchain-based...         A system may facilitate... 	    G07C	        G07                 1.0, 0.0	        Foreign Company or Corporation, Foreign Company or Corporation	    DSX Holdings Limited, ACCENTURE GLOBAL SOLUTIONS LIMITED	nan, nan	        JE, IE              	1
10560272	utility	    2020-02-11  	Bio-information...      	Disclosed is a method of... 	H04L	         H04                  0.0, 1.0      	Foreign Company or Corporation, Foreign Company or Corporation  	MACROGEN, INC., MACROGEN, INC.                          	nan, MD         	KR, US              	1

this code is for running on server, rows are not limited and selected words are comprehensive
'''


import pandas as pd
import time

# go to this address to download the dataset
# https://patentsview.org/download/data-download-tables
g_patent = 'g_patent_n.tsv'
g_assignee = 'g_assignee_disambiguated_n.tsv'
g_location = 'g_location_disambiguated_n.tsv'
g_cpc = 'g_cpc_current_n.tsv'

# Function to filter g_patent database based on selected_word dictionary
def filter_g_patent(selected_word):
    g_patent_df = pd.read_csv(g_patent, sep='\t', usecols=['patent_id', 'patent_date', 'patent_type', 'patent_abstract', 'patent_title'], dtype=str)
    g_patent_df['combined_text'] = g_patent_df['patent_title'].str.lower() + ' ' + g_patent_df['patent_abstract'].str.lower().fillna('')
    filtered_patents = g_patent_df[g_patent_df['combined_text'].str.contains('|'.join(selected_word), case=False)]
    return filtered_patents


# Function to perform left join based on common key (patent_id)
def left_join(left_df, right_df, key):
    return pd.merge(left_df, right_df, on=key, how='left')

def filter_g_cpc_by_sequence(file_path, sequence='0'):
    # Read the 'g_cpc' TSV file into a DataFrame and select only the required columns
    columns_to_keep = ['patent_id', 'cpc_subclass', 'cpc_sequence','cpc_class']
    filtered_g_cpc = pd.read_csv(file_path, sep='\t', usecols=columns_to_keep, dtype=str)

    # Filter the DataFrame to keep only rows where cpc_sequence is equal to the given sequence
    filtered_g_cpc = filtered_g_cpc[filtered_g_cpc['cpc_sequence'] == str(sequence)]

    return filtered_g_cpc

# Step 3-1: Concatenate assignee_sequence for each patent_id
def concat_sequence(group):
    # Convert 'assignee_sequence' to string type and fill NaN with '0'
    group['assignee_sequence'] = group['assignee_sequence'].astype(str).fillna('0')
    #sorted_sequences = sorted(group['assignee_sequence'])
    return ', '.join(group['assignee_sequence'])

# Step 3-3: If assignee_sequence is greater than 0, aggregate the columns for each patent_id
def aggregate_assignees_reg(group):
    max_sequence = group['assignee_sequence'].max()
    if max_sequence != 0:
        group['assignee_sequence'] = concat_sequence(group)
        assignee_types = '& '.join(group['assignee_type_reg'].astype(str))
        assignee_names = '& '.join(group['assignee_name'].astype(str))
        disambig_states = '& '.join(group['disambig_state'].astype(str))
        disambig_countries = '& '.join(group['disambig_country'].astype(str))
        group.iloc[0, group.columns.get_loc('assignee_type_reg')] = assignee_types
        group.iloc[0, group.columns.get_loc('assignee_name')] = assignee_names
        group.iloc[0, group.columns.get_loc('disambig_state')] = disambig_states
        group.iloc[0, group.columns.get_loc('disambig_country')] = disambig_countries
        return group.iloc[[0]]
    else:
        return group


#Define a function to convert assignee_sequence to int and handle NaN values
def convert_assignee_sequence(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0  # Or any other default value you prefer

# Main function
def main():


    selected_word = {'blockchain', 'bitcoin','bit-coin', 'block-chain', 'blocksign','codius','colored coin',
                 'colored-coin', 'crypto currency', 'crypto-currency', 'cryptocurrency', 'distributed ledger',
                 'distributed-ledger', 'dogecoin', 'doge-coin', 'ethereum','factom','litecoin','lite-coin',
                 'pay-to-script-hash', 'p2sh', 'proof of stake', 'proof-of-stake', 'sidechain','smart contract',
                 'smart-contract', 'factom','zcash','zerocash'}


    # Define a mapping dictionary for assignee_type values
    assignee_type_mapping_reg = {
        1: 'Unassigned',
        2: 'US Company or Corporation',
        3: 'Foreign Company or Corporation',
        4: 'US Individual',
        5: 'Foreign Individual',
        6: 'US Federal Government',
        7: 'Foreign Government',
        8: 'US County Government',
        9: 'US State Government',
    }

    assignee_type_mapping_unified = {
        'Unassigned': 'Unassigned',
        'US Company or Corporation': 'Company',
        'Foreign Company or Corporation': 'Company',
        'US Individual': 'Individual',
        'Foreign Individual': 'Individual',
        'US Federal Government': 'Government',
        'Foreign Government': 'Government',
        'US County Government': 'Government',
        'US State Government': 'Government',
    }
    # Step 1: Filter g_patent
    print('Step 1: Filtering g_patent based on selected words...')
    start_time = time.time()
    filtered_patents = filter_g_patent(selected_word)
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print('First 3 rows of filtered_patents:')
    print(filtered_patents.head(3))
    print()

    # Step 2-0: keep first cpc code in g_cpc
    print('Step 2-0: keep data with sequence= 0 in cpc file')
    filtered_g_cpc= filter_g_cpc_by_sequence(g_cpc, sequence='0')

    # Step 2: Left join g_patent with g_cpc
    print('Step 2: Performing left join between g_patent and g_cpc...')
    start_time = time.time()
    joined_g_cpc = left_join(filtered_patents, filtered_g_cpc, 'patent_id')
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()
    print('First 3 rows of cpc and patent joined:')
    print(joined_g_cpc.head(3))
    print()

    # Step 3: Left join joined_g_cpc with g_assignee
    print('Step 3: Performing left join between joined_g_cpc and g_assignee...')
    start_time = time.time()

    ##g_assignee_df = pd.read_csv(g_assignee, sep='\t', usecols=['patent_id', 'disambig_assignee_individual_name_first', 'disambig_assignee_individual_name_last', 'disambig_assignee_organization','assignee_sequence','assignee_type', 'location_id'], dtype=str)
    g_assignee_df = pd.read_csv(g_assignee, sep='\t', usecols=['patent_id', 'disambig_assignee_individual_name_first', 'disambig_assignee_individual_name_last', 'disambig_assignee_organization','assignee_sequence','assignee_type', 'location_id'], dtype={'patent_id': str, 'disambig_assignee_individual_name_first': str, 'disambig_assignee_individual_name_last': str, 'disambig_assignee_organization': str, 'assignee_type': str, 'location_id': str}, converters={'assignee_sequence': convert_assignee_sequence})

    # Duplicate the 'assignee_type' column to create two new columns
    g_assignee_df['assignee_type'] = g_assignee_df['assignee_type'].fillna('0').astype(int)
    g_assignee_df['assignee_type_reg'] = g_assignee_df['assignee_type']

    # Apply the 'assignee_type_mapping_reg' mappings
    g_assignee_df['assignee_type_reg'] = g_assignee_df['assignee_type_reg'].map(assignee_type_mapping_reg).fillna(
        'Unknown')

    # Handle incompatible data types
    g_assignee_df['assignee_type_reg'] = g_assignee_df['assignee_type_reg'].astype(str)

    # Merge 'disambig_assignee_individual_name_first' and 'disambig_assignee_individual_name_last' into 'assignee_name'
    g_assignee_df['assignee_name'] = g_assignee_df['disambig_assignee_organization']
    empty_mask = g_assignee_df['assignee_name'].isnull()
    g_assignee_df.loc[empty_mask, 'assignee_name'] = g_assignee_df['disambig_assignee_individual_name_first'] + ' ' + \
                                                     g_assignee_df['disambig_assignee_individual_name_last']
    g_assignee_df.drop(columns=['disambig_assignee_individual_name_first', 'disambig_assignee_individual_name_last',
                                'disambig_assignee_organization'], inplace=True)


    final_data = left_join(joined_g_cpc, g_assignee_df, 'patent_id')


    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()

    # Step 4: Left join final_data with g_location based on location_id
    print('Step 4: Performing left join between final_data and g_location...')
    start_time = time.time()
    g_location_df = pd.read_csv(g_location, sep='\t', usecols=['location_id', 'disambig_state', 'disambig_country'], dtype=str)
    final_data = left_join(final_data, g_location_df, 'location_id')
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()

    # Step 5: Remove cpc_sequence and location_id columns from the final_data
    final_data.drop(columns=['cpc_sequence', 'location_id', 'combined_text'], inplace=True)

    # Step 5-1: Find the maximum assignee_sequence for each patent_id
    max_assignee_sequence = g_assignee_df.groupby('patent_id')['assignee_sequence'].max().reset_index()

    # Step 5-2: Merge max_assignee_sequence with final_data
    final_data = final_data.groupby('patent_id').apply(aggregate_assignees_reg).reset_index(drop=True)

    # Split the 'assignee_type_reg' column on '&'
    split_assignee_types = final_data['assignee_type_reg'].str.split('&')

    # Map and join each part with '&'
    unified_assignee_types = split_assignee_types.apply(
        lambda parts: ' & '.join([assignee_type_mapping_unified.get(part.strip(), part.strip()) for part in parts]))

    # Assign the result to the 'assignee_type_unified' column
    final_data['assignee_type_unified'] = unified_assignee_types

    # Step 5-3: Merge max_assignee_sequence with final_data
    final_data = pd.merge(final_data, max_assignee_sequence, on='patent_id', how='left')

    # Step 5-4: Print the number of distinct patent IDs with sequence=0
    num_distinct_patents = final_data['patent_id'].nunique()
    print("Number of distinct patent IDs with sequence=0:", num_distinct_patents)

    # Step 6: Store the final_data in a CSV
    output_file = 'final_data_blockchain_2024_n_1.csv'
    final_data.to_csv(output_file, index=False)
    print(f"Final data has been stored in '{output_file}'.")
    print()

if __name__ == "__main__":
    main()
