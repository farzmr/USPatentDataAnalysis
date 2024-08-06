'''
input:
selected CPC

sample result:
Prints the number of rows (patents) with selected CPCs.
save resulted patent id in csv

p.s: I have checked my result with sampling
'''

import pandas as pd
import time

# go to this address to download the dataset
# https://patentsview.org/download/data-download-tables
g_cpc = 'g_cpc_current.tsv'

# Function to filter g_cpc by sequence and list of CPCs
def filter_g_cpc_by_sequence_and_cpcs(file_path, sequence='0', cpc_prefix=''):
    columns_to_keep = ['patent_id', 'cpc_subclass', 'cpc_sequence','cpc_class','cpc_group']
    filtered_g_cpc = pd.read_csv(file_path, sep='\t', usecols=columns_to_keep, dtype=str)

    # Filter the DataFrame to keep only rows where cpc_sequence is equal to the given sequence
    filtered_g_cpc = filtered_g_cpc[filtered_g_cpc['cpc_sequence'] == str(sequence)]

    # Filter further by CPC prefix
    if cpc_prefix:
        filtered_g_cpc = filtered_g_cpc[filtered_g_cpc['cpc_subclass'].isin(cpc_prefix)]

    return filtered_g_cpc

#Main
def main():
    cpc_prefix_to_filter = ['F24S','H02S']

    # Step 2: Filter g_cpc by sequence and list of CPCs
    print('Step 2: Filtering g_cpc based on sequence=0 and CPC prefix...')
    start_time = time.time()
    filtered_g_cpc = filter_g_cpc_by_sequence_and_cpcs(g_cpc, sequence='0', cpc_prefix=cpc_prefix_to_filter)
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    print()

    # Step 4: Store the filtered data in a CSV
    output_file = 'num_patent_cpc_solar.csv'
    filtered_g_cpc.to_csv(output_file, index=False)
    print(f"'num_patent_cpc' data has been stored in '{output_file}'.")
    print()

    # Step 5: Print the number of distinct patent IDs with sequence=0
    num_distinct_patents = filtered_g_cpc['patent_id'].nunique()
    print("Number of distinct patent IDs with sequence=0 and CPC prefix:", num_distinct_patents)

if __name__ == "__main__":
    main()
