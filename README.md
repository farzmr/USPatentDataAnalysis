

# Patent Data Processing Scripts

This repository contains several Python scripts for processing and analyzing patent data. This code is part of my master thesis for retrieving blockchain patents from the US Patent Office. Each script has specific functionality related to filtering and extracting information from patent datasets. Below are descriptions of each script, including their inputs, processing steps, and outputs.

**Note:** The results have been verified with sampling.

## Prerequisites

Before running the scripts, download the required dataset from the US Patent Office. You can find the dataset at the following link:
- [US Patent Office Data Download](https://patentsview.org/download/data-download-tables)

## Scripts Overview

### 1. `patent_whole_data_selected_words.py`

**Purpose:**  
Extracts detailed patent information based on selected words in the title and abstract. This script is designed to run on a server with no row limitations.

**Inputs:**
- `selected_words`: Words to search for in the patent title and abstract.
- `cpc_sequence`: Fixed to `0`.

**Sample Result:**
- Outputs a detailed CSV file with the following columns:
  - `patent_id, patent_type, patent_date, patent_title, patent_abstract, cpc_subclass, cpc_class, assignee_sequence_x, assignee_type, assignee_name, disambig_state, disambig_country, assignee_sequence_y`

### 2. `patent_whole_data_patent_list.py`

**Purpose:**  
Matches selected patent IDs with detailed data and extracts the desired columns. This script is also designed to run on a server with no row limitations.

**Inputs:**
- `selected_patent_id_csv`: A CSV file containing the selected patent IDs.

**Process:**
- Matches the provided patent IDs with the full dataset and extracts columns as specified.

**Sample Result:**
- Outputs a CSV file with the following columns:
  - `patent_id, patent_type, patent_date, patent_title, patent_abstract, cpc_subclass, assignee_sequence_x, assignee_type, assignee_name, disambig_state, disambig_country, assignee_sequence_y`

### 3. `num_patent_text_and_cpc.py`

**Purpose:**  
Filters patents based on selected words in the title and abstract and optionally filters by CPC subclass.

**Inputs:**
- `selected_words`: Words to search for in the patent title and abstract.
- `cpc_codes_input` (optional): CPC subclass codes to filter patents. Leave empty to ignore CPC filtering.

**Sample Result:**
- Prints the number of patents matching the criteria.
- Outputs a CSV file with the following columns:
  - `patent_id, patent_date, patent_abstract, patent_title, cpc_subclass, cpc_sequence`

### 4. `num_patent_cpc.py`

**Purpose:**  
Counts the number of patents with selected CPC codes and saves the resulting patent IDs.

**Inputs:**
- `selected_cpc`: CPC codes to filter patents.

**Sample Result:**
- Prints the number of rows (patents) with the selected CPC codes.
- Saves the resulting patent IDs to a CSV file.

