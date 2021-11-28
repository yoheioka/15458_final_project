# 15458_final_project

## To Run
1. `pip install -r requirements.txt`
2. Create mysql table. Commands found in `schemas`
3. Run script to fetch raw data from Oanda and save in mysql
    - `python scripts/0_fetch_oanda_dataset.py 1 EUR_USD`
4. Run script to add binary indicators
    - `python scripts/1_transform_binary_classification.py 1 EUR_USD`
