import pandas as pd
import random
from pathlib import Path

DATA_DIR = Path('data')

def add_quality_issues_to_customers(df: pd.DataFrame) -> pd.DataFrame:
    df_dirty = df.copy()
    n_rows = len(df_dirty)

    all_index = list(range(n_rows))

    index_email_missing = random.sample(all_index, int(n_rows * 0.003))
    df_dirty.loc[index_email_missing, 'email'] = ''

    remaining = list(set(all_index) - set(index_email_missing))
    index_email_invalid = random.sample(remaining, min(int(n_rows * 0.001), len(remaining)))
    df_dirty.loc[index_email_invalid, 'email'] = [random.choice(['bor@gmail..com', 'bor@com', 'bor@@gmail.com']) for _ in index_email_invalid]

    df_dirty.loc[random.sample(all_index, int(n_rows * 0.0006)), 'customer_segment'] = 'Basicos'
    df_dirty.loc[random.sample(all_index, int(n_rows * 0.0009)), 'risk_score'] = 1.23
    df_dirty.loc[random.sample(all_index, int(n_rows * 0.006)), 'city'] = 'Limasse'

    return df_dirty

def add_quality_issues_to_merchants(df: pd.DataFrame) -> pd.DataFrame:
    df_dirty = df.copy()
    n_rows = len(df_dirty)

    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.004)), 'category'] = ''
    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.002)), 'country'] = 'Peruss'
    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.003)), 'mcc_code'] = 99999

    index_name = random.sample(range(n_rows), int(n_rows * 0.005))
    df_dirty.loc[index_name, 'merchant_name'] = df_dirty.loc[index_name, 'merchant_name'].str.replace('S.A.', 'SA', regex=False)

    return df_dirty

def add_quality_issues_to_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df_dirty = df.copy()
    n_rows = len(df_dirty)

    index_amount = random.sample(range(n_rows), int(n_rows * 0.0005))
    df_dirty.loc[index_amount, 'amount'] = df_dirty.loc[index_amount, 'amount'] * -1

    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.0008)), 'status'] = 'failing'
    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.0015)), 'merchant_id'] = None
    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.001)), 'currency'] = 'SOL'
    df_dirty.loc[random.sample(range(n_rows), int(n_rows * 0.0012)), 'channel'] = 'app'

    return df_dirty

def main():
    customers = pd.read_json(DATA_DIR / 'customers.json', lines=True)
    add_quality_issues_to_customers(customers).to_json(DATA_DIR / 'customers.json', orient='records', lines=True)

    merchants = pd.read_json(DATA_DIR / 'merchants.json', lines=True)
    add_quality_issues_to_merchants(merchants).to_json(DATA_DIR / 'merchants.json', orient='records', lines=True)

    for txn_file in (DATA_DIR / 'transactions').glob('transactions_*.csv'):
        transactions = pd.read_csv(txn_file)
        add_quality_issues_to_transactions(transactions).to_csv(txn_file, index=False)

if __name__ == '__main__':
    main()