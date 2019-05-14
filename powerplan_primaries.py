import argparse
import csv
import pandas as pd

def get_unique_catalog_cd(extract_df: pd.DataFrame) -> set:
    """
    Returns a set of unique catalog_cd's from a DataFrame
    """
    return set(extract_df['CATALOG_CD'].tolist())


def get_pwrpln_unique_catalog_count(extract_df: pd.DataFrame) -> dict:
    """
    Returns a dictionary with the number of unique catalog_cd's per
    PowerPlan, given a dataframe
    """
    return extract_df.groupby('POWERPLAN_DESCRIPTION')['CATALOG_CD'].apply(set).to_dict()


def get_catalog_primary_map(extract_df: pd.DataFrame) -> dict:
    """
    Returns a mapping from catalog_cd to primary string value
    """
    unique_catalog_primary_df = extract_df[[
        'CATALOG_CD', 'PRIMARY']].drop_duplicates()
    return_dict = unique_catalog_primary_df.set_index('CATALOG_CD').to_dict()
    return return_dict['PRIMARY']


def get_pwrpln_max_unique_catalog(pwrpln_uniq_cat_cnt: dict) -> str:
    return [k for k in pwrpln_uniq_cat_cnt.keys()
            if sum(pwrpln_uniq_cat_cnt.get(k)) ==
            max([sum(n) for n in pwrpln_uniq_cat_cnt.values()])][0]


def cnvt_rep_dict_to_df(rep_dict: dict) -> pd.DataFrame:
    keys = [k for k in rep_dict.keys() for v in rep_dict[k]]
    values = [v for k in rep_dict.keys() for v in rep_dict[k]]
    return pd.DataFrame.from_dict({'PowerPlan': keys, 'Catalog_cd': values})

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('pwrpln_spreadsheet', type=str,
                        help='Path to the powerplan-synonym extract spreadsheet')
    args = parser.parse_args()
    pwrpln_df = pd.read_excel(args.pwrpln_spreadsheet)

    unique_catalog_cd = get_unique_catalog_cd(pwrpln_df)
    pwrpln_to_use = {}

    while True:
        output_file_name = input('Enter output Excel file name (default: output.xlsx): ')
        if not output_file_name:
            output_file_name = 'output.xlsx'
            break
        elif output_file_name.split('.')[-1] == 'xlsx':
            break
        else:
            output_file_name = output_file_name + '.xlsx'
            break

    pwrpln_unique_catalog_count = get_pwrpln_unique_catalog_count(pwrpln_df)
    catalog_primary_map = get_catalog_primary_map(pwrpln_df)

    while unique_catalog_cd:
        pwrpln_candidate = get_pwrpln_max_unique_catalog(
            pwrpln_unique_catalog_count)

        pwrpln_candidate_catalog = pwrpln_unique_catalog_count[pwrpln_candidate]
        pwrpln_to_use[pwrpln_candidate] = list(pwrpln_candidate_catalog)

        pwrpln_unique_catalog_count.pop(pwrpln_candidate)
        for v in pwrpln_unique_catalog_count.values():
            v -= pwrpln_candidate_catalog

        unique_catalog_cd -= pwrpln_candidate_catalog

    output_df = cnvt_rep_dict_to_df(pwrpln_to_use)
    output_df['Primary'] = output_df['Catalog_cd'].map(catalog_primary_map)
    output_df.to_excel(output_file_name, index=False)

if __name__ == "__main__":
    main()