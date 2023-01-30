import pandas as pd


# merge given datasets
def merge_datasets(df_1, df_2, on_column):
    df = df_1.merge(df_2,  how="left", left_on=on_column, right_on=on_column)
    return df


def get_status_value_sums(df, groupby):
    table = pd.pivot_table(df,
                           values=['rating', 'value'],
                           index=groupby,
                           columns=['status'],
                           aggfunc=sum,
                           fill_value=0
                           )
    table['sum(value where status=ARAP)'] = table['value']['ARAP']
    table['sum(value where status=ACCR)'] = table['value']['ACCR']
    table = table.reset_index()
    table = table[groupby + [
        'sum(value where status=ARAP)',
        'sum(value where status=ACCR)'
    ]]
    return table.stack()


def get_max_values(df, groupby=[]):
    table = pd.pivot_table(df,
                           values=['rating', 'value'],
                           index=groupby,
                           columns=['status'],
                           aggfunc=max,
                           fill_value=0)
    table[
        'max(rating by counterparty)'] = table['rating'].loc[
        :,
        ['ARAP', 'ACCR']
    ].max(axis=1)
    table = table.reset_index()
    table = table[groupby + ['max(rating by counterparty)']]
    return table.stack()


def get_totals(df, index_cols, totals_cols):
    table = pd.pivot_table(df, values=totals_cols, index=index_cols,
                           aggfunc=pd.Series.nunique, fill_value=0)
    return pd.DataFrame(table.to_records())


def get_col_stats(df, index_cols, total_cols):
    max_status_df = get_max_values(df, index_cols)
    sum_values_df = get_status_value_sums(df, index_cols)
    sum_values_df[
        'max(rating by counterparty)'
    ] = max_status_df['max(rating by counterparty)']
    merged_df = sum_values_df
    if len(total_cols) > 0:
        totals_df = get_totals(df, index_cols,  total_cols)
        return merge_datasets(merged_df, totals_df, on_column=index_cols)
    return max_status_df.reset_index()[index_cols + [
        'max(rating by counterparty)',
        'sum(value where status=ARAP)',
        'sum(value where status=ACCR)']
    ]


if __name__ == '__main__':
    # read in files and merge to get tier
    dataset1_df = pd.read_csv('dataset1.csv')
    dataset2_df = pd.read_csv('dataset2.csv')
    df = merge_datasets(dataset1_df, dataset2_df, on_column="counter_party")

    # get grouped by legal_entity 
    merged_df = get_col_stats(df, index_cols=['legal_entity'], total_cols=[
                              'counter_party', 'tier'])

    # get grouped by tier 
    merged_df = merged_df.append(get_col_stats(
        df, index_cols=['tier'], total_cols=['legal_entity', 'counter_party']))

    # get grouped by counter_party
    merged_df = merged_df.append(get_col_stats(
        df, index_cols=['counter_party'], total_cols=['legal_entity', 'tier']))

    # get grouped by legal_entity and counter_party
    merged_df = merged_df.append(get_col_stats(
        df, index_cols=['legal_entity', 'counter_party'], total_cols=['tier']))

    # reorder columns for output
    merged_df = merged_df[[
        'legal_entity',
        'counter_party',
        'tier',
        'max(rating by counterparty)',
        'sum(value where status=ARAP)',
        'sum(value where status=ACCR)'
    ]]
    merged_df.to_csv('out/pandas.csv', index=False)
