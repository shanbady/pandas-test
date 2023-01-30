import pandas as pd
import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import read_csv
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam import dataframe


def reformat_row(row):
    formatted_row = beam.Row(
        legal_entity=row.legal_entity,
        counter_party=row.counter_party,
        tier=row.tier,
        max_rating_by_counter_party=row.max_rating_by_counter_party,
        sum_value_where_status_ARAP=row.sum_value_where_status_ARAP,
        sum_value_where_status_ACCR=row.sum_value_where_status_ACCR)
    return formatted_row


def sum_value_where_status_ARAP(item):
    if item.status == 'ARAP':
        return item.value
    return 0


def sum_value_where_status_ACCR(item):
    if item.status == 'ACCR':
        return item.value
    return 0


with beam.Pipeline('DirectRunner') as p:
    df_first = p | "Read first dataset" >> read_csv('dataset1.csv')
    df_second = p | "Read second dataset" >> read_csv('dataset2.csv')
    df = df_first.merge(
        df_second.set_index("counter_party").tier,
        right_index=True,
        left_on="counter_party",
        how="left",
    )
    original = to_pcollection(df)

    # group by counterparty  - get legal_entity and tier totals
    counter_party_grouped = (
        original
        | 'counter_party group' >> beam.GroupBy('counter_party')
        .aggregate_field('legal_entity', CountCombineFn(), 'legal_entity')
        .aggregate_field('tier', CountCombineFn(), 'tier')
        .aggregate_field('rating', max, 'max_rating_by_counter_party')
        .aggregate_field(
            sum_value_where_status_ARAP, sum, 'sum_value_where_status_ARAP')
        .aggregate_field(
            sum_value_where_status_ACCR, sum, 'sum_value_where_status_ACCR')
        | 'counter_party_groupedto rows' >> beam.Map(reformat_row)
    )
    final_dataframe = to_dataframe(counter_party_grouped)

    # group by legal_entity  - get counter_party and tier totals
    legal_entity_grouped = (
        original
        | 'legal_entity group' >> beam.GroupBy('legal_entity')
        .aggregate_field('counter_party', CountCombineFn(), 'counter_party')
        .aggregate_field('tier', CountCombineFn(), 'tier')
        .aggregate_field('rating', max, 'max_rating_by_counter_party')
        .aggregate_field(
            sum_value_where_status_ARAP, sum, 'sum_value_where_status_ARAP')
        .aggregate_field(
            sum_value_where_status_ACCR, sum, 'sum_value_where_status_ACCR')
        | 'legal_entity_grouped to rows' >> beam.Map(reformat_row)
    )
    final_dataframe = final_dataframe.append(
        to_dataframe(legal_entity_grouped))

    # group by tier  - get legal_entity and counter_party totals
    tier_grouped = (
        original
        | 'tier group' >> beam.GroupBy('tier')
        .aggregate_field('counter_party', CountCombineFn(), 'counter_party')
        .aggregate_field('legal_entity', CountCombineFn(), 'legal_entity')
        .aggregate_field(
            'rating',
            max,
            'max_rating_by_counter_party')
        .aggregate_field(
            sum_value_where_status_ARAP, sum, 'sum_value_where_status_ARAP')
        .aggregate_field(
            sum_value_where_status_ACCR, sum, 'sum_value_where_status_ACCR')
        | 'tier_grouped to rows' >> beam.Map(reformat_row)
    )
    final_dataframe = final_dataframe.append(to_dataframe(tier_grouped))

    # group by counterparty and legal_entity  - get tier totals
    legal_entity_counter_party_grouped = (
        original
        | 'group le and cp' >> beam.GroupBy(
            'legal_entity',
            'counter_party')
        .aggregate_field('tier', CountCombineFn(), 'tier')
        .aggregate_field('rating', max, 'max_rating_by_counter_party')
        .aggregate_field(
            sum_value_where_status_ARAP, sum, 'sum_value_where_status_ARAP')
        .aggregate_field(
            sum_value_where_status_ACCR, sum, 'sum_value_where_status_ACCR')
        | 'legal_entity_counter_party_grouped to rows' >> beam.Map(reformat_row)
    )
    final_dataframe = final_dataframe.append(
        to_dataframe(legal_entity_counter_party_grouped))

    # reformat and reorder rows before outputting
    with dataframe.allow_non_parallel_operations():
        final_dataframe = final_dataframe[[
            'legal_entity',
            'counter_party',
            'tier',
            'max_rating_by_counter_party',
            'sum_value_where_status_ARAP',
            'sum_value_where_status_ACCR'
        ]].rename(columns={
            'sum_value_where_status_ARAP': 'sum(value where status=ARAP)',
            'sum_value_where_status_ACCR': 'sum(value where status=ACCR)',
            'max_rating_by_counter_party': 'max(rating by counterparty)'
        })
    final_dataframe.to_csv('out/beam.csv', index=False)
