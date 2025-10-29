import os
import pandas as pd

def aggregate_gold(silver_file: str, gold_dir: str) -> str:
    import os
    import pandas as pd

    print(f"Aggregating silver file: {silver_file}")
    df = pd.read_parquet(silver_file)

    # Ensure numeric columns are correct type
    numeric_cols = ['age', 'salary', 'number_of_years_worked']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Aggregate statistics per department
    agg_df = df.groupby("department").agg(
        highsalary=('salary', 'max'),
        lowsalary=('salary', 'min'),
        minage=('age', 'min'),
        maxage=('age', 'max'),
        max_exp=('number_of_years_worked', 'max'),
        min_exp=('number_of_years_worked', 'min')
    ).reset_index()

    # Add employee names for each metric
    agg_df['highsalary_name'] = agg_df.apply(
        lambda x: df[df['department']==x['department']].loc[
            df[df['department']==x['department']]['salary'].idxmax(), 'name'
        ], axis=1
    )

    agg_df['lowsalary_name'] = agg_df.apply(
        lambda x: df[df['department']==x['department']].loc[
            df[df['department']==x['department']]['salary'].idxmin(), 'name'
        ], axis=1
    )

    agg_df['minage_name'] = agg_df.apply(
        lambda x: df[df['department']==x['department']].loc[
            df[df['department']==x['department']]['age'].idxmin(), 'name'
        ], axis=1
    )

    agg_df['maxage_name'] = agg_df.apply(
        lambda x: df[df['department']==x['department']].loc[
            df[df['department']==x['department']]['age'].idxmax(), 'name'
        ], axis=1
    )

    agg_df['maxexp_name'] = agg_df.apply(
        lambda x: df[df['department']==x['department']].loc[
            df[df['department']==x['department']]['number_of_years_worked'].idxmax(), 'name'
        ], axis=1
    )

    agg_df['minexp_name'] = agg_df.apply(
        lambda x: df[df['department']==x['department']].loc[
            df[df['department']==x['department']]['number_of_years_worked'].idxmin(), 'name'
        ], axis=1
    )

    # Save gold file
    os.makedirs(gold_dir, exist_ok=True)
    gold_file = os.path.join(gold_dir, os.path.basename(silver_file).replace("_silver.parquet", "_gold.parquet"))
    agg_df.to_parquet(gold_file, index=False)
    print(f"Saved Gold file: {gold_file}")
    return gold_file
