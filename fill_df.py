import pandas as pd
import numpy as np


def fill_df(df):

    df.loc[:, "date"] = pd.to_datetime(df.loc[:, "date"], format="%Y-%m-%d")
    df = (
        df.drop_duplicates(subset=["date", "site_title", "site_link"])
        .reset_index()
        .reset_index()
        .drop(columns="id")
        .rename(columns={"index": "id"})
        .set_index("id")
    )

    df.price_new = df.price_new.astype(float)
    start_date = df.date.values.min()
    end_date = df.date.values.max()
    daterange = pd.date_range(start=start_date, end=end_date)
    df.loc[:, "unq"] = df.category_id.astype(str) + df.site_link
    pvt_before = pd.pivot_table(df, columns="unq", index="date",
                                values="price_new")
    n_days_limit = 150
    pvt_after = (
        pvt_before.merge(
            pd.Series(index=daterange, data=np.nan, name=1),
            left_index=True,
            right_index=True,
            how="right",
        )
        .iloc[:, :-1]
        .apply(lambda x: x.ffill(limit=n_days_limit))
    )

    df = df.drop("price_new", axis=1).merge(
        pvt_after.transpose().stack().rename("price_new"),
        left_on=["unq", "date"],
        right_index=True,
        how="right",
    )
    df.loc[:, "miss"] = df.loc[:, "site_title"].isna().astype(int)
    df = df.sort_values(["unq", "date"])
    df = df.groupby("unq").transform(lambda x: x.ffill())
    df = (
        df.reset_index()
        .drop("id", axis=1)
        .reset_index()
        .rename(columns={"index": "id"})
        .set_index("id")
    )
    df.loc[:, "category_id"] = df.category_id.astype(int)
    df.loc[:, "price_old"] = df.price_old.astype(float)
    df.loc[:, "nsprice_f"] = (df.price_old == -1.0).replace(False, np.nan) * (
        df.price_new
    ).apply(lambda x: np.nan if x == 0 else x)
    df = df.groupby("site_link").apply(
        lambda x: x.ffill(limit=n_days_limit)
    )
    cond = df.nsprice_f.isna()
    df.loc[cond, "nsprice_f"] = df.loc[cond, "price_old"]

    return df
