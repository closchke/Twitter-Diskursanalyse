

def sort_week_year(input_df):
    # sort by week_year
    input_df["year"] = input_df["week_year"].str.split("-", n = 1, expand = True)[1]
    input_df["week"] = input_df["week_year"].str.split("-", n = 1, expand = True)[0]
    # convert year and week to int
    input_df["year"] = input_df["year"].astype(int)
    input_df["week"] = input_df["week"].astype(int)

    # sort by year first and then by week
    input_df = input_df.sort_values(by=['year', 'week']).reset_index(drop=True)
    # drop year and week columns
    input_df = input_df.drop(columns=["year", "week"])
    return input_df