import pandas as pd
import os

def sum_year(df, output_dir):
    rain_df = df.groupby('year')['rain'].sum().reset_index()
    rain_df.to_csv(f'{output_dir}rain_sum_peryear.csv')
    rain_df = rain_df.rename(columns={'year': 'year_each' ,'rain': "rain_each"})
    # print(rain_df.info())
    return rain_df

def main():
    df = pd.read_csv('../input/wheat_weather.csv',
                    usecols=['year', 'rain', 'timestamp', 'month', 'past_year'])

    df['cumulative_sum'] = df.groupby(['year', 'month'])['rain'].cumsum()
    df['cumulative_sum_year'] = df.groupby('past_year')['rain'].cumsum()
    df['year'] = df['year'].astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    select = []
    for year in range(1983, 2023):
        select_year = pd.concat([df[df['year'] == year], df[df['year'] == year + 1]])
        select_year = select_year[((select_year["timestamp"].dt.strftime("%Y-%m-%d") >= f'{year}-10-01') & (select_year["timestamp"].dt.strftime("%Y-%m-%d") <= f'{year+1}-04-15'))]
        select.append(select_year)

    select = pd.concat(select, axis=1, ignore_index=False)



    output_dir = "../output/"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_filename = os.path.join(output_dir, "wheat_weather_result.csv")

    sum_year(df, output_dir)
    select.to_csv(output_filename)
    # all = pd.concat([select, sum_year(df, output_dir)], axis=0)
    # all.to_csv(f"{output_dir}all_summary.csv")

if __name__ == '__main__':
    main()