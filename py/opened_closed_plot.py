
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import DateFormatter

# Load the data
df = pd.read_csv("cleaned_data.csv")
df = df[pd.to_datetime(df['creationDate']) >= '2025-04-12']

# Calculate closed counts
closed_df = (df[df['ad_is_closed']]
             .assign(dt=pd.to_datetime(df['editDate'], format = 'ISO8601').dt.date)
             .groupby(['ad_deal_type', 'dt'])
             .size()
             .reset_index(name='close_cnt'))

# Calculate opened counts
opened_df = (df
             .assign(dt=pd.to_datetime(df['creationDate']).dt.date)
             .groupby(['ad_deal_type', 'dt'])
             .size()
             .reset_index(name='open_cnt'))

# Merge and melt the data
merged_df = pd.merge(opened_df, closed_df, on=['ad_deal_type', 'dt'], how='inner')
melted_df = merged_df.melt(id_vars=['ad_deal_type', 'dt'], 
                           value_vars=['open_cnt', 'close_cnt'],
                           var_name='name', value_name='value')

# Plotting
g = sns.FacetGrid(melted_df, col='ad_deal_type', sharey=False, col_wrap=2, height=4)
g.map_dataframe(sns.lineplot, x='dt', y='value', hue='name', palette='Set1')

# Update x-axis and y-axis labels
for ax in g.axes.flat:
    ax.xaxis.set_major_formatter(DateFormatter('%d %b'))
    ax.set_ylabel('')
    ax.set_xlabel('Date')
    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_horizontalalignment('right')

# Update plot titles
g.set_titles(col_template="{col_name}")

# Add legend
g.add_legend()

plt.show()