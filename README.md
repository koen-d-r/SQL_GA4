# SQL queries for GA4


### brand_preference.sql
In this query I'm calculating the brand preference per user_pseudo_id from GA4 data.

First step
In the first step, I'm scoring each event with a specific score.

event_name 'view_item_list' scores 1 point
event_name 'view_item' scores 5 points
event_name 'add-to-cart' scores 10 points
Second step
In the second step, I'm calculating the score per user_pseudo_id and correcting the score with a so-called recency score. The longer ago, the less weight it scores. I'm calculating the brand scores with the formula below.

SUM(score * (timedelta * (1 / timedelta_dataset)))

Finally, the output is ranked and then ready for export!
