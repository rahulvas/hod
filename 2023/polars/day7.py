import polars as pl
import numpy as np
from datetime import datetime

####
# 7. The Meet Cute
# “Oh that tapestry, with the colorful toucan on it! I’ll tell you what happened to it.

# “One day, I was at Noah’s Market, and I was just about to leave when someone behind me said ‘Miss! You dropped something!’

# “Well I turned around to see this cute guy holding an item I had bought. He said, ‘I got the same thing!’ We laughed about it and wound up swapping items because I wanted the color he got. We had a moment when our eyes met and my heart stopped for a second. I asked him to get some food with me and we spent the rest of the day together.

# “Before long I moved into his place, but the romance faded quickly, as he wasn’t the prince I imagined. I left abruptly one night, forgetting the tapestry on his wall. But by then, it symbolized our love, and I wanted nothing more to do with it. For all I know, he still has it.”

# Can you figure out her ex-boyfriend’s phone number?
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')

prod = prod.select(
    '*',
    pl.col('desc').str.extract(r"\((.*?)\)", 1).alias("color"),
    pl.col('desc').str.replace(r" \([^)]+\)", "").alias("base_item")
)

collectibles = prod.filter(pl.col('sku').str.contains('COL'))

day6_ans = '585-838-9161'
day6_customerid = cust.filter(pl.col('phone') == day6_ans).select('customerid')['customerid'][0]

orders = orders.join(order_items.join(collectibles, on = 'sku'), on = 'orderid')

## lags and leads
orders = orders.sort('ordered', descending = False)
lorders = orders.select(
    '*',
    pl.col('base_item').shift(1).alias('prev_item'),
    pl.col('color').shift(1).alias('prev_color'),
    pl.col('customerid').shift(1).alias('prev_customerid')
)

prev_cust_id = lorders.filter(
    (pl.col('customerid') == day6_customerid)
    & (pl.col('base_item') == pl.col('prev_item'))
    ).select('prev_customerid')['prev_customerid'][0]

answer = cust.filter(pl.col('customerid') == prev_cust_id)['phone'][0]

print(answer)