import polars as pl
import numpy as np
from datetime import datetime

####
# The Bargain Hunter
# “Why yes, I did have that rug for a little while in my living room! My cats can’t see a thing but they sure chased after the squirrel on it like it was dancing in front of their noses.

# “It was a nice rug and they were surely going to ruin it, so I gave it to my cousin, who was moving into a new place that had wood floors.

# “She refused to buy a new rug for herself–she said they were way too expensive. She’s always been very frugal, and she clips every coupon and shops every sale at Noah’s Market. In fact I like to tease her that Noah actually loses money whenever she comes in the store.

# “I think she’s been taking it too far lately though. Once the subway fare increased, she stopped coming to visit me. And she’s really slow to respond to my texts. I hope she remembers to invite me to the family reunion next year.”

# Can you find her cousin’s phone number?
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')

oi = order_items.join(prod.select('sku', 'wholesale_cost'), on = 'sku')
oi = oi.select('*', (pl.col('qty') * pl.col('wholesale_cost')).alias('total_wholesale_cost')).groupby('orderid').agg(total_wholesale_cost = pl.sum('total_wholesale_cost'))

money_loss = orders.join(oi, on = 'orderid').filter(pl.col('total') <= pl.col('total_wholesale_cost'))
money_loss = money_loss.groupby('customerid').agg(num_times = pl.count('orderid'))
answer = cust.join(money_loss, on = 'customerid').sort('num_times', descending = True)['phone'][0]

print(answer)