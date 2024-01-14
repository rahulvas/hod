import polars as pl
import numpy as np
from datetime import datetime

####
# The Early Bird
# The investigator called the phone number you found and left a message, and a man soon called back:

# “Wow, that was years ago! It was quite an elegant tapestry.

# “It took a lot of patience, but I did manage to get the dirt out of one section, which uncovered a superb owl. I put it up on my wall, and sometimes at night I swear I could hear the owl hooting.

# “A few weeks later my bike chain broke on the way home, and I needed to get it fixed before work the next day. Thankfully, this woman I met on Tinder came over at 5am with her bike chain repair kit and some pastries from Noah’s. Apparently she liked to get up before dawn and claim the first pastries that came out of the oven.

# “I didn’t have any money or I would’ve paid her for her trouble. She really liked the tapestry, though, so I wound up giving it to her.

# “I don’t remember her name or anything else about her.”

# Can you find the bicycle fixer’s phone number?
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')

baked_goods = prod.filter(pl.col('sku').str.to_lowercase().str.contains('bky'))
baked_good_orders = order_items.join(baked_goods.select('sku'), on = 'sku').select('orderid').unique()
baked_good_orders = orders.join(baked_good_orders, on = 'orderid')
baked_good_orders = baked_good_orders.select('*', pl.col('ordered').apply(lambda x : datetime.strptime(x,'%Y-%m-%d %H:%M:%S').hour).alias('time_of_day'))
early_orders = baked_good_orders.filter(pl.col('time_of_day') <= 5)
early_orderer = early_orders.groupby('customerid').agg(order_count = pl.count('orderid')).sort('order_count', descending = True)['customerid'][0]

answer = cust.filter(pl.col('customerid') == early_orderer)['phone'][0]

print(answer)