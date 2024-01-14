import polars as pl
import numpy as np
from datetime import datetime

####
# The Collector
# “Oh that damned woman! She moved in, clogged my bathtub, left her coupons all over the kitchen, and then just vanished one night without leaving so much as a note.

# Except she did leave behind that nasty carpet. I spent months cleaning one corner, only to discover a snake hiding in the branches! I knew then that she was never coming back, and I had to get it out of my sight.

# “Well, I don’t have any storage here, and it didn’t seem right to sell it, so I gave it to my sister. She wound up getting a newer and more expensive carpet, so she gave it to an acquaintance of hers who collects all sorts of junk. Apparently he owns an entire set of Noah’s collectibles! He probably still has the carpet, even.

# “My sister is away for the holidays, but I can have her call you in a few weeks.”

# The family dinner is tonight! Can you find the collector’s phone number in time?
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')

collectibles = prod.filter(pl.col('sku').str.contains('COL'))
num_colletibles = collectibles.shape
oi = order_items.join(collectibles, on = 'sku')
collectible_orders = orders.join(oi, on = 'orderid').groupby('customerid').agg(num_items = pl.n_unique('sku')).sort('num_items', descending = True)
cust_id = collectible_orders.filter(pl.col('num_items') == num_colletibles[0])['customerid'][0]

answer = cust.filter(pl.col('customerid') == cust_id)['phone'][0]

print(answer)