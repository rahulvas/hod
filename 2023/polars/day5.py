import polars as pl
import numpy as np
from datetime import datetime

####
# The Cat Lady
# “Yes, I did have that tapestry for a little bit. I even cleaned a blotchy section that turned out to be a friendly koala.

# “But it was still really dirty, so when I was going through a Marie Kondo phase, I decided it wasn’t sparking joy anymore.

# “I listed it on Freecycle, and a woman in Staten Island came to pick it up. She was wearing a ‘Noah’s Market’ sweatshirt, and it was just covered in cat hair. When I suggested that a clowder of cats might ruin such a fine tapestry, she looked at me funny. She said “I only have ten or eleven cats, and anyway they are getting quite old now, so I doubt they’d care about some old rug.”

# “It took her 20 minutes to stuff the tapestry into some plastic bags she brought because it was raining. I spent the evening cleaning my apartment.”

# What’s the phone number of the woman from Freecycle?
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')


cat = prod.filter(pl.col('desc').str.contains('Cat'))


cat_orders = order_items.join(cat.select('sku', 'desc'), on = 'sku').groupby('orderid').agg(num_items = pl.sum('qty'))

## at least 10 cans of food
cat_orders = orders.join(cat_orders, on = ['orderid']).filter(pl.col('num_items') >= 10)

## more than 1 time
cat_orders = cat_orders.groupby('customerid').agg(num_times = pl.count('orderid')).filter(pl.col('num_times') > 1).select('customerid')

answer = cust.join(cat_orders, on = 'customerid').filter(pl.col('citystatezip').str.contains('Staten'))['phone'][0]
print(answer)