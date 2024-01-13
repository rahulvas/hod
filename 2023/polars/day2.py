import polars as pl
import numpy as np

####
# The Contractor
# Thanks to your help, Sarah called the investigator that afternoon. The investigator went directly to the cleaners to see if they could get any more information about the unclaimed rug.

# While they were out, Sarah said, “I tried cleaning the rug myself, but there was this snail on it that always seemed to leave a trail of slime behind it. I spent a few hours cleaning it, and the next day the slime trail was back.”

# When the investigator returned, they said, “Apparently, this cleaner had a special projects program, where they outsourced challenging cleaning projects to industrious contractors. As they’re right across the street from Noah’s, they usually talked about the project over coffee and bagels at Noah’s before handing off the item to be cleaned. The contractors would pick up the tab and expense it, along with their cleaning supplies.

# “So this rug was apparently one of those special projects. The claim ticket said ‘2017 JP’. ‘2017’ is the year the item was brought in, and ‘JP’ is the initials of the contractor.

# “But they stopped outsourcing a few years ago, and don’t have contact information for any of these workers anymore.”

# Sarah first seemed hopeless, and then glanced at the USB drive you had just put back in her hand. She said, “I know it’s a long shot, but is there any chance you could find their phone number?”
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')

### tried coffee sku with too much noise. bagel sku gives me one answer
bagel_sku = prod.filter(pl.col('desc').str.to_lowercase().str.contains('bagel'))['sku'][0]
orders_with_bagels = orders.join(order_items.filter(pl.col('sku') == bagel_sku).select('orderid'), on = 'orderid')

customers_with_bagels = cust.join(orders_with_bagels, on = 'customerid')
customers_with_bagels = customers_with_bagels.select('*',
                                                     pl.col('name').apply(lambda x : x.split(" ")[0]).alias('first_name'),
                                                     pl.col('name').apply(lambda x : x.split(" ")[1]).alias('last_name'))

answer = customers_with_bagels.filter(
    (pl.col('first_name').str.starts_with('J'))
    & (pl.col('last_name').str.starts_with('P'))
)['phone'][0]

print(answer)