import polars as pl
import numpy as np
from datetime import datetime

####
# The Neighbor

# Sarah and the investigator were very impressed with your data skills, as you were able to figure out the phone number of the contractor. They called up the cleaning contractor straight away and asked about the rug.

# “Oh, yeah, I did some special projects for them a few years ago. I remember that rug unfortunately. I managed to clean one section, which revealed a giant spider that startled me whenever I tried to work on it.

# “I already had a fear of spiders before this, but this spider was so realistic that I had a hard time making any more progress. I kept expecting the cleaners would call for the rug, but they never did. I felt so bad about it, I couldn’t face them, and of course they never gave me another project.

# “At last I couldn’t deal with the rug taking up my whole bathtub, so I gave it to this guy who lived in my neighborhood. He said that he was naturally intuitive because he was a Cancer born in the year of the Rabbit, so maybe he was able to clean it.

# “I don’t remember his name. Last time I saw him, he was leaving the subway and carrying a bag from Noah’s. I swore I saw a spider on his hat.”

# Can you find the phone number of the person that the contractor gave the rug to?
####

cust = pl.read_csv('2023//5784/noahs-customers.csv')
orders = pl.read_csv('2023/5784/noahs-orders.csv')
order_items = pl.read_csv('2023/5784/noahs-orders_items.csv')
prod = pl.read_csv('2023/5784/noahs-products.csv')

def calc_zodiac(date):
    """
    Chinese Zodiac Year. I know 2024 is the year of the dragon so I'll work backwords
    Every 12 years is the year of the dragon starting from 1928 (https://en.wikipedia.org/wiki/Dragon_(zodiac))
    """
    year = int(date[:4])

    z_list = ['dragon', 'snake', 'horse', 'goat', 'monkey', 'rooster', 'dog', 'pig', 'rat', 'ox', 'tiger', 'rabbit']
    ix = (year - 1928) % 12
    return z_list[ix]

def calc_star_sign(date):
    """
    There's probably an elegant way to do this, but I'm going to brute force. ChatGPT approved. 
    """
    month = int(date.split("-")[1])
    day = int(date.split("-")[2])

    if (month == 3 and day >= 21) or (month == 4 and day <= 19):
        return "Aries"
    elif (month == 4 and day >= 20) or (month == 5 and day <= 20):
        return "Taurus"
    elif (month == 5 and day >= 21) or (month == 6 and day <= 20):
        return "Gemini"
    elif (month == 6 and day >= 21) or (month == 7 and day <= 22):
        return "Cancer"
    elif (month == 7 and day >= 23) or (month == 8 and day <= 22):
        return "Leo"
    elif (month == 8 and day >= 23) or (month == 9 and day <= 22):
        return "Virgo"
    elif (month == 9 and day >= 23) or (month == 10 and day <= 22):
        return "Libra"
    elif (month == 10 and day >= 23) or (month == 11 and day <= 21):
        return "Scorpio"
    elif (month == 11 and day >= 22) or (month == 12 and day <= 21):
        return "Sagittarius"
    elif (month == 12 and day >= 22) or (month == 1 and day <= 19):
        return "Capricorn"
    elif (month == 1 and day >= 20) or (month == 2 and day <= 18):
        return "Aquarius"
    elif (month == 2 and day >= 19) or (month == 3 and day <= 20):
        return "Pisces"
    else:
        return "Invalid date"


cust = cust.select(
    '*',
    pl.col('birthdate').apply(lambda x : calc_zodiac(x)).alias('c_zodiac'),
    pl.col('birthdate').apply(lambda x : calc_star_sign(x)).alias('star_sign')
)

day2_phone = '332-274-4185'

day2_nbhd = cust.filter(pl.col('phone') == day2_phone)['citystatezip'][0]

answer = cust.filter(
    (pl.col('c_zodiac') =='rabbit')
    & (pl.col('star_sign') == 'Cancer')
    & (pl.col('citystatezip') == day2_nbhd)
    )['phone'][0]

print(answer)