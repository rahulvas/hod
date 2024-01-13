import polars as pl
import numpy as np


####
# The Investigator
# Sarah brought a cashier over. She said, “Joe here says that one of our customers is a skilled private investigator.”

# Joe nodded, “They showed me their business card, and that’s what it said. Skilled Private Investigator. And their phone number was their last name spelled out. I didn’t know what that meant, but apparently before there were smartphones, people had to remember phone numbers or write them down. If you wanted a phone number that was easy-to-remember, you could get a number that spelled something using the letters printed on the phone buttons: like 2 has “ABC”, and 3 “DEF”, etc. And I guess this person had done that, so if you dialed the numbers corresponding to the letters in their name, it would call their phone number!

# “I thought that was pretty cool. But I don’t remember their name, or anything else about them for that matter. I couldn’t even tell you if they were male or female.”

# Sarah said, “This person seems like they are skilled at investigation. I need them to find Noah’s rug before the Hanukkah dinner. I don’t know how to contact them, but apparently they shop here at Noah’s Market.”

# She nodded at the USB drive in your hand.

# “Can you find this investigator’s phone number?”
####

letter_to_num = {
    'a' : 2, 'b' : 2, 'c' : 2,
    'd' : 3, 'e' : 3, 'f' : 3,
    'g' : 4, 'h' : 4, 'i' : 4,
    'j' : 5, 'k' : 5, 'l' : 5,
    'm' : 6, 'n' : 6, 'o' : 6,
    'p' : 7, 'q' : 7, 'r' : 7, 's' : 7,
    't' : 8, 'u' : 8, 'v': 8,
    'w' : 9, 'x' : 9, 'y' : 9, 'z' : 9
}

cust = pl.read_csv('2023//5784/noahs-customers.csv')

cust = cust.select('*', 
                   pl.col('name').str.replace_all(r"(?:Jr\.|Sr\.|III$|II$|IV$|V$)","").str.strip().str.to_lowercase().alias('fixed_name'))

cust = cust.select('*', pl.col('fixed_name').apply(lambda x: x.split(" ")[-1]).alias('last_name'))

cust = cust.select('*', 
                   pl.col('last_name').apply(lambda x : "".join([str(letter_to_num[z]) for z in x])).alias('numeric_last_name'),
                   pl.col('phone').str.replace_all(r"(\-)", "").alias('phone_clean')
                   )

answer = cust.filter(pl.col('phone_clean') == pl.col('numeric_last_name')).select('phone')['phone'][0]

print(answer)