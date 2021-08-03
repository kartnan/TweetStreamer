import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import pandas as pd
import string
import config
from datetime import datetime
from pytz import timezone
import re
import logging
import time
import queries as q
import sqlalchemy
from sqlalchemy import text
import unicodedata
import emoji
from string import punctuation

punc_table = {ord(c): None for c in punctuation}
gruber = re.compile(
    r'''(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))''')

# Variables that contains the credentials to access Twitter API
ACCESS_TOKEN = 'xxxxxxxxxxxxxxxxxxx'
ACCESS_SECRET = 'xxxxxxxxxxxxxxxxxxx'
CONSUMER_KEY = 'xxxxxxxxxxxxxxxxxxx'
CONSUMER_SECRET = 'xxxxxxxxxxxxxxxxxxx'

logger = logging.getLogger()


# Setup access to API
def connect_to_twitter_OAuth():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
    try:
        api.verify_credentials()
    except Exception as e:
        logger.error("Error creating API", exc_info=True)
        raise e
    logger.info("API created")
    return api


# Create API object
api = connect_to_twitter_OAuth()


def clean_tweet(tweet):
    # return ' '.join(re.sub('(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', ' ', tweet).split())
    # return ' '.join(re.sub('([^0-9A-Za-z@#:/\/. \t])', ' ', tweet).split())
    # tweet = tweet.translate(str.maketrans('', '', string.punctuation))
    try:
        tweet = emoji.get_emoji_regexp().sub(u'', tweet)
    except:
        pass
    try:
        tweet = str("".join(t if i % 2 else t.translate(punc_table) for (i, t) in enumerate(gruber.split(tweet))))
    except:
        pass
    # return ' '.join(re.sub(r"[-()\"#/@;:<>{}=~|.?,]", '', str(tweet)).split())
    return tweet

def strip_accents(text):
    """
    Strip accents from input String.

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    try:
        text = unicode(text, 'utf-8')
    except (TypeError, NameError): # unicode is a default on python 3
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)

# Create list for column names
COLUMNS = ["Tweet_ID", "Username", "Display_Name", "User_ID", "User_Location",
"Description", "User_URL", "Total_Followers", "Total_Following", "User_Since", "Favourites_Count", "UTC_Offset",
"User_Time_Zone", "Location_Enabled", "User_Verified", "Total_Tweets", "User_Language", "Tweet_URL", "Tweet_Text",
"Tweet_Date", "Hashtags_in_Tweet", "URLs_in_Tweet", "Mentions_in_Tweet", "Replying_to", "Language_Code",
"Tweet_Language", "Location", "Coordinates", "Place_Type", "Place_Name", "Country", "Tweet_Coordinates",
"Total_Retweets", "Total_Likes", "Quoted_Tweet", "Quoted_Tweet_From", "Quoted_Tweet_ID", "Quoted_User_Location",
"Quoted_User_Bio", "Quoted_User_Bio_URL", "Quoted_Status", "Quoted_Tweet_Text"]

Insert1 = '''INSERT INTO "Hashtwts" ("Tweet_ID", "Username", "Display_Name", "User_ID", "User_Location", 
"Description", "User_URL", "Total_Followers", "Total_Following", "User_Since", "Favourites_Count", "UTC_Offset", 
"User_Time_Zone", "Location_Enabled", "User_Verified", "Total_Tweets", "User_Language", "Tweet_URL", "Tweet_Text", 
"Tweet_Date", "Hashtags_in_Tweet", "URLs_in_Tweet", "Mentions_in_Tweet", "Replying_to", "Language_Code", 
"Tweet_Language", "Location", "Coordinates", "Place_Type", "Place_Name", "Country", "Tweet_Coordinates", 
"Total_Retweets", "Total_Likes", "Quoted_Tweet", "Quoted_Tweet_From", "Quoted_Tweet_ID", "Quoted_User_Location", 
"Quoted_User_Bio", "Quoted_User_Bio_URL", "Quoted_Status", "Quoted_Tweet_Text") VALUES '''


def location(loc):
    if loc is None:
        return ["None", "None", "None", "None"]
    else:
        return loc['place_type'], loc['full_name'], loc['country'], loc['bounding_box']['coordinates']

def dms2dd(loc):
    loc = "".join(loc.split('\xa0'))
    dms_lat, dms_lon = loc.split("  ")
    deg1, minutes1, seconds1, direction1 = re.split('[°\'"]', dms_lat)
    deg2, minutes2, seconds2, direction2 = re.split('[°\'"]', dms_lon)
    lat = (float(deg1) + float(minutes1)/60 + float(seconds1)/(60*60)) * (-1 if direction1 in ['W', 'S'] else 1)
    lon = (float(deg2) + float(minutes2)/60 + float(seconds2)/(60*60)) * (-1 if direction2 in ['W', 'S'] else 1)
    return str(str(lat) + ", "+str(lon))

def quote_tweet(twt, t):
    if twt is False:
        return ["False", "None", 0, "None", "None", "None", "None", "None"]
    else:
        twt = t
        try:
            return str(twt['is_quote_status']), str(twt['retweeted_status']['quoted_status']['user']['name']) + " - " + \
                   str(twt['retweeted_status']['quoted_status']['user']['screen_name']), \
                   str(twt['retweeted_status']['quoted_status']['user']['id']), \
                   str(dms2dd(twt['retweeted_status']['quoted_status']['user']['location'])), \
                   str(clean_tweet(twt['retweeted_status']['quoted_status']['user']['description'])), \
                   "None" if twt['retweeted_status']['quoted_status']['user']['url'] is None else \
                       str(twt['retweeted_status']['quoted_status']['user']['entities']['url']['urls'][0]['expanded_url']), \
                   'https://twitter.com/' + str(twt['retweeted_status']['quoted_status']['user']['screen_name']) + '/status/' + str(twt['retweeted_status']['quoted_status']['id']), \
                   str(clean_tweet(twt['retweeted_status']['quoted_status']['full_text']))
        except:
            try:
                return str(twt['is_quote_status']), str(twt['quoted_status']['user']['name']) + " - " + str(twt['quoted_status']['user']['screen_name']), \
                   str(twt['quoted_status']['user']['id']), \
                   str(dms2dd(twt['quoted_status']['user']['location'])), \
                   str(clean_tweet(twt['quoted_status']['user']['description'])), \
                   "None" if twt['quoted_status']['user']['url'] is None else \
                       str(twt['quoted_status']['user']['entities']['url']['urls'][0]['expanded_url']), \
                   'https://twitter.com/' + str(twt['quoted_status']['user']['screen_name']) + '/status/' + str(twt['quoted_status']['id']), \
                   str(clean_tweet(twt['quoted_status']['full_text']))
            except:

                return str(twt['is_quote_status']), "None", 0, "None", "None", "None", "None", "None"

def full_txt(twt):
    if twt['full_text'][-1] == '…':
        try:
            if twt['retweeted_status']:
                return twt['retweeted_status']['full_text']
        except:
            return twt['full_text']
    else:
        return twt['full_text']


def write_tweets(keyword, date_since, date_until, count=0):
    # create dataframe from defined column list
    df = pd.DataFrame(columns=COLUMNS)
    # until = datetime.strptime(date_until, '%Y-%m-%d')
    # since = datetime.strptime(date_since, '%Y-%m-%d')
    tweets_stalin = tweepy.Cursor(api.search, q=keyword, until=date_until, since=date_since,tweet_mode='extended', include_rts=False).items(50000)

    for tweets in tweets_stalin:
        try:

            # t = tweets_stalin.next()

            # storing all JSON data from twitter API
            t = tweets._json
            # print(t)
            # creating string array
            new_entry = []
            # Time from UTC to IST -->>
            # from datetime import datetime, timedelta
            # f'{datetime.strptime("Tue Nov 24 23:31:32 +0000 2020", "%a %b %d %H:%M:%S %z %Y") + timedelta(hours=5.5):%d-%b-%Y %I:%M:%S %p IST +0530}'

            place_type, full_name, country, bounding_box_coordinates = location(t['place'])
            quoted_twt, quoted_twt_user_name, quoted_twt_user_screen_name, quoted_twt_user_location, quoted_twt_user_description, quoted_twt_user_url, quoted_twt_user_tweet_link, quoted_twt_full_text = quote_tweet(t['is_quote_status'], t)
            print(f'{datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y"):%d-%b-%Y %I:%M:%S %p %Z %z}')
            # print("-"*30)
            # print(str(full_txt(t)))
            # print("-"*30)
            new_entry += [t['id'],
                          str(t['user']['screen_name']),
                          str(clean_tweet(t['user']['name'])),
                          t['user']['id'],
                          "None" if not t['user']['location'] else str(clean_tweet(t['user']['location'])),
                          "None" if not t['user']['description'] else str(clean_tweet(t['user']['description'])),
                          "None" if t['user']['url'] is None else str(t['user']['entities']['url']['urls'][0][
                                                                          'expanded_url']),
                          t['user']['followers_count'],
                          t['user']['friends_count'],
                          f'{datetime.strptime(t["user"]["created_at"], "%a %b %d %H:%M:%S %z %Y"):%d-%b-%Y %I:%M:%S %p %Z %z}',
                          t['user']['favourites_count'],
                          str(t['user']['utc_offset']),
                          str(t['user']['time_zone']),
                          str(t['user']['geo_enabled']),
                          str(t['user']['verified']),
                          t['user']['statuses_count'],
                          str(t['user']['lang']),
                          'https://twitter.com/' + str(t['user']['screen_name']) + '/status/' + str(t['id']),
                          str(clean_tweet(full_txt(t))),
                          f'{datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y"):%d-%b-%Y %I:%M:%S %p %Z %z}',
                          str(",".join(['#' + i['text'] for i in t['entities']['hashtags']])),
                          "None" if not t['entities']['urls'] else str(
                              "".join([i['expanded_url'] for i in t['entities']['urls']])),
                          "None" if not t['entities']['user_mentions'] else str(
                              ",".join([str(i['screen_name']) + " - " + str(clean_tweet(strip_accents(i['name']))) for i in t['entities']['user_mentions']])),
                          "None" if t['in_reply_to_screen_name'] is None else str(t['in_reply_to_screen_name']),
                          str(t['metadata']['iso_language_code']),
                          str(t['lang']),
                          str(t['geo']),
                          0 if t['coordinates'] is None else t['coordinates'],
                          str(place_type),
                          str(full_name),
                          str(country),
                          str(bounding_box_coordinates),
                          t['retweet_count'],
                          t['favorite_count'],
                          str(quoted_twt),
                          str(quoted_twt_user_name),
                          str(quoted_twt_user_screen_name),
                          str(quoted_twt_user_location),
                          str(quoted_twt_user_description),
                          str(quoted_twt_user_url),
                          str(quoted_twt_user_tweet_link),
                          str(quoted_twt_full_text)]

            # Append the JSON parsed data to the string list:

            # print(new_entry)
            # print(len(new_entry))
            # Try-Catch blocks can act as empty placeholders
            # try:
            #     place = tweet['place']['name']
            # except TypeError:
            #     place = 'no place'
            # new_entry.append(place)

            # wrap up all the data into a data frame
            #            single_tweet_df = pd.DataFrame([new_entry], columns=COLUMNS)
            #            df = df.append(single_tweet_df, ignore_index=True)

            # get rid of tweets without a place
            # df_cleaned = df[df.place != 'no place']
            #            df.to_excel('MKS_Tweets_17-06.xlsx', columns=COLUMNS, index=False, encoding='utf-8')
            # time.sleep(2)

            q.dbConnect(new_entry)
            count = count + 1
            print("Query {} executed".format(count))

        except tweepy.RateLimitError as e:
            logging.error("Twitter api rate limit reached".format(e))
            time.sleep(60)
            continue

        except tweepy.TweepError as e:
            logging.error("Tweepy error occured:{}".format(e))
            time.sleep(60)
            continue
            # break

        except StopIteration:
            break

        except Exception as e:
            logger.error("Failed while fetching replies {}".format(e))
            break


# Give Location range
# geo='48.136353, 11.575004, 25km'

# Twitter API doesn't allow pulls to go back that far so ~weeks is all we can get
date_since = "2021-07-27"
date_until = "2021-07-30"

# search_words = ["#StalinGoBacktoModi -filter:retweets", '#stalin_total_surrender -filter:retweets', '#DelhiWelcomesStalin -RT']

#search_words = "#KonguNadu -filter:retweets OR #கொங்குநாடு -filter:retweets"
search_words = "#திமுக_சொன்னீங்களே_செஞ்சீங்களா -filter:retweets"

# call main method passing keywords and file path
write_tweets(search_words, date_since, date_until)
