import psycopg2


def dbConnect(query):

    conn = psycopg2.connect(host="localhost", database="xxxxxx", port=5432, user='xxxxx', password='xxxxxx')
    cur = conn.cursor()
    Create = '''CREATE TABLE IF NOT EXISTS "Hashtwts_1" ("Tweet_ID" BIGINT PRIMARY KEY, "Username" TEXT, "Display_Name" TEXT, "User_ID" BIGINT, "User_Location" TEXT, "Description" TEXT, "User_URL" TEXT, "Total_Followers" INT, "Total_Following" INT, "User_Since" TEXT, "Favourites_Count" INT, "UTC_Offset" TEXT, "User_Time_Zone" TEXT, "Location_Enabled" TEXT, "User_Verified" TEXT, "Total_Tweets" INT, "User_Language" TEXT, "Tweet_URL" TEXT, "Tweet_Text" TEXT, "Tweet_Date" TEXT, "Hashtags_in_Tweet" TEXT, "URLs_in_Tweet" TEXT, "Mentions_in_Tweet" TEXT, "Replying_to" TEXT, "Language_Code" TEXT, "Tweet_Language" TEXT, "Location" TEXT, "Coordinates" INT, "Place_Type" TEXT, "Place_Name" TEXT, "Country" TEXT, "Tweet_Coordinates" TEXT, "Total_Retweets" INT, "Total_Likes" INT, "Quoted_Tweet" TEXT, "Quoted_Tweet_From" TEXT, "Quoted_Tweet_ID" BIGINT, "Quoted_User_Location" TEXT, "Quoted_User_Bio" TEXT, "Quoted_User_Bio_URL" TEXT, "Quoted_Status" TEXT, "Quoted_Tweet_Text" TEXT)'''

    # Insert = '''INSERT INTO "Hashtwts" (t['id'], t['user']['screen_name'], t['user']['name'], t['user']['id'], t['user']['location'], t['user']['description'],
    #                           "None" if t['user']['url'] is None else t['user']['entities']['url']['urls'][0]['expanded_url'],
    #                           t['user']['followers_count'], t['user']['friends_count'],
    #                           f'{datetime.strptime(t["user"]["created_at"], "%a %b %d %H:%M:%S %z %Y"):%d-%b-%Y %I:%M:%S %p %Z %z}',
    #                           t['user']['favourites_count'], t['user']['utc_offset'], t['user']['time_zone'], t['user']['geo_enabled'],
    #                           t['user']['verified'], t['user']['statuses_count'], t['user']['lang'],
    #                           'https://twitter.com/' + t['user']['screen_name'] + '/status/' + str(t['id']), t['full_text'],
    #                           f'{datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y"):%d-%b-%Y %I:%M:%S %p %Z %z}',
    #                           [i['text'] for i in t['entities']['hashtags']], [i['expanded_url'] for i in t['entities']['urls']],
    #                           [i for i in t['entities']['user_mentions']], t['in_reply_to_screen_name'],
    #                           t['metadata']['iso_language_code'],
    #                           t['geo'], t['coordinates'], place_type, full_name, country, bounding_box_coordinates, t['retweet_count'], t['favorite_count'],
    #                           quoted_twt, quoted_twt_user_name, quoted_twt_user_screen_name, quoted_twt_user_location, quoted_twt_user_description,
    #                           quoted_twt_user_url, quoted_twt_user_tweet_link, quoted_twt_full_text)'''

    #cur.execute(Create)

    # var_string = ', '.join('?' * len(query))
    # print(var_string)
    query = tuple(query)
    # print(query)
    # print(len(query))
    #Insert1 = '''INSERT INTO "Hashtwts" VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'''.format(*query)
    #Insert1 = '''INSERT INTO "Hashtwts" VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'''.format(
    #    query[0], query[1], query[2], query[3], query[4], query[5], query[6], query[7], query[8], query[9], query[10],
    #    query[11], query[12], query[13], query[14], query[15], query[16], query[17], query[18], query[19], query[20],
    #    query[21], query[22], query[23], query[24], query[25], query[26], query[27], query[28], query[29], query[30],
    #    query[31], query[32], query[33], query[34], query[35], query[36], query[37], query[38], query[39], query[40])
    Insert1 = '''INSERT INTO "Hashtwts_1" ("Tweet_ID", "Username", "Display_Name", "User_ID", "User_Location", "Description", "User_URL", "Total_Followers", "Total_Following", "User_Since", "Favourites_Count", "UTC_Offset", "User_Time_Zone", "Location_Enabled", "User_Verified", "Total_Tweets", "User_Language", "Tweet_URL", "Tweet_Text", "Tweet_Date", "Hashtags_in_Tweet", "URLs_in_Tweet", "Mentions_in_Tweet", "Replying_to", "Language_Code", "Tweet_Language", "Location", "Coordinates", "Place_Type", "Place_Name", "Country", "Tweet_Coordinates", "Total_Retweets", "Total_Likes", "Quoted_Tweet", "Quoted_Tweet_From", "Quoted_Tweet_ID", "Quoted_User_Location", "Quoted_User_Bio", "Quoted_User_Bio_URL", "Quoted_Status", "Quoted_Tweet_Text") VALUES '''
    conflict = ''' ON CONFLICT("Tweet_ID") DO NOTHING'''
    Insert1 = Insert1+str(query)+str(conflict)
    #Insert1 = cur.mogrify(Insert1)
    # print(Insert1)

    #Query = Insert + ''' VALUES (%d, %s, %s, %d, %s, %s, %s, %d, %d, %s, %d, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %d, %d, %s, %s, %d, %s, %s, %s, %s, %s);'''

    #print(query)

    #print(Insert1)
    cur.execute(Create)
    cur.execute(Insert1)
    # print(cur.statusmessage)
    # if cur.rowcount == 1:
    #     print("Query {} executed".format(count))
    # Close communication with server
    conn.commit()
    cur.close()
    conn.close()

# variable_1 = "HELLO"
# variable_2 = "ADIOS"
# varlist = [variable_1,variable_2]
# var_string = ', '.join('?' * len(varlist))
# query_string = 'INSERT INTO table VALUES (%s);' % var_string
# cursor.execute(query_string, varlist)

# q = '''SELECT * FROM public."Hashtwts" where "Username" in ('illuminati_upis', 'komali1991', 'narnia4k', 'LokeshKongu1', 'mailtopuhal1983', 'SaketRam10', 'SaffronSurge3', 'senthilkavin217', 'Govinda63106467', 'KingsofKongu', 'Bala49788277', 'dharan6624', 'TiruppurSailor', 'it_kvg_official', 'Saravan61334989', 'Nachiket26', 'AlphaLifeCode', 'ramkris420', 'EuphoricEnigMeh', 'RagavendhraAna1', 'Maatram2', 'SaffronDalit', 'panni_official', 'BJP_Gayathri_Rm', 'GeethaR95243466', 'PoongodiSuganth', 'kavundarsangam', 'rsswarrior1', 'Pooja49649779', 'orangetalkies', '24amPulikeshi', 'FutureMaha4', 'kgoopinath', 'aususa7', 'SanthoshRaman6', 'ThalSainik007', 'Ellalan941', 'jkgche', 'imSathish523', 'ibctamilmedia', 'Ramaswamie', 'gounderism', 'NAMKALAIGNAR', 'Vidiyalstalin', 'ShivaPunishBJP', 'SURYahNaRaYaNaN', 'KPKPriya', 'FervidIndian', 'HimaniVanni', 'Karthikkovai', 'Dhanaba68200531', 'DKumara06619357', 'VisheshOff', 'bhaskarlivein', 'marunk13', 'VIGNESH66467731', 'Saimanrajs', 'KuttyNaai_', 'jagasu88', 'isitso15', 'Saimani07', 'alakshya2', 'Kavinakv2001', 'Malarva39104324', 'KalyanKesavan', 'Enian36693326', 'Soujugada', 'JEGANTHOMAS5', 'Manimin37926049', 'itsNilaMan', 'rana_dheeran', 'kavi_tweetz', 'nsm110621', 'ibelong2india', 'snat_i', 'Aswattaman')
# ORDER BY "Tweet_ID" DESC'''