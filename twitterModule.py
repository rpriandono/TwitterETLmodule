import json
import oauth2 as oauth
import time
from csv import DictWriter
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def ConvertTime(tweetTime):
    """ Conver tweeter time into epoch
    """
    return time.mktime(time.strptime(tweetTime, "%a %b %d %H:%M:%S +0000 %Y"))


def ExtractDelta(dict1, dict2):
    """ Exepct opertation: to identify new tweets
    """
    check = set([(d['userID'], d['messageDate']) for d in dict2])
    return [d for d in dict1 if (d['userID'], d['messageDate']) not in check]


def write2file(tweets):
    """ Load operation: write the dictionary into .tsv file format
    """
    keys = ['name', 'userID', 'userDate', 'screenName',
            'author', 'messageID', 'messageDate', 'messageText']
    with open("picnic-output.tsv", "w") as f:
        dict_writer = DictWriter(f, keys, delimiter="\t")
        dict_writer.writeheader()
        for tweet in tweets:
            dict_writer.writerow(tweet)


def GetTweet():
    """ access twitter API and perfrom Extract and Transform operation of bieber tweets. 
    """
    consumer_key = "your consumer key"
    consumer_secret = "your consumer secret key"

    token_key = "your token key"
    token_secret = "your token secret key"

    consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)
    access_token = oauth.Token(key=token_key, secret=token_secret)
    client = oauth.Client(consumer, access_token)

    query = "https://api.twitter.com/1.1/search/tweets.json?q=bieber&count=80"

    response, data = client.request(query)
    tweets = json.loads(data)
    dict = []

    for i in range(0, 99):
        try:
            messageID = tweets['statuses'][i]['id']
            messageText = tweets['statuses'][i]['text']
            messageDateTemp = tweets['statuses'][i]['created_at']
            messageDate = ConvertTime(messageDateTemp)
            author = tweets['statuses'][i]['user']['name']
            userID = tweets['statuses'][i]['user']['id']
            userDateTemp = tweets['statuses'][i]['user']['created_at']
            userDate = ConvertTime(userDateTemp)
            name = tweets['statuses'][i]['user']['name']
            screenName = tweets['statuses'][i]['user']['screen_name']
            temp = {"messageID": messageID, "messageText": messageText, "messageDate": messageDate,
                    "author": author, "userID": userID, "userDate": userDate, "name": name, "screenName": screenName}

            dict.append(temp.copy())
            i = i + 1

        except IndexError:
            return dict
            break


def main():
    print "Program running until, user interupt"
    outputBuffer = []

    try:
        while True:
            dictBuffer = []
            itteration = 0
            # ==== 30 seconds or 100 (distinct) messages which ever comefirst =
            while itteration <= 30 and len(dictBuffer) < 100:
                temp = GetTweet()
                print "temp size :", len(temp)

                delta = ExtractDelta(temp, dictBuffer)
                print "delta size :", len(delta)

                dictBuffer = dictBuffer + delta
                print "dictBuffer :", len(dictBuffer)

                time.sleep(1)
                itteration = itteration + 1
                print itteration

            agregate = ExtractDelta(dictBuffer, outputBuffer)
            outputBuffer = outputBuffer + agregate
            print "agregate :", len(agregate)
            print "outputBuffer :", len(outputBuffer)

    except KeyboardInterrupt:
        print "+++ Program Stop, user interupt +++++"
        # ===== Sort user and messages per user. =======
        userSort = sorted(outputBuffer, key=lambda x: (
            x['userDate'], x['messageDate']))
        write2file(userSort)
        print "==== Output sorted and writen OK ==="


if __name__ == "__main__":
    main()
