import sys
import time
import random
import tweepy
import collections

# spark-submit task3.py <port #> <output_file_path>
# spark-submit task3.py 9999 task3.csv


# start_time = time.time()


def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(MyStreamListener, self).__init__()
        self.n = 0
        self.hs100list = []
        self.s = 100

    def on_status(self, status):
        # print(status.place)
        hashtags = status.entities['hashtags']
        if len(hashtags) > 0:
            # print("\n")
            # print(status.text)
            # for hashtag in hashtags:
            #     print(hashtag['text'])
            #     print('-------')
            hs = [x['text'] for x in hashtags]
            hs = [x.replace('\n', ' ') for x in hs]
            hs = [x for x in hs if isEnglish(x)]
            if len(hashtags) > 0:
                self.n += 1
                if self.n <= 100:
                    self.hs100list.append(hs)
                else:
                    rd = random.randint(0, (self.n-1))
                    if rd < self.s:
                        # print(rd)
                        # print(len(self.hs100list))
                        self.hs100list.pop(rd)
                        # print(len(self.hs100list))
                        self.hs100list.append(hs)
                        # print(len(self.hs100list))

                flatlist = [item for sublist in self.hs100list for item in sublist]
                ocdict = dict(collections.Counter(flatlist))
                ocsorted = sorted(list(set(ocdict.values())), reverse=True)
                top3 = ocsorted[:min(3, len(ocsorted))]
                selected = []
                for tag, oc in ocdict.items():
                    if oc in top3:
                        selected.append((tag, oc))
                selected = sorted(selected, key=lambda x: x[0])
                selected = sorted(selected, key=lambda x: x[1], reverse=True)

                with open(output_file_path, 'a') as f:
                    f.write("The number of tweets with tags from the beginning: " + str(self.n) + "\n")
                    for slct in selected:
                        f.write(slct[0] + " : " + str(slct[1]) + "\n")
                    f.write("\n")

    def on_error(self, status_code):
        return False


# if __name__ == "__main__":
portnumber = int(sys.argv[1])
output_file_path = sys.argv[2]

consumer_key = ''
consumer_secret = ''

access_token = ''
access_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.api.wait_on_rate_limit = True

with open(output_file_path, "w") as f:
    f.write("")
myStream.filter(track=".", languages=['en'])

# print('Duration: ' + str(time.time() - start_time))
