import requests
from datetime import datetime as dt, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

class TwitterScraper:
    def __init__(self, 
                 api_key: str,
                 ):

        self.api_key = api_key
        self.base_url = "https://twitter154.p.rapidapi.com/user/tweets"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "twitter154.p.rapidapi.com"
        }

    def _get_tweets(self, 
                    username: str,
                    start_date: str,
                    end_date: str,
                    continuation_token: str = None,
                    include_replies: bool = False,
                    ):
        
        url = self.base_url if continuation_token is None else f"{self.base_url}/continuation"
        
        querystring = {
            "username": username,
            "limit": "100",
            "include_replies": "true" if include_replies else "false"
        }
        
        if continuation_token:
            querystring["continuation_token"] = continuation_token
        
        response = requests.get(url, headers=self.headers, params=querystring)
        data = response.json()
        
        if 'results' not in data:
            print(f"Error fetching tweets for {username}: {data.get('message', 'Unknown error')}")
            return {'results': []}
        
        return data
        
    def scrape_tweets(self, 
                      username: str,
                      start_date: str,
                      end_date: str,
                      include_replies: bool = False,
                      ):
        
        start_date = dt.strptime(start_date, "%Y-%m-%d")
        end_date = dt.strptime(end_date, "%Y-%m-%d")
        
        all_tweets = []
        continuation_token = None

        while True:
            data = self._get_tweets(
                username=username,
                start_date=start_date,
                end_date=end_date,
                continuation_token=continuation_token,
                include_replies=include_replies,
            )
            
            for tweet in data['results']:
                tweet_date = dt.strptime(tweet['creation_date'], "%a %b %d %H:%M:%S %z %Y")
                if start_date <= tweet_date.replace(tzinfo=None) <= end_date:
                    all_tweets.append(tweet)
                elif tweet_date.replace(tzinfo=None) < start_date:
                    return all_tweets

            if 'continuation_token' in data:
                continuation_token = data['continuation_token']
            else:
                break

            # To avoid hitting rate limits we add this sleep
            time.sleep(1)  

        return all_tweets

    def get_latest_tweets(self, 
                          usernames: str,
                          last_check_time: str = None,
                          lookback_if_no_check: int = 7,
                          ):

        if last_check_time is None:
            last_check_time = dt.now() - timedelta(days=lookback_if_no_check)
        
        def fetch_user_tweets(username: str):
            data = self._get_tweets(username, None, None)
            new_tweets = []
            for tweet in data['results']:
                tweet_date = dt.strptime(tweet['creation_date'], "%a %b %d %H:%M:%S %z %Y")
                if tweet_date.replace(tzinfo=None) > last_check_time:
                    new_tweets.append(tweet)
                else:
                    break
            return (username, new_tweets)
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fetch_user_tweets, username) for username in usernames]
            results = {}
            for future in as_completed(futures):
                try:
                    username, tweets = future.result()
                    results[username] = tweets
                except Exception as e:
                    print(f"Error processing tweets: {str(e)}")
        
        return results
