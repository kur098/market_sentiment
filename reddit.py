import logging
import json
import boto3
import praw
from datetime import datetime
from botocore.exceptions import ClientError
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedditScraper:
    def __init__(self, secret_name='reddit_api_keys'):
        self.secret_name = secret_name
        self.reddit = None
        self.initialize_reddit_client()

    def get_secret(self):
        client = boto3.client('secretsmanager')
        try:
            response = client.get_secret_value(SecretId=self.secret_name)
            return json.loads(response['SecretString'])
        except ClientError as e:
            logger.error(f"Error retrieving secret: {e}")
            raise

    def initialize_reddit_client(self):
        secret = self.get_secret()
        self.reddit = praw.Reddit(
            client_id=secret['client_id'],
            client_secret=secret['client_secret'],
            user_agent=secret['user_agent']
        )

    def rotate_keys(self):
        logger.info("Rotating API keys...")
        self.initialize_reddit_client()

    def scrape_posts(self, subreddit_name, post_type='new', limit=10):
        subreddit = self.reddit.subreddit(subreddit_name)
        
        if post_type == 'new':
            posts = subreddit.new(limit=limit)
        elif post_type == 'hot':
            posts = subreddit.hot(limit=limit)
        elif post_type == 'top':
            posts = subreddit.top(limit=limit)
        else:
            raise ValueError("Invalid post_type. Choose 'new', 'hot', or 'top'.")

        scraped_posts = []
        for post in posts:
            try:
                post_data = {
                    'id': post.id,
                    'title': post.title,
                    'text': post.selftext,
                    'author': str(post.author),
                    'score': post.score,
                    'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                    'num_comments': post.num_comments,
                    'url': post.url
                }
                scraped_posts.append(post_data)
            except praw.exceptions.RedditAPIException as e:
                if e.error_type == "RATELIMIT":
                    logger.warning("Rate limit hit. Rotating keys...")
                    self.rotate_keys()
                    continue
                else:
                    logger.error(f"Error scraping post {post.id}: {e}")

        return scraped_posts

    def scrape_subreddit(self, subreddit_name, post_types=['new', 'hot', 'top'], limit=10):
        results = {}
        for post_type in post_types:
            results[post_type] = self.scrape_posts(subreddit_name, post_type, limit)
        return results