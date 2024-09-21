import re

def parse_db_tweet(tweet):
    text = tweet[1] if len(tweet) > 1 else ""
    
    source = None
    coins = []
    
    if not text.startswith('http'):
        source_match = re.search(r'Source:\s*(\[?DB\]?)', text)
        coins_match = re.search(r'Coins:\s*([\w\s,]+)', text)
        
        if source_match:
            source = source_match.group(1).strip('[]')
        
        if coins_match:
            coins = [coin.strip() for coin in coins_match.group(1).split(',')]
    
    return {
        'date': tweet[0],
        'text': text,
        'source': source,
        'coins': coins
    }
