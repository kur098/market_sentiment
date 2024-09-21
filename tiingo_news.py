import requests
from typing import List, Optional

class TiingoData:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://api.tiingo.com/tiingo"
        self.news_ext = "/news"
        self.headers = {'Content-Type': 'application/json'}

    def get_bulk_download_list(self):
        url = f"{self.base_url}{self.news_ext}/bulk_download?token={self.api_token}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def download_batch_file(self, file_id: str):
        url = f"{self.base_url}{self.news_ext}/bulk_download/{file_id}?token={self.api_token}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.content

    def get_latest_news(self, 
                        tickers: Optional[List[str]] = None, 
                        tags: Optional[List[str]] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        limit: int = 100,
                        offset: int = 0,
                        sort_by: str = "publishedDate"
                        ):
        
        params = {
            'token': self.api_token,
            'limit': limit,
            'offset': offset,
            'sortBy': sort_by
        }
        if tickers:
            params['tickers'] = ','.join(tickers)
        if tags:
            params['tags'] = ','.join(tags)
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date

        response = requests.get(
            self.base_url + self.news_ext,
            headers=self.headers,
            params=params
        )
        response.raise_for_status()
        return response.json()
