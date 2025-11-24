import os

def get_crawler_settings():
    return {
        'headless': os.getenv('CRAWLER_HEADLESS', 'False').lower() == 'true',  # mặc định là False
        'save_checkpoint': os.getenv('CRAWLER_SAVE_CHECKPOINT', 'True').lower() == 'true',
        'user_agent': os.getenv(
            'CRAWLER_USER_AGENT',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        ),
    }