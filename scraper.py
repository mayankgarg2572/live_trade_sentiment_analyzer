"""
scraper.py

Advanced Twitter/X Scraper with Anti-Bot Measures

This script provides a robust solution for scraping tweets from Twitter/X using Playwright. 
It includes advanced anti-detection techniques, such as realistic browser fingerprinting, 
human-like interaction patterns, and session persistence. The scraper can extract tweets 
for specified hashtags, along with metadata like engagement metrics, mentions, hashtags, 
and URLs.

Features:
- Browser setup with anti-detection measures (e.g., stealth mode, randomized user agents).
- Manual or session-based login to Twitter.
- Hashtag-based tweet scraping with deduplication.
- Human-like interaction patterns (e.g., typing, scrolling, mouse movements).
- Data extraction, including tweet content, engagement metrics, and metadata.
- Saves data in CSV and JSON formats, with summary statistics.

Usage:
- Configure hashtags and the number of tweets per hashtag in the `main()` function.
- Run the script, log in manually if required, and let the scraper collect tweets.
- Outputs are saved in the `twitter_data/` directory.

Dependencies:
- Playwright (with Chromium browser)
- BeautifulSoup (for HTML parsing)
- Pandas (for data handling)

Entry Point:
- The `main()` function serves as the entry point for the script.

"""

import asyncio
import json
import random
import os
from datetime import datetime
from typing import Dict, List, Optional
import re
import hashlib

from playwright.async_api import async_playwright

from bs4 import BeautifulSoup
import pandas as pd


class AdvancedTwitterScraper:
    def __init__(self):
        self.browser = None
        self.page = None
        self.context = None
        self.tweets_data = {}
        self.config_file = 'twitter_session_config.json'
        self.cookies_file = 'twitter_cookies.pkl'
        
    def get_random_user_agent(self):
        """Get a random realistic user agent"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        ]
        return random.choice(user_agents)
        
    def get_random_viewport(self):
        """Get random realistic viewport dimensions"""
        viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1366, 'height': 768},
            {'width': 1536, 'height': 864},
            {'width': 1440, 'height': 900},
            {'width': 1280, 'height': 720},
            {'width': 2560, 'height': 1440}
        ]
        return random.choice(viewports)
        
    async def setup_browser(self):
        """Initialize browser with advanced anti-detection measures"""
        self.playwright = await async_playwright().start()
        
        # Advanced browser arguments for anti-detection
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-site-isolation-trials',
            '--disable-features=BlockInsecurePrivateNetworkRequests',
            '--disable-features=OutOfBlinkCors',
            '--window-position=0,0',
            '--window-size=1920,1080',
            '--start-maximized',
            '--disable-gpu',
            '--disable-setuid-sandbox',
            '--disable-accelerated-2d-canvas',
            '--disable-features=ChromeWhatsNewUI',
            '--disable-features=AudioServiceOutOfProcess',
            '--force-color-profile=srgb',
            '--metrics-recording-only',
            '--password-store=basic',
            '--use-mock-keychain',
            '--export-tagged-pdf',
            '--no-default-browser-check',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-ipc-flooding-protection',
            '--enable-features=NetworkService,NetworkServiceInProcess',
            '--force-color-profile=srgb',
            '--hide-scrollbars',
            '--mute-audio',
            '--disable-features=UserAgentClientHint',
            '--disable-features=WebRTC',
            '--disable-features=OptimizationGuideModelDownloading,OptimizationHintsFetching,OptimizationTargetPrediction,OptimizationHints'
        ]
        
        # Launch browser
        self.browser = await self.playwright.chromium.launch(
            headless=False,
            args=browser_args,
            ignore_default_args=['--enable-automation'],
            chromium_sandbox=False
        )
        
        # Get random configuration
        user_agent = self.get_random_user_agent()
        viewport = self.get_random_viewport()
        
        # Create context with advanced settings
        self.context = await self.browser.new_context(
            viewport=viewport,
            user_agent=user_agent,
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation', 'notifications'],
            geolocation={'latitude': 40.7128, 'longitude': -74.0060},
            color_scheme='light',
            reduced_motion='no-preference',
            forced_colors='none',
            accept_downloads=False,
            has_touch=False,
            is_mobile=False,
            device_scale_factor=1.0,
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
                'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Pragma': 'no-cache',
                'Priority': 'u=0, i'
            }
        )
        
        self.page = await self.context.new_page()
        
        # Apply stealth mode if available
        try:
            from playwright_stealth import stealth_async
            await stealth_async(self.page)
        except:
            pass  # Stealth mode optional
        
        # Advanced JavaScript injection for anti-detection
        await self.page.add_init_script("""
            // Override webdriver detection
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override chrome detection
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        name: 'Chrome PDF Plugin',
                        description: 'Portable Document Format',
                        filename: 'internal-pdf-viewer',
                        length: 1
                    },
                    {
                        name: 'Chrome PDF Viewer',
                        description: 'Portable Document Format',
                        filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
                        length: 1
                    },
                    {
                        name: 'Native Client',
                        description: '',
                        filename: 'internal-nacl-plugin',
                        length: 2
                    }
                ]
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Override platform
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32'
            });
            
            // Override memory
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
            
            // Override hardware concurrency
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 4
            });
            
            // Override WebGL
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) {
                    return 'Intel Inc.';
                }
                if (parameter === 37446) {
                    return 'Intel Iris OpenGL Engine';
                }
                return getParameter.apply(this, arguments);
            };
            
            // Override canvas fingerprinting
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type) {
                if (type === 'image/png' && this.width === 280 && this.height === 60) {
                    return 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
                }
                return originalToDataURL.apply(this, arguments);
            };
            
            // Override timezone
            Date.prototype.getTimezoneOffset = function() { return -300; };
            
            // Remove automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """)
        
        # Set additional CDP properties
        try:
            client = await self.page.context.new_cdp_session(self.page)
            await client.send('Network.setUserAgentOverride', {
                'userAgent': user_agent,
                'acceptLanguage': 'en-US,en;q=0.9',
                'platform': 'Win32'
            })
        except:
            pass
            
    async def save_cookies(self):
        """Save cookies to file"""
        try:
            cookies = await self.context.cookies()
            storage_state = await self.context.storage_state()
            user_agent = await self.page.evaluate("() => navigator.userAgent")  # str
            config = {
                'cookies': cookies,
                'storage_state': storage_state,
                'user_agent': user_agent,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
                
            print("✓ Session saved successfully")
            return True
        except Exception as e:
            print(f"\n Error saving session: {str(e)}")
            return False
            
    async def load_cookies(self):
        """Load cookies from file"""
        try:
            if not os.path.exists(self.config_file):
                return False
                
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                
            # Check if session is recent (less than 7 days old)
            session_time = datetime.fromisoformat(config['timestamp'])
            if (datetime.now() - session_time).days > 7:
                print("\n Session too old, manual login required")
                return False
                
            # Load cookies
            if 'cookies' in config:
                await self.context.add_cookies(config['cookies'])
                
            print("✓ Previous session loaded")
            return True
            
        except Exception as e:
            print(f"\n Error loading session: {str(e)}")
            return False
            
    async def human_like_typing(self, selector: str, text: str):
        """Type text with human-like delays"""
        element = self.page.locator(selector)
        await element.click()
        
        for char in text:
            await element.type(char, delay=random.randint(50, 200))
            
            # Occasional pauses
            if random.random() < 0.1:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
    async def human_like_delay(self, min_seconds: float = 1, max_seconds: float = 3):
        """Add random human-like delays with micro-movements"""
        delay = random.uniform(min_seconds, max_seconds)
        
        # Add micro-delays
        steps = random.randint(3, 7)
        for _ in range(steps):
            await asyncio.sleep(delay / steps)
            
            # Random micro mouse movements
            if random.random() < 0.3:
                await self.page.mouse.move(
                    random.randint(100, 300),
                    random.randint(100, 300)
                )
                
    async def random_mouse_movement(self):
        """Simulate realistic mouse movements with curves"""
        for _ in range(random.randint(2, 4)):
            # Generate bezier curve points
            start_x = random.randint(100, 500)
            start_y = random.randint(100, 400)
            end_x = random.randint(500, 900)
            end_y = random.randint(200, 600)
            
            # Move in steps to simulate curve
            steps = random.randint(5, 10)
            for i in range(steps):
                t = i / steps
                x = int(start_x + (end_x - start_x) * t)
                y = int(start_y + (end_y - start_y) * t)
                
                await self.page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.01, 0.05))
                
    async def random_scrolling(self):
        """Perform human-like scrolling"""
        scroll_amount = random.randint(100, 500)
        
        # Smooth scroll with acceleration
        await self.page.evaluate(f"""
            (function() {{
                let scrolled = 0;
                const target = {scroll_amount};
                const duration = {random.randint(500, 1500)};
                const start = Date.now();
                
                function easeInOutQuad(t) {{
                    return t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
                }}
                
                function scroll() {{
                    const elapsed = Date.now() - start;
                    const progress = Math.min(elapsed / duration, 1);
                    const eased = easeInOutQuad(progress);
                    
                    window.scrollBy(0, (target - scrolled) * eased * 0.1);
                    scrolled += (target - scrolled) * eased * 0.1;
                    
                    if (progress < 1) {{
                        requestAnimationFrame(scroll);
                    }}
                }}
                
                scroll();
            }})();
        """)
        
    async def login_to_twitter(self):
        """Navigate to Twitter and handle login with session persistence"""
        print("\n" + "="*60)
        print("LOGIN PROCESS")
        print("="*60)
        
        # Try to load existing session
        session_loaded = await self.load_cookies()
        
        await self.page.goto('https://twitter.com/home', wait_until='networkidle')
        await self.human_like_delay(15, 20)
        
        # Check if already logged in
        current_url = self.page.url
        
        if 'home' in current_url and '/login' not in current_url:
            print("✓ Already logged in using saved session!")
            return True
            
        # Manual login required
        if not session_loaded:
            print("\n Manual login required")
            
        await self.page.goto('https://twitter.com/login', wait_until='networkidle')
        
        print("\nPlease log in manually in the browser window.")
        print("After successful login, press Enter to continue...")
        input()
        
        # Verify login and save session
        await self.human_like_delay(2, 4)
        current_url = self.page.url
        
        if 'home' in current_url or 'twitter.com' in current_url:
            print("✓ Login successful!")
            
            # Save session for future use
            await self.save_cookies()
            
            return True
        else:
            print("✗ Login may have failed. Current URL:", current_url)
            print("Press Enter to continue anyway or Ctrl+C to exit...")
            input()
            return True
            
    async def wait_for_human_intervention(self, message: str):
        """Pause for human intervention"""
        print("\n" + "!"*60)
        print(f"HUMAN INTERVENTION REQUIRED: {message}")
        print("!"*60)
        print("Resolve the issue in the browser, then press Enter to continue...")
        input()
        await self.human_like_delay(1, 2)
        
    async def detect_and_handle_challenges(self):
        """Detect and handle various anti-bot challenges"""
        # Check for captcha
        captcha_selectors = [
            'iframe[title*="recaptcha"]',
            'div[class*="captcha"]',
            'div[id*="captcha"]',
            '[aria-label*="captcha"]',
            'text=/verify you are human/i'
        ]
        
        for selector in captcha_selectors:
            if await self.page.locator(selector).count() > 0:
                await self.wait_for_human_intervention("CAPTCHA detected")
                return
                
        # Check for rate limiting
        rate_limit_indicators = [
            'text=/rate limit/i',
            'text=/too many requests/i',
            'text=/please wait/i',
            'text=/try again later/i'
        ]
        
        for indicator in rate_limit_indicators:
            if await self.page.locator(indicator).count() > 0:
                wait_time = random.uniform(30, 60)
                print(f"\n Rate limit detected. Waiting {wait_time:.0f} seconds...")
                await asyncio.sleep(wait_time)
                return
                
    async def search_hashtag(self, hashtag: str):
        """Search for a specific hashtag with anti-detection measures"""
        hashtag = hashtag.strip('#')
        
        # Random delay before search
        await self.human_like_delay(2, 5)
        
        # Sometimes use search box, sometimes direct URL
        if random.random() < 0.7:
            # Direct URL navigation
            search_url = f"https://twitter.com/search?q=%23{hashtag}&src=typed_query&f=live"
            print(f"\nNavigating to #{hashtag}...")
            
            await self.page.goto(search_url, wait_until='networkidle', timeout=30000)
        else:
            # Use search box
            print(f"\nSearching for #{hashtag} using search box...")
            
            search_box = self.page.locator('[data-testid="SearchBox_Search_Input"]').first
            if await search_box.count() > 0:
                await search_box.click()
                await self.human_like_delay(0.5, 1)
                await search_box.clear()
                await self.human_like_typing('[data-testid="SearchBox_Search_Input"]', f"#{hashtag}")
                await self.page.keyboard.press('Enter')
                
        await self.human_like_delay(3, 6)
        
        # Check for challenges
        await self.detect_and_handle_challenges()
        
        # Random interactions
        await self.random_mouse_movement()
        
        # Sometimes click on "Latest" tab
        if random.random() < 0.5:
            latest_tab = self.page.locator('a[href*="f=live"]').first
            if await latest_tab.count() > 0:
                await latest_tab.click()
                await self.human_like_delay(2, 4)
                
        return True
        
    async def extract_tweet_data(self, tweet_element) -> Optional[Dict]:
        """Extract data from a single tweet element"""
        try:
            tweet_html = await tweet_element.inner_html()
            soup = BeautifulSoup(tweet_html, 'html.parser')
            
            # Extract username
            username = None
            user_link = soup.find('a', href=re.compile(r'^/[^/]+$'))
            if user_link:
                username = user_link.get('href', '').strip('/')
                
            # Extract timestamp
            timestamp = None
            time_element = soup.find('time')
            if time_element:
                timestamp = time_element.get('datetime', '')
                
            # Extract tweet content
            content = None
            tweet_text = soup.find('div', {'data-testid': 'tweetText'})
            if tweet_text:
                content = tweet_text.get_text(strip=True)
                
            # Extract engagement metrics
            metrics = {}
            
            # Reply count
            reply_button = soup.find('div', {'data-testid': 'reply'})
            if reply_button:
                reply_text = reply_button.get_text(strip=True)
                metrics['replies'] = self.parse_metric(reply_text)
                
            # Retweet count
            retweet_button = soup.find('div', {'data-testid': 'retweet'})
            if retweet_button:
                retweet_text = retweet_button.get_text(strip=True)
                metrics['retweets'] = self.parse_metric(retweet_text)
                
            # Like count
            like_button = soup.find('div', {'data-testid': 'like'})
            if like_button:
                like_text = like_button.get_text(strip=True)
                metrics['likes'] = self.parse_metric(like_text)
                
            # Extract mentions
            mentions = []
            mention_links = soup.find_all('a', href=re.compile(r'^/[^/]+$'))
            for link in mention_links:
                text = link.get_text(strip=True)
                if text.startswith('@'):
                    mentions.append(text)
                    
            # Extract hashtags
            hashtags = []
            hashtag_links = soup.find_all('a', href=re.compile(r'/hashtag/'))
            for link in hashtag_links:
                hashtags.append(link.get_text(strip=True))
                
            # Extract URLs
            urls = []
            url_links = soup.find_all('a', href=re.compile(r'^https?://'))
            for link in url_links:
                url = link.get('href')
                if url and 'twitter.com' not in url:
                    urls.append(url)
                    
            if username and content:
                return {
                    'username': username,
                    'timestamp': timestamp,
                    'content': content,
                    'replies': metrics.get('replies', 0),
                    'retweets': metrics.get('retweets', 0),
                    'likes': metrics.get('likes', 0),
                    'mentions': ', '.join(mentions),
                    'hashtags': ', '.join(hashtags),
                    'urls': ', '.join(urls),
                    'scraped_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"\n Error extracting tweet data: {str(e)}")
            
        return None
        
    def parse_metric(self, text: str) -> int:
        """Parse engagement metric from text"""
        if not text:
            return 0
            
        text = text.upper().strip()
        
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        
        for suffix, multiplier in multipliers.items():
            if suffix in text:
                try:
                    number = float(text.replace(suffix, '').replace(',', ''))
                    return int(number * multiplier)
                except:
                    pass
                    
        # Try to extract raw number
        try:
            return int(''.join(filter(str.isdigit, text)))
        except:
            return 0
            
    async def scrape_tweets_for_hashtag(self, hashtag: str, max_tweets: int = 500):
        """Scrape tweets with advanced anti-detection"""
        print(f"\n🔍 Starting to scrape #{hashtag}...")
        
        if not await self.search_hashtag(hashtag):
            return []
            
        tweets = []
        tweet_ids = set()  # Track unique tweets
        last_height = 0
        no_new_tweets_count = 0
        max_scroll_attempts = 60
        
        # Random starting behavior
        await self.random_scrolling()
        # await self.human_like_delay(2, 4)
        
        for scroll_attempt in range(max_scroll_attempts):
            # Check for interventions periodically
            if scroll_attempt % 10 == 0:
                await self.detect_and_handle_challenges()
                
            # Get all tweet elements
            tweet_elements = await self.page.locator('article[data-testid="tweet"]').all()
            
            print(f"Scroll {scroll_attempt + 1}: Found {len(tweet_elements)} elements (Collected: {len(tweets)}/{max_tweets})")
            
            # Process tweets in random order sometimes
            if random.random() < 0.3:
                random.shuffle(tweet_elements)
                
            for element in tweet_elements:
                if len(tweets) >= max_tweets:
                    break
                    
                tweet_data = await self.extract_tweet_data(element)
                
                if tweet_data:
                    # Create unique ID for deduplication
                    tweet_id = hashlib.md5(
                        f"{tweet_data['username']}{tweet_data['content']}".encode()
                    ).hexdigest()
                    
                    if tweet_id not in tweet_ids:
                        tweet_ids.add(tweet_id)
                        tweets.append(tweet_data)
                        print(f"✓ Tweet {len(tweets)}: @{tweet_data['username'][:20]}...")
                        
                        # Random micro-delay between extractions
                        await asyncio.sleep(random.uniform(0.01, 0.1))
                        
            if len(tweets) >= max_tweets:
                print(f"\nReached target of {max_tweets} tweets")
                break
                
            # Human-like scrolling patterns
            scroll_patterns = [
                lambda: self.random_scrolling(),
                lambda: self.page.evaluate("window.scrollBy(0, window.innerHeight * 0.7)"),
                lambda: self.page.evaluate("window.scrollBy(0, 500)"),
                lambda: self.page.mouse.wheel(0, random.randint(300, 700))
            ]
            
            await random.choice(scroll_patterns)()
            
            # Variable delays based on scroll count
            if scroll_attempt < 10:
                await self.human_like_delay(2, 4)
            elif scroll_attempt < 30:
                await self.human_like_delay(3, 6)
            else:
                await self.human_like_delay(4, 8)
                
            # Check if new content loaded
            new_height = await self.page.evaluate("document.body.scrollHeight")
            
            if new_height == last_height:
                no_new_tweets_count += 1
                
                if no_new_tweets_count >= 3:
                    # Try refreshing or scrolling up then down
                    await self.page.evaluate("window.scrollTo(0, 0)")
                    await self.human_like_delay(2, 3)
                    await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await self.human_like_delay(3, 5)
                    
                if no_new_tweets_count >= 5:
                    print("\n No new tweets loading. Stopping...")
                    break
            else:
                no_new_tweets_count = 0
                
            last_height = new_height
            
            # Random interactions
            if scroll_attempt % 7 == 0:
                await self.random_mouse_movement()
                
            # Occasional pause to seem more human
            if random.random() < 0.1:
                pause_time = random.uniform(5, 15)
                print(f"Taking a {pause_time:.1f} second break...")
                await asyncio.sleep(pause_time)
                
        print(f"\nCollected {len(tweets)} unique tweets for #{hashtag}")
        return tweets
        
    async def save_data(self, hashtag: str, tweets: List[Dict]):
        """Save tweets to multiple formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create data directory
        os.makedirs('twitter_data', exist_ok=True)
        
        # Save as CSV
        if tweets:
            df = pd.DataFrame(tweets)
            csv_filename = f"twitter_data/tweets_{hashtag}_{timestamp}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"\nSaved CSV: {csv_filename}")
            
            # Save summary statistics
            stats = {
                'hashtag': hashtag,
                'total_tweets': len(tweets),
                'unique_users': df['username'].nunique(),
                'avg_likes': df['likes'].mean(),
                'avg_retweets': df['retweets'].mean(),
                'avg_replies': df['replies'].mean(),
                'date_range': {
                    'start': df['timestamp'].min(),
                    'end': df['timestamp'].max()
                }
            }
            
            stats_filename = f"ScrapStatsData/stats_{hashtag}_{timestamp}.json"
            with open(stats_filename, 'w') as f:
                json.dump(stats, f, indent=2)
            print(f"\nSaved statistics: {stats_filename}")
            
    async def run(self, hashtags: List[str], tweets_per_hashtag: int = 500):
        while True:
            try:
                print("\nStarting Advanced Twitter Research Scraper")
                print("="*60)
                print("Anti-bot measures enabled:")
                print("✓ Realistic browser fingerprinting")
                print("✓ Human-like interaction patterns")
                print("✓ Session persistence")
                print("✓ Advanced evasion techniques")
                print("="*60)
                
                await self.setup_browser()
                await self.login_to_twitter()
                await self.save_cookies()
                break
            except KeyboardInterrupt:
                print("\n Scraping interrupted by user")
                if self.browser:
                    print("\nClosing browser...")
                    await self.browser.close()
                if self.playwright:
                    print("\nClosing Playwright...")
                    await self.playwright.stop()
                return
            except Exception as e:
                print(f"\nError: {str(e)}")
                await self.wait_for_human_intervention(f"Unexpected error: {str(e)}")
        
        try:
            for i, hashtag in enumerate(hashtags):
                hashtag = hashtag.strip('#')
                
                print(f"\n{'='*60}")
                print(f"Processing #{hashtag} ({i+1}/{len(hashtags)})")
                print(f"{'='*60}")
                
                # Random longer delay at the start of each hashtag
                if i > 0:
                    delay = random.uniform(15, 30)
                    print(f"\nWaiting {delay:.1f} seconds before next hashtag...")
                    await asyncio.sleep(delay)
                    
                    # Sometimes navigate to home between searches
                    if random.random() < 0.3:
                        print("\nReturning to home feed briefly...")
                        await self.page.goto('https://twitter.com/home')
                        await self.human_like_delay(5, 10)
                        
                tweets = await self.scrape_tweets_for_hashtag(hashtag, tweets_per_hashtag)
                await self.save_data(hashtag, tweets)
            
            
            print("\nScraping completed successfully!")
            
        except KeyboardInterrupt:
            print("\nScraping interrupted by user")
            await self.save_cookies()
        except Exception as e:
            print(f"\nError: {str(e)}")
            await self.wait_for_human_intervention(f"Unexpected error: {str(e)}")
            
        finally:
            if self.browser:
                print("\nClosing browser...")
                await self.browser.close()
            if self.playwright:
                print("\nClosing Playwright...")
                await self.playwright.stop()


async def main():
    # Configuration
    HASHTAGS = ['nifty50', 'sensex', 'intraday', 'banknifty']
    TWEETS_PER_HASHTAG = 50
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║           Advanced Twitter/X  Scrapper Tool              ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    print(f"\nConfiguration:")
    print(f"   Hashtags: {', '.join(['#' + h for h in HASHTAGS])}")
    print(f"   Tweets per hashtag: {TWEETS_PER_HASHTAG}")
    
    response = input("\nProceed with scrapping(you need to manually login)? Type either yes or no: ").lower()
    
    if response == 'yes':
        scraper = AdvancedTwitterScraper()
        await scraper.run(HASHTAGS, TWEETS_PER_HASHTAG)
    else:
        print("Exiting...")


if __name__ == "__main__":
    asyncio.run(main())