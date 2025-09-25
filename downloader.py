"""Network operations manager for the book downloader application."""

import network
network.init()
import requests
import time
from io import BytesIO
from typing import Optional
from urllib.parse import urlparse
from tqdm import tqdm
from typing import Callable
from threading import Event
from logger import setup_logger
from config import PROXIES
from env import MAX_RETRY, DEFAULT_SLEEP, USE_CF_BYPASS, USING_EXTERNAL_BYPASSER
if USE_CF_BYPASS:
    if USING_EXTERNAL_BYPASSER:
        from cloudflare_bypasser_external import get_bypassed_page
    else:
        from cloudflare_bypasser import get_bypassed_page

logger = setup_logger(__name__)


def html_get_page(url: str, retry: int = MAX_RETRY, use_bypasser: bool = False) -> str:
    """Fetch HTML content from a URL with retry mechanism.
    
    Args:
        url: Target URL
        retry: Number of retry attempts
        skip_404: Whether to skip 404 errors
        
    Returns:
        str: HTML content if successful, None otherwise
    """
    response = None
    try:
        logger.debug(f"html_get_page: {url}, retry: {retry}, use_bypasser: {use_bypasser}")
        if use_bypasser and USE_CF_BYPASS:
            logger.info(f"GET Using Cloudflare Bypasser for: {url}")
            return get_bypassed_page(url)
        else:
            logger.info(f"GET: {url}")
            response = requests.get(url, proxies=PROXIES)
            response.raise_for_status()
            logger.debug(f"Success getting: {url}")
            time.sleep(1)
        return str(response.text)
        
    except Exception as e:
        if retry == 0:
            logger.error_trace(f"Failed to fetch page: {url}, error: {e}")
            return ""
        
        if use_bypasser and USE_CF_BYPASS:
            logger.warning(f"Exception while using cloudflare bypass for URL: {url}")
            logger.warning(f"Exception: {e}")
            logger.warning(f"Response: {response}")
        elif response is not None and response.status_code == 404:
            logger.warning(f"404 error for URL: {url}")
            return ""
        elif response is not None and response.status_code == 403:
            logger.warning(f"403 detected for URL: {url}. Should retry using cloudflare bypass.")
            return html_get_page(url, retry - 1, True)
            
        sleep_time = DEFAULT_SLEEP * (MAX_RETRY - retry + 1)
        logger.warning(
            f"Retrying GET {url} in {sleep_time} seconds due to error: {e}"
        )
        time.sleep(sleep_time)
        return html_get_page(url, retry - 1, use_bypasser)

def download_url(link: str, size: str = "", progress_callback: Optional[Callable[[float], None]] = None, cancel_flag: Optional[Event] = None) -> Optional[BytesIO]:
    """Download content from URL into a BytesIO buffer.
    
    Args:
        link: URL to download from
        size: Expected file size (for progress calculation)
        progress_callback: Function to call with progress updates
        cancel_flag: Event to check for cancellation
        
    Returns:
        BytesIO: Buffer containing downloaded content if successful
    """
    try:
        logger.info(f"Starting download from: {link}")
        
        # Add retry logic for rate limiting and timeouts
        max_retries = 3
        base_delay = 30  # Start with 30 second delay
        
        for attempt in range(max_retries):
            try:
                # Increased timeout for better reliability
                response = requests.get(link, stream=True, proxies=PROXIES, timeout=(30, 120))  # 30s connect, 120s read
                response.raise_for_status()
                break  # Success, exit retry loop
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    if attempt < max_retries - 1:  # Not the last attempt
                        wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Rate limited (429), waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        
                        # Check for cancellation during wait
                        if cancel_flag:
                            for _ in range(wait_time):
                                if cancel_flag.is_set():
                                    logger.info("Download cancelled during rate limit wait")
                                    return None
                                time.sleep(1)
                        else:
                            time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} attempts")
                        raise
                else:
                    # Other HTTP error, don't retry
                    raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    wait_time = 15 * (attempt + 1)  # Progressive delay: 15s, 30s, 45s
                    error_type = "timeout" if "timeout" in str(e).lower() else "connection error"
                    logger.warning(f"Download {error_type}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    
                    # Check for cancellation during wait
                    if cancel_flag:
                        for _ in range(wait_time):
                            if cancel_flag.is_set():
                                logger.info("Download cancelled during retry wait")
                                return None
                            time.sleep(1)
                    else:
                        time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Download failed after {max_retries} attempts: {e}")
                    raise

        # Calculate expected size
        total_size : float = 0.0
        try:
            # we assume size is in MB
            total_size = float(size.strip().replace(" ", "").replace(",", ".").upper()[:-2].strip()) * 1024 * 1024
            logger.info(f"Expected download size: {total_size/1024/1024:.2f} MB")
        except:
            total_size = float(response.headers.get('content-length', 0))
            if total_size > 0:
                logger.info(f"Download size from headers: {total_size/1024/1024:.2f} MB")
            else:
                logger.info("Download size unknown")
        
        buffer = BytesIO()
        downloaded = 0
        start_time = time.time()
        last_progress_time = start_time
        last_downloaded = 0
        last_logged_progress = -1

        # Disable tqdm to avoid interference with logs
        pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc='Downloading', 
                   disable=True)  # Disabled to prevent log interference
        
        try:
            # Wrap the streaming in timeout retry logic
            max_stream_retries = 2
            for stream_attempt in range(max_stream_retries):
                try:
                    for chunk in response.iter_content(chunk_size=16384):  # Larger chunks for better performance
                        if cancel_flag is not None and cancel_flag.is_set():
                            logger.info("Download cancelled")
                            pbar.close()
                            return None
                        
                        buffer.write(chunk)
                        downloaded += len(chunk)
                        pbar.update(len(chunk))
                        
                        # Calculate and report progress (call callback every chunk, log less frequently)
                        current_time = time.time()
                        if total_size > 0:
                            progress_percent = (downloaded / total_size) * 100.0
                            
                            # Always call progress callback for backend tracking
                            if progress_callback is not None:
                                progress_callback(progress_percent)
                            
                            # Log progress less frequently to avoid spam (every 5 seconds and at milestones)
                            current_progress_milestone = int(progress_percent // 5) * 5  # Round to 5% increments
                            if (current_time - last_progress_time >= 5.0) or (current_progress_milestone > last_logged_progress and current_progress_milestone % 10 == 0):
                                # Calculate current speed
                                time_diff = current_time - last_progress_time
                                bytes_diff = downloaded - last_downloaded
                                current_speed_mb = (bytes_diff / time_diff) / (1024 * 1024) if time_diff > 0 else 0
                                
                                if current_progress_milestone > last_logged_progress and current_progress_milestone % 10 == 0:
                                    logger.debug(f"Download progress: {current_progress_milestone}% ({downloaded/1024/1024:.1f}/{total_size/1024/1024:.1f} MB) - {current_speed_mb:.1f} MB/s")
                                    last_logged_progress = current_progress_milestone
                                
                                # Update tracking variables
                                last_progress_time = current_time
                                last_downloaded = downloaded
                    
                    # If we reach here, streaming completed successfully
                    break
                    
                except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
                    if stream_attempt < max_stream_retries - 1 and "timed out" in str(e).lower():
                        logger.warning(f"Stream interrupted at {downloaded/1024/1024:.1f}MB, retrying... (attempt {stream_attempt + 1}/{max_stream_retries})")
                        # Note: We can't resume the download easily, so we start over
                        # In a future update, we could implement Range header requests for resume
                        buffer = BytesIO()
                        downloaded = 0
                        
                        # Get a fresh response
                        response = requests.get(link, stream=True, proxies=PROXIES, timeout=(30, 120))
                        response.raise_for_status()
                        continue
                    else:
                        raise
        finally:
            pbar.close()
            
        elapsed_time = time.time() - start_time
        final_size_mb = downloaded / (1024 * 1024)
        avg_speed_mb = final_size_mb / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(f"Download completed: {final_size_mb:.2f} MB in {elapsed_time:.1f}s (avg {avg_speed_mb:.1f} MB/s)")
        
        # Final progress callback
        if progress_callback is not None and total_size > 0:
            progress_callback(100.0)
        
        # Validate download completion
        if total_size > 0 and downloaded < (total_size * 0.9):  # Allow 10% variance
            content_type = response.headers.get('content-type', '')
            if content_type.startswith('text/html'):
                logger.warning(f"Download may have failed - received HTML content instead of file")
                return None
            else:
                logger.warning(f"Download size mismatch: expected {total_size/1024/1024:.2f} MB, got {final_size_mb:.2f} MB")
        
        return buffer
        
    except requests.exceptions.RequestException as e:
        logger.error_trace(f"Failed to download from {link}: {e}")
        return None

def get_absolute_url(base_url: str, url: str) -> str:
    """Get absolute URL from relative URL and base URL.
    
    Args:
        base_url: Base URL
        url: Relative URL
    """
    if url.strip() == "":
        return ""
    if url.strip("#") == "":
        return ""
    if url.startswith("http"):
        return url
    parsed_url = urlparse(url)
    parsed_base = urlparse(base_url)
    if parsed_url.netloc == "" or parsed_url.scheme == "":
        parsed_url = parsed_url._replace(netloc=parsed_base.netloc, scheme=parsed_base.scheme)
    return parsed_url.geturl()
