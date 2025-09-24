"""Book download manager handling search and retrieval operations."""

import time, json, re
from pathlib import Path
from urllib.parse import quote
from typing import List, Optional, Dict, Union, Callable
from threading import Event
from bs4 import BeautifulSoup, Tag, NavigableString, ResultSet

import downloader
from logger import setup_logger
from config import SUPPORTED_FORMATS, BOOK_LANGUAGE, AA_BASE_URL
from env import AA_DONATOR_KEY, USE_CF_BYPASS, PRIORITIZE_WELIB
from models import BookInfo, SearchFilters
logger = setup_logger(__name__)



def search_books(query: str, filters: SearchFilters) -> List[BookInfo]:
    """Search for books matching the query.

    Args:
        query: Search term (ISBN, title, author, etc.)

    Returns:
        List[BookInfo]: List of matching books

    Raises:
        Exception: If no books found or parsing fails
    """
    query_html = quote(query)

    if filters.isbn:
        # ISBNs are included in query string
        isbns = " || ".join(
            [f"('isbn13:{isbn}' || 'isbn10:{isbn}')" for isbn in filters.isbn]
        )
        query_html = quote(f"({isbns}) {query}")

    filters_query = ""

    for value in filters.lang or BOOK_LANGUAGE:
        if value != "all":
            filters_query += f"&lang={quote(value)}"

    if filters.sort:
        filters_query += f"&sort={quote(filters.sort)}"

    if filters.content:
        for value in filters.content:
            filters_query += f"&content={quote(value)}"

    # Handle format filter
    formats_to_use = filters.format if filters.format else SUPPORTED_FORMATS

    index = 1
    for filter_type, filter_values in vars(filters).items():
        if filter_type == "author" or filter_type == "title" and filter_values:
            for value in filter_values:
                filters_query += (
                    f"&termtype_{index}={filter_type}&termval_{index}={quote(value)}"
                )
                index += 1

    url = (
        f"{AA_BASE_URL}"
        f"/search?index=&page=1&display=table"
        f"&acc=aa_download&acc=external_download"
        f"&ext={'&ext='.join(formats_to_use)}"
        f"&q={query_html}"
        f"{filters_query}"
    )

    html = downloader.html_get_page(url)
    if not html:
        raise Exception("Failed to fetch search results")

    if "No files found." in html:
        logger.info(f"No books found for query: {query}")
        raise Exception("No books found. Please try another query.")

    soup = BeautifulSoup(html, "html.parser")
    tbody: Tag | NavigableString | None = soup.find("table")

    if not tbody:
        logger.warning(f"No results table found for query: {query}")
        raise Exception("No books found. Please try another query.")

    books = []
    if isinstance(tbody, Tag):
        for line_tr in tbody.find_all("tr"):
            try:
                book = _parse_search_result_row(line_tr)
                if book:
                    books.append(book)
            except Exception as e:
                logger.error_trace(f"Failed to parse search result row: {e}")

    books.sort(
        key=lambda x: (
            SUPPORTED_FORMATS.index(x.format)
            if x.format in SUPPORTED_FORMATS
            else len(SUPPORTED_FORMATS)
        )
    )

    return books


def _parse_search_result_row(row: Tag) -> Optional[BookInfo]:
    """Parse a single search result row into a BookInfo object."""
    try:
        cells = row.find_all("td")
        preview_img = cells[0].find("img")
        preview = preview_img["src"] if preview_img else None

        return BookInfo(
            id=row.find_all("a")[0]["href"].split("/")[-1],
            preview=preview,
            title=cells[1].find("span").next,
            author=cells[2].find("span").next,
            publisher=cells[3].find("span").next,
            year=cells[4].find("span").next,
            language=cells[7].find("span").next,
            format=cells[9].find("span").next.lower(),
            size=cells[10].find("span").next,
        )
    except Exception as e:
        logger.error_trace(f"Error parsing search result row: {e}")
        return None


def get_book_info(book_id: str) -> BookInfo:
    """Get detailed information for a specific book.

    Args:
        book_id: Book identifier (MD5 hash)

    Returns:
        BookInfo: Detailed book information
    """
    url = f"{AA_BASE_URL}/md5/{book_id}"
    html = downloader.html_get_page(url)

    if not html:
        raise Exception(f"Failed to fetch book info for ID: {book_id}")

    soup = BeautifulSoup(html, "html.parser")

    return _parse_book_info_page(soup, book_id)


def _is_likely_title(text: str) -> bool:
    """Determine if text is likely a book title vs author name."""
    if not text or len(text.strip()) < 3:
        return False
    
    text = text.strip()
    
    # Common title indicators
    title_indicators = [
        ':', ';', '?', '!', '—', '–', '-',  # punctuation
        'the ', 'a ', 'an ',  # articles
        'how to', 'guide to', 'introduction to',  # instructional
        'volume', 'vol.', 'part', 'book', 'chapter'  # book parts
    ]
    
    # Author name indicators (suggesting this is NOT a title)
    author_indicators = [
        'by ', 'author:', 'written by',  # explicit author markers
    ]
    
    text_lower = text.lower()
    
    # If it contains author indicators, it's not a title
    if any(indicator in text_lower for indicator in author_indicators):
        return False
    
    # Check word count - titles are typically longer than author names
    words = text.split()
    word_count = len(words)
    
    # Very short (1-2 words) might be author name unless it has title indicators
    if word_count <= 2:
        return any(indicator in text_lower for indicator in title_indicators)
    
    # 3+ words with title indicators are likely titles
    if word_count >= 3 and any(indicator in text_lower for indicator in title_indicators):
        return True
    
    # If it's all capitalized, might be a title
    if text.isupper() and word_count > 1:
        return True
    
    # Longer texts (4+ words) are more likely to be titles
    return word_count >= 4


def _is_likely_author(text: str) -> bool:
    """Determine if text is likely an author name."""
    if not text or len(text.strip()) < 2:
        return False
    
    text = text.strip()
    words = text.split()
    
    # Typical author name patterns
    if len(words) == 2:  # "First Last"
        return all(word.isalpha() and word[0].isupper() for word in words)
    elif len(words) == 3:  # "First Middle Last" or "First M. Last"
        return all(word.isalpha() or (len(word) == 2 and word.endswith('.')) 
                  for word in words)
    elif len(words) == 1:  # Single name (less common)
        return text.isalpha() and text[0].isupper()
    
    # Too many words suggests it's not an author name
    return len(words) <= 4


def _parse_book_info_page(soup: BeautifulSoup, book_id: str) -> BookInfo:
    """Parse the book info page HTML into a BookInfo object."""
    logger.info(f"=== PARSING BOOK INFO FOR {book_id} ===")
    
    # Get page text for pattern matching
    page_text = soup.get_text()
    
    # Extract preview image
    preview = ""
    data = soup.select_one("body > main > div:nth-of-type(1)")
    if data:
        node = data.select_one("div:nth-of-type(1) > img")
        if node:
            preview_value = node.get("src", "")
            if isinstance(preview_value, list):
                preview = preview_value[0]
            else:
                preview = preview_value

    # Get divs for fallback extraction
    data = soup.find_all("div", {"class": "main-inner"})[0].find_next("div")
    divs = list(data.children) if data else []
    
    # Initialize with defaults
    title = "Unknown Title"
    author = "Unknown Author"  
    publisher = "Unknown Publisher"
    format = "epub"
    size = ""
    
    # Get download links for URL-based extraction
    download_links = soup.find_all("a", href=True)
    
    # Strategy 1: Extract title from download URLs first (most reliable)
    for link in download_links:
        href = link.get('href', '')
        # Look for title patterns in URLs
        if '.epub' in href.lower() and '%20' in href:
            # Try to extract title from URL
            url_patterns = [
                r'/([^/]*Thursday[^/]*Murder[^/]*Club[^/]*)%20--%20',  # Thursday Murder Club books
                r'/([^/]*Bullet[^/]*Missed[^/]*)%20--%20',            # Bullet That Missed
                r'/([^/]*Devil[^/]*Die[^/]*)%20--%20',                # Last Devil to Die
                r'/([^/]*%20[^/]*%20[^/]*%20[^/]*)%20--%20',         # Any title with multiple words
            ]
            
            for pattern in url_patterns:
                match = re.search(pattern, href)
                if match:
                    url_title = match.group(1).replace('%20', ' ').replace('%3A', ':').replace('%28', '(').replace('%29', ')')
                    # Clean up common artifacts
                    url_title = re.sub(r'\s+', ' ', url_title).strip()
                    if len(url_title) > 10 and _is_likely_title(url_title):
                        title = url_title
                        logger.info(f"Found title from download URL: '{title}'")
                        break
            if title != "Unknown Title":
                break
    
    # Strategy 2: Extract from the source title pattern in metadata
    if title == "Unknown Title":
        source_title_match = re.search(r'source title:\s*([^:]+?)(?:\s*date open sourced|\n|\r|$)', page_text, re.IGNORECASE)
        if source_title_match:
            raw_title = source_title_match.group(1).strip()
            title = raw_title.title() if not raw_title.isupper() else raw_title
            logger.info(f"Found title from source title pattern: '{title}'")
    
    # Strategy 3: Look for author name patterns 
    # Check both in URL and text
    if 'richard osman' in page_text.lower():
        author_patterns = [
            r'--\s*(richard\s+osman)\s*--',  # From URLs
            r'by\s+(richard\s+osman)',       # From descriptions
            r'author[:\s]+(richard\s+osman)', # From metadata
        ]
        for pattern in author_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                author = match.group(1).title()
                logger.info(f"Found author from pattern: '{author}'")
                break
    
    # Strategy 4: Publisher extraction from URLs and metadata
    if 'penguin' in page_text.lower():
        publisher_patterns = [
            r'penguin\s+(?:books|publishing|random\s+house)',
            r'--\s*(penguin[^-]*?)(?:\s*--|\s*,)',  # From URLs
        ]
        for pattern in publisher_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                pub_text = match.group(1) if match.groups() else match.group(0)
                publisher = pub_text.strip().title()
                logger.info(f"Found publisher: '{publisher}'")
                break

    # Strategy 5: Fallback HTML div extraction only if we still need data
    if title == "Unknown Title" or author == "Unknown Author":
        logger.info("Attempting HTML div extraction as final fallback...")
        
        # Look through divs more carefully but avoid the description-heavy divs
        for i, div in enumerate(divs[:15]):  # Only first 15 divs to avoid descriptions
            if not div or not hasattr(div, 'get_text'):
                continue
                
            try:
                text = div.get_text(strip=True)
                
                # Skip very long texts (likely descriptions like we saw in logs)
                if len(text) > 200:
                    continue
                    
                # Skip obvious non-title/author content
                if any(skip in text.lower() for skip in ['description', 'alternative', 'metadata', '°°°']):
                    continue
                
                logger.debug(f"Checking div {i}: '{text[:50]}...'")
                
                # Look for title patterns
                if title == "Unknown Title":
                    # Check if this looks like a book title
                    if (_is_likely_title(text) and 
                        not _is_likely_author(text) and
                        len(text.split()) >= 2):  # At least 2 words for a title
                        title = text.title()
                        logger.info(f"Found title from div[{i}]: '{title}'")
                
                # Look for author patterns  
                if author == "Unknown Author":
                    if (_is_likely_author(text) and
                        not _is_likely_title(text)):
                        author = text
                        logger.info(f"Found author from div[{i}]: '{author}'")
                
                if title != "Unknown Title" and author != "Unknown Author":
                    break
                    
            except Exception as e:
                logger.debug(f"Error processing div {i}: {e}")
                continue
    
    # Strategy 6: Extract format and size more carefully
    try:
        # First, try to find actual file size information
        size_patterns = [
            r'(\d+(?:\.\d+)?\s*(?:mb|kb|gb))',  # Direct size pattern
            r'size[:\s]*(\d+(?:\.\d+)?\s*(?:mb|kb|gb))',  # Size with label
        ]
        
        for pattern in size_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                size = match.group(1).lower()
                logger.info(f"Found size from pattern: '{size}'")
                break
        
        # If we still don't have size, look in specific divs but be more careful
        if not size:
            # Look for actual size info in shorter div texts
            for i, div in enumerate(divs[:20]):
                if not div or not hasattr(div, 'get_text'):
                    continue
                try:
                    div_text = div.get_text(strip=True)
                    # Only consider short div texts for size (avoid descriptions)
                    if len(div_text) < 100 and any(u in div_text.lower() for u in ["mb", "kb", "gb"]):
                        # Extract just the size part
                        size_match = re.search(r'(\d+(?:\.\d+)?\s*(?:mb|kb|gb))', div_text, re.IGNORECASE)
                        if size_match:
                            size = size_match.group(1).lower()
                            logger.info(f"Found size from div[{i}]: '{size}'")
                            break
                except:
                    continue
        
        # Look for format in the URL (most reliable source based on your log)
        for link in download_links:
            href = link.get('href', '')
            # Look for .epub, .pdf, etc. in download URLs
            for fmt in SUPPORTED_FORMATS:
                if f'.{fmt}' in href.lower():
                    format = fmt
                    logger.info(f"Found format from URL: '{format}'")
                    break
            if format != "epub":
                break
                    
    except Exception as e:
        logger.debug(f"Error in format/size extraction: {e}")
    
    # Final validation and cleanup
    if title and title != "Unknown Title":
        title = title.strip()
        # Remove common prefixes that might indicate this isn't actually the title
        title = re.sub(r'^(title:|source title:|book title:)\s*', '', title, flags=re.IGNORECASE)
        title = title.title() if not title.isupper() else title  # Proper case if not all caps
    
    if author and author != "Unknown Author":
        author = author.strip()
        # Remove common prefixes
        author = re.sub(r'^(author:|by |written by )\s*', '', author, flags=re.IGNORECASE)
    
    # Log final results
    logger.info(f"=== FINAL EXTRACTION RESULTS ===")
    logger.info(f"Title: '{title}'")
    logger.info(f"Author: '{author}'")
    logger.info(f"Publisher: '{publisher}'")
    logger.info(f"Format: '{format}'")
    logger.info(f"Size: '{size}'")
    logger.info(f"================================")

    # Extract download URLs (existing logic)
    slow_urls_no_waitlist = set()
    slow_urls_with_waitlist = set()
    external_urls_libgen = set()
    external_urls_z_lib = set()
    external_urls_welib = set()

    for url in download_links:
        try:
            if url.text.strip().lower().startswith("slow partner server"):
                if (
                    url.next is not None
                    and url.next.next is not None
                    and "waitlist" in url.next.next.strip().lower()
                ):
                    internal_text = url.next.next.strip().lower()
                    if "no waitlist" in internal_text:
                        slow_urls_no_waitlist.add(url["href"])
                    else:
                        slow_urls_with_waitlist.add(url["href"])
            elif (
                url.next is not None
                and url.next.next is not None
                and "click \"GET\" at the top" in url.next.next.text.strip()
            ):
                libgen_url = url["href"]
                # TODO : Temporary fix ? Maybe get URLs from https://open-slum.org/ ?
                libgen_url = re.sub(r'libgen\.(lc|is|bz|st)', 'libgen.gl', url["href"])

                external_urls_libgen.add(libgen_url)
            elif url.text.strip().lower().startswith("z-lib"):
                if ".onion/" not in url["href"]:
                    external_urls_z_lib.add(url["href"])
        except:
            pass

    external_urls_welib = _get_download_urls_from_welib(book_id) if USE_CF_BYPASS else set()

    urls = []
    urls += list(external_urls_welib) if PRIORITIZE_WELIB else []
    urls += list(slow_urls_no_waitlist) if USE_CF_BYPASS else []
    urls += list(external_urls_libgen)
    urls += list(external_urls_welib) if not PRIORITIZE_WELIB else []
    urls += list(slow_urls_with_waitlist)  if USE_CF_BYPASS else []
    urls += list(external_urls_z_lib)

    for i in range(len(urls)):
        urls[i] = downloader.get_absolute_url(AA_BASE_URL, urls[i])

    # Remove empty urls
    urls = [url for url in urls if url != ""]

    # Create book info object
    book_info = BookInfo(
        id=book_id,
        preview=preview,
        title=title,
        publisher=publisher,
        author=author,
        format=format,
        size=size,
        download_urls=urls,
    )

    # Extract additional metadata (existing logic)
    try:
        info = _extract_book_metadata(divs[-6])
        book_info.info = info

        # Set language and year from metadata if available
        if info.get("Language"):
            book_info.language = info["Language"][0]
        if info.get("Year"):
            book_info.year = info["Year"][0]
    except (IndexError, AttributeError, Exception) as e:
        logger.warning(f"Could not extract metadata for book ID {book_id}: {e}")
        book_info.info = {}

    return book_info

def _get_download_urls_from_welib(book_id: str) -> set[str]:
    """Get download urls from welib.org."""
    url = f"https://welib.org/md5/{book_id}"
    logger.info(f"Getting download urls from welib.org for {book_id}. While this uses the bypasser, it will not start downloading them yet.")
    html = downloader.html_get_page(url, use_bypasser=True)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    download_links = soup.find_all("a", href=True)
    download_links = [link["href"] for link in download_links]
    download_links = [link for link in download_links if "/slow_download/" in link]
    download_links = [downloader.get_absolute_url(url, link) for link in download_links]
    return set(download_links)

def _extract_book_metadata(
    metadata_divs
) -> Dict[str, List[str]]:
    """Extract metadata from book info divs."""
    info: Dict[str, List[str]] = {}

    # Process the first set of metadata
    sub_datas = metadata_divs.find_all("div")[0]
    sub_datas = list(sub_datas.children)
    for sub_data in sub_datas:
        if sub_data.text.strip() == "":
            continue
        sub_data = list(sub_data.children)
        key = sub_data[0].text.strip()
        value = sub_data[1].text.strip()
        if key not in info:
            info[key] = set()
        info[key].add(value)
    
    # make set into list
    for key, value in info.items():
        info[key] = list(value)

    # Filter relevant metadata
    relevant_prefixes = [
        "ISBN-",
        "ALTERNATIVE",
        "ASIN",
        "Goodreads",
        "Language",
        "Year",
    ]
    return {
        k.strip(): v
        for k, v in info.items()
        if any(k.lower().startswith(prefix.lower()) for prefix in relevant_prefixes)
        and "filename" not in k.lower()
    }


def download_book(book_info: BookInfo, book_path: Path, progress_callback: Optional[Callable[[float], None]] = None, cancel_flag: Optional[Event] = None) -> bool:
    """Download a book from available sources.

    Args:
        book_id: Book identifier (MD5 hash)
        title: Book title for logging

    Returns:
        Optional[BytesIO]: Book content buffer if successful
    """

    if len(book_info.download_urls) == 0:
        book_info = get_book_info(book_info.id)
    download_links = book_info.download_urls

    # If AA_DONATOR_KEY is set, use the fast download URL. Else try other sources.
    if AA_DONATOR_KEY != "":
        download_links.insert(
            0,
            f"{AA_BASE_URL}/dyn/api/fast_download.json?md5={book_info.id}&key={AA_DONATOR_KEY}",
        )

    for link in download_links:
        try:
            download_url = _get_download_url(link, book_info.title, cancel_flag)
            if download_url != "":
                logger.info(f"Downloading `{book_info.title}` from `{download_url}`")

                data = downloader.download_url(download_url, book_info.size or "", progress_callback, cancel_flag)
                if not data:
                    raise Exception("No data received")

                logger.info(f"Download finished. Writing to {book_path}")
                with open(book_path, "wb") as f:
                    f.write(data.getbuffer())
                logger.info(f"Writing `{book_info.title}` successfully")
                return True

        except Exception as e:
            logger.error_trace(f"Failed to download from {link}: {e}")
            continue

    return False


def _get_download_url(link: str, title: str, cancel_flag: Optional[Event] = None) -> str:
    """Extract actual download URL from various source pages."""

    url = ""

    if link.startswith(f"{AA_BASE_URL}/dyn/api/fast_download.json"):
        page = downloader.html_get_page(link)
        url = json.loads(page).get("download_url")
    else:
        html = downloader.html_get_page(link)

        if html == "":
            return ""

        soup = BeautifulSoup(html, "html.parser")

        if link.startswith("https://z-lib."):
            download_link = soup.find_all("a", href=True, class_="addDownloadedBook")
            if download_link:
                url = download_link[0]["href"]
        elif "/slow_download/" in link:
            download_links = soup.find_all("a", href=True, string="📚 Download now")
            if not download_links:
                countdown = soup.find_all("span", class_="js-partner-countdown")
                if countdown:
                    sleep_time = int(countdown[0].text)
                    logger.info(f"Waiting {sleep_time}s for {title}")
                    if cancel_flag is not None and cancel_flag.wait(timeout=sleep_time):
                        logger.info(f"Cancelled wait for {title}")
                        return ""
                    url = _get_download_url(link, title, cancel_flag)
            else:
                url = download_links[0]["href"]
        else:
            url = soup.find_all("a", string="GET")[0]["href"]

    return downloader.get_absolute_url(link, url)
