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


def _parse_book_info_page(soup: BeautifulSoup, book_id: str) -> BookInfo:
    """Parse the book info page HTML into a BookInfo object."""
    data = soup.select_one("body > main > div:nth-of-type(1)")

    if not data:
        raise Exception(f"Failed to parse book info for ID: {book_id}")

    preview: str = ""

    node = data.select_one("div:nth-of-type(1) > img")
    if node:
        preview_value = node.get("src", "")
        if isinstance(preview_value, list):
            preview = preview_value[0]
        else:
            preview = preview_value

    data = soup.find_all("div", {"class": "main-inner"})[0].find_next("div")
    divs = list(data.children)
    
    # Try to extract format and size from multiple locations
    format = ""
    size = ""
    
    # Strategy 1: Original approach - look at divs[13]
    try:
        if len(divs) > 13 and divs[13]:
            _details = divs[13].text.strip().lower().split(" · ")
            logger.info(f"Details from divs[13]: {_details}")
            
            for f in _details:
                if format == "" and f.strip().lower() in SUPPORTED_FORMATS:
                    format = f.strip().lower()
                if size == "" and any(u in f.strip().lower() for u in ["mb", "kb", "gb"]):
                    size = f.strip().lower()

            if format == "" or size == "":
                for f in _details:
                    if format == "" and f.strip() and not " " in f.strip().lower():
                        potential_format = f.strip().lower()
                        if potential_format in SUPPORTED_FORMATS:
                            format = potential_format
                    if size == "" and "." in f.strip().lower():
                        size = f.strip().lower()
    except Exception as e:
        logger.debug(f"Error extracting from divs[13]: {e}")
    
    # Strategy 2: Look through all divs for format/size info
    if not format or not size:
        try:
            for div in divs:
                if hasattr(div, 'text'):
                    div_text = div.text.strip().lower()
                    # Look for format indicators
                    for fmt in SUPPORTED_FORMATS:
                        if fmt in div_text and not format:
                            format = fmt
                            break
                    # Look for size indicators  
                    if any(u in div_text for u in ["mb", "kb", "gb"]) and not size:
                        # Extract size with regex
                        import re
                        size_match = re.search(r'(\d+(?:\.\d+)?\s*(?:mb|kb|gb))', div_text)
                        if size_match:
                            size = size_match.group(1)
                            break
        except Exception as e:
            logger.debug(f"Error in format/size strategy 2: {e}")
    
    # Strategy 3: Look in download URLs for format hints
    if not format:
        try:
            # We'll extract URLs first, then check them for format clues
            every_url = soup.find_all("a")
            for url in every_url:
                href = url.get('href', '')
                for fmt in SUPPORTED_FORMATS:
                    if f'.{fmt}' in href.lower():
                        format = fmt
                        break
                if format:
                    break
        except Exception as e:
            logger.debug(f"Error in format strategy 3: {e}")
    
    # Fallback format
    if not format:
        format = "epub"  # Default format
        
    logger.info(f"Final extracted format: '{format}', size: '{size}'")

    every_url = soup.find_all("a")
    slow_urls_no_waitlist = set()
    slow_urls_with_waitlist = set()
    external_urls_libgen = set()
    external_urls_z_lib = set()
    external_urls_welib = set()

    for url in every_url:
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

    # Helper function to safely get text from divs
    def safe_get_text(div_element, default=""):
        """Safely extract text from a div element."""
        if div_element is None:
            return default
        
        # First try to get the text content directly from the element
        if hasattr(div_element, 'get_text'):
            try:
                text = div_element.get_text(strip=True)
                if text and not text.startswith('span') and 'class' not in text.lower():
                    return text
            except:
                pass
        
        if hasattr(div_element, 'text'):
            try:
                text = div_element.text.strip()
                if text and not text.startswith('span') and 'class' not in text.lower():
                    return text
            except:
                pass
        
        # Try to get the next sibling text (original approach) but validate it
        if hasattr(div_element, 'next') and div_element.next is not None:
            next_element = div_element.next
            if hasattr(next_element, 'strip') and callable(getattr(next_element, 'strip')):
                try:
                    text = next_element.strip()
                    # Validate that we're not getting HTML/CSS artifacts
                    if text and not text.startswith('span') and 'class' not in text.lower() and len(text) > 3:
                        return text
                except:
                    pass
            else:
                try:
                    text = str(next_element).strip()
                    # Validate that we're not getting HTML/CSS artifacts
                    if text and not text.startswith('span') and 'class' not in text.lower() and len(text) > 3:
                        return text
                except:
                    pass
        
        # Try finding text in child elements
        if hasattr(div_element, 'find_all'):
            try:
                # Look for text nodes that aren't in span elements with classes
                text_elements = div_element.find_all(text=True)
                for text_elem in text_elements:
                    if text_elem.strip() and not text_elem.strip().startswith('span') and 'class' not in text_elem.strip().lower():
                        text = text_elem.strip()
                        if len(text) > 3:  # Ensure it's not just punctuation
                            return text
            except:
                pass
        
        # Last resort - convert to string but validate
        try:
            text = str(div_element).strip()
            # If it contains HTML tags, try to extract just the text
            if '<' in text and '>' in text:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(text, 'html.parser')
                text = soup.get_text(strip=True)
            
            if text and not text.startswith('span') and 'class' not in text.lower() and len(text) > 3:
                return text
        except:
            pass
            
        return default

    # Try multiple strategies to extract book information
    def extract_book_details(soup, divs):
        """Extract title, author, publisher using multiple strategies."""
        details = {'title': '', 'author': '', 'publisher': ''}
        
        # Strategy 1: Look for "source title:" in the metadata text
        try:
            page_text = soup.get_text().lower()
            if 'source title:' in page_text:
                # Extract the line containing source title
                lines = page_text.split('\n')
                for line in lines:
                    if 'source title:' in line:
                        title_part = line.split('source title:')[1].strip()
                        # Clean up the title
                        if title_part:
                            details['title'] = title_part.title()
                            logger.info(f"Found title from source title: {details['title']}")
                            break
        except Exception as e:
            logger.debug(f"Error in source title strategy: {e}")
        
        # Strategy 2: Try the original div indices but validate the content
        strategies = [
            # (div_index, expected_field)
            (7, 'title'),
            (9, 'author'), 
            (11, 'publisher')
        ]
        
        for div_idx, field in strategies:
            try:
                if div_idx < len(divs) and not details[field]:
                    text = safe_get_text(divs[div_idx], "")
                    if text and len(text) > 2:
                        details[field] = text
            except (IndexError, AttributeError):
                continue
        
        # Strategy 3: Look for specific text patterns in the page
        if not details['title']:
            try:
                # Find h1 tags which often contain titles
                h1_tags = soup.find_all('h1')
                for h1 in h1_tags:
                    text = safe_get_text(h1, "")
                    if text and len(text) > 5:
                        details['title'] = text
                        break
                        
                # Look for spans or divs with book-related content
                potential_titles = soup.find_all(['span', 'div'], text=True)
                for elem in potential_titles:
                    text = safe_get_text(elem, "").strip()
                    if text and len(text) > 10 and ':' in text:
                        # Likely a book title if it's long and has a colon
                        details['title'] = text
                        break
                        
            except Exception as e:
                logger.debug(f"Error in strategy 3: {e}")
        
        # Strategy 4: Extract from metadata if available
        if not details['title']:
            try:
                # Look for meta tags or structured data
                meta_title = soup.find('meta', property='og:title')
                if meta_title:
                    details['title'] = meta_title.get('content', '')
                    
                # Look for JSON-LD structured data
                scripts = soup.find_all('script', type='application/ld+json')
                for script in scripts:
                    try:
                        import json
                        data = json.loads(script.string)
                        if isinstance(data, dict):
                            if 'name' in data:
                                details['title'] = data['name']
                            if 'author' in data and not details['author']:
                                author_data = data['author']
                                if isinstance(author_data, dict) and 'name' in author_data:
                                    details['author'] = author_data['name']
                                elif isinstance(author_data, str):
                                    details['author'] = author_data
                    except:
                        continue
                        
            except Exception as e:
                logger.debug(f"Error in strategy 4: {e}")
            
        return details

    # Extract book details using multiple strategies
    book_details = extract_book_details(soup, divs)
    
    # Extract basic information with error handling
    title = book_details['title'] or "Unknown Title"
    author = book_details['author'] or "Unknown Author"  
    publisher = book_details['publisher'] or "Unknown Publisher"
    
    # Log what we extracted
    logger.info(f"Extracted book details - Title: '{title}', Author: '{author}', Publisher: '{publisher}'")

    # If title looks like an author name and author is empty, swap them
    if title and author == "Unknown Author" and len(title.split()) <= 3 and all(word.istitle() for word in title.split()):
        logger.info(f"Title '{title}' looks like an author name, swapping fields")
        author = title
        title = "Unknown Title"

    # Extract basic information
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

    # Extract additional metadata
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
