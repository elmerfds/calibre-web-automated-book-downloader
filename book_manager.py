"""Book download manager handling search and retrieval operations."""

import time, json, re, hashlib
from pathlib import Path
from urllib.parse import quote, unquote
from typing import List, Optional, Dict, Union, Callable, Tuple
from threading import Event
from bs4 import BeautifulSoup, Tag, NavigableString

import downloader
from logger import setup_logger
from config import SUPPORTED_FORMATS, BOOK_LANGUAGE, AA_BASE_URL
from env import AA_DONATOR_KEY, USE_CF_BYPASS, PRIORITIZE_WELIB
from models import BookInfo, SearchFilters

logger = setup_logger(__name__)


def search_books(query: str, filters: SearchFilters) -> List[BookInfo]:
    """Search for books matching the query across multiple sources."""
    books = []
    
    # Search Anna's Archive first
    aa_books = _search_annas_archive(query, filters)
    books.extend(aa_books)
    
    # Search OceanofPDF for EPUB books
    try:
        ocean_books = _search_oceanofpdf(query, filters)
        books.extend(ocean_books)
        logger.info(f"Added {len(ocean_books)} EPUB books from OceanofPDF")
    except Exception as e:
        logger.error_trace(f"OceanofPDF search failed: {e}")
    
    # Sort by format preference and source
    books.sort(key=lambda x: (
        SUPPORTED_FORMATS.index(x.format) if x.format in SUPPORTED_FORMATS else len(SUPPORTED_FORMATS),
        0 if x.info and "source" in x.info and "Anna's Archive" in x.info["source"] else 1
    ))
    
    total_books = len(books)
    aa_count = len([b for b in books if b.info and "source" in b.info and "Anna's Archive" in b.info["source"]])
    ocean_count = len([b for b in books if b.info and "source" in b.info and "OceanofPDF" in b.info["source"]])
    
    logger.info(f"Total search results: {total_books} books (Anna's Archive: {aa_count}, OceanofPDF: {ocean_count})")
    
    return books


def _search_annas_archive(query: str, filters: SearchFilters) -> List[BookInfo]:
    """Search Anna's Archive (existing functionality)."""
    query_html = quote(query)

    if filters.isbn:
        isbns = " || ".join([f"('isbn13:{isbn}' || 'isbn10:{isbn}')" for isbn in filters.isbn])
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

    formats_to_use = filters.format if filters.format else SUPPORTED_FORMATS

    index = 1
    for filter_type, filter_values in vars(filters).items():
        if filter_type == "author" or filter_type == "title" and filter_values:
            for value in filter_values:
                filters_query += f"&termtype_{index}={filter_type}&termval_{index}={quote(value)}"
                index += 1

    url = (f"{AA_BASE_URL}/search?index=&page=1&display=table"
           f"&acc=aa_download&acc=external_download"
           f"&ext={'&ext='.join(formats_to_use)}&q={query_html}{filters_query}")

    html = downloader.html_get_page(url)
    if not html or "No files found." in html:
        logger.info(f"No books found in Anna's Archive for query: '{query}'")
        return []

    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("table")
    if not tbody:
        logger.info(f"No results table found in Anna's Archive for query: '{query}'")
        return []

    books = []
    for line_tr in tbody.find_all("tr"):
        try:
            book = _parse_aa_search_result_row(line_tr)
            if book:
                # Add source information
                book.info = book.info or {}
                book.info["source"] = ["Anna's Archive"]
                books.append(book)
        except Exception as e:
            logger.error_trace(f"Failed to parse Anna's Archive result row: {e}")

    logger.info(f"Found {len(books)} books from Anna's Archive")
    return books


def _search_oceanofpdf(query: str, filters: SearchFilters = None) -> List[BookInfo]:
    """Search OceanofPDF and return EPUB book information."""
    try:
        books = []
        search_url = f"https://oceanofpdf.com/?s={quote(query)}"
        
        html = downloader.html_get_page(search_url, use_bypasser=True)
        if not html:
            return books
            
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all("article", class_=lambda x: x and "post" in x and "type-post" in x)
        
        for article in articles:
            try:
                title_link = article.find("a", class_="entry-title-link")
                if not title_link:
                    continue
                    
                book_url = title_link.get("href", "")
                title = title_link.get_text(strip=True)
                
                # Extract metadata
                meta_div = article.find("div", class_="postmetainfo")
                author = "Unknown Author"
                language = "English"
                genres = ""
                
                if meta_div:
                    meta_text = meta_div.get_text()
                    author_match = re.search(r"Author:\s*([^\n\r]+)", meta_text)
                    if author_match:
                        author = author_match.group(1).strip()
                    
                    lang_match = re.search(r"Language:\s*([^\n\r]+)", meta_text)
                    if lang_match:
                        language = lang_match.group(1).strip()
                    
                    genre_match = re.search(r"Genre:\s*([^\n\r]+)", meta_text)
                    if genre_match:
                        genres = genre_match.group(1).strip()
                
                # Get EPUB download info from book page
                epub_download_info = _get_oceanofpdf_epub_info(book_url)
                
                if epub_download_info:
                    book_info = BookInfo(
                        id=f"oceanpdf_{hashlib.md5(book_url.encode()).hexdigest()[:12]}",
                        title=title,
                        author=author,
                        language=language,
                        format="epub",  # Always EPUB for OceanofPDF
                        preview=_extract_cover_image(article),
                        download_urls=[epub_download_info],
                        info={
                            "source": ["OceanofPDF"],
                            "genres": [genres] if genres else []
                        }
                    )
                    books.append(book_info)
                
            except Exception as e:
                logger.debug(f"Error parsing OceanofPDF book: {e}")
                continue
                
        logger.info(f"Found {len(books)} EPUB books from OceanofPDF")
        return books
        
    except Exception as e:
        logger.error_trace(f"Error searching OceanofPDF: {e}")
        return []


def _get_oceanofpdf_epub_info(book_page_url: str) -> Optional[str]:
    """Extract EPUB download information from OceanofPDF book page."""
    try:
        html = downloader.html_get_page(book_page_url, use_bypasser=True)
        if not html:
            return None
            
        soup = BeautifulSoup(html, "html.parser")
        
        # Find the EPUB form
        forms = soup.find_all("form", action=lambda x: x and "Fetching_Resource.php" in x)
        
        for form in forms:
            filename_input = form.find("input", {"name": "filename"})
            if filename_input:
                filename = filename_input.get("value", "")
                if filename.endswith(".epub"):
                    id_input = form.find("input", {"name": "id"})
                    if id_input:
                        server_id = id_input.get("value", "")
                        # Return custom URL format for OceanofPDF
                        return f"oceanofpdf://{server_id}/{quote(filename)}/{quote(book_page_url)}"
                        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting OceanofPDF EPUB info: {e}")
        return None


def _extract_cover_image(article) -> str:
    """Extract cover image from OceanofPDF article."""
    img_link = article.find("a", class_="entry-image-link")
    if img_link:
        img = img_link.find("img")
        if img:
            return img.get("data-src") or img.get("src", "")
    return ""


def _parse_aa_search_result_row(row: Tag) -> Optional[BookInfo]:
    """Parse a single Anna's Archive search result row into a BookInfo object."""
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
        logger.error_trace(f"Error parsing Anna's Archive search result row: {e}")
        return None


def get_book_info(book_id: str) -> BookInfo:
    """Get detailed information for a specific book."""
    # Handle OceanofPDF IDs differently
    if book_id.startswith("oceanpdf_"):
        # For OceanofPDF books, we already have the download info
        # This is a simplified version - in practice you might want to store more info
        return BookInfo(
            id=book_id,
            title="OceanofPDF Book",
            author="Unknown Author",
            format="epub",
            download_urls=[]  # Would need to reconstruct from stored data
        )
    
    # Existing Anna's Archive logic
    url = f"{AA_BASE_URL}/md5/{book_id}"
    html = downloader.html_get_page(url)
    if not html:
        raise Exception(f"Failed to fetch book info for ID: {book_id}")

    soup = BeautifulSoup(html, "html.parser")
    return _parse_book_info_page(soup, book_id)


def download_book_with_final_url(book_info: BookInfo, book_path: Path, progress_callback: Optional[Callable[[float], None]] = None, cancel_flag: Optional[Event] = None) -> Tuple[bool, Optional[str]]:
    """Download a book from available sources and return the final download URL."""
    if len(book_info.download_urls) == 0:
        book_info = get_book_info(book_info.id)
    
    download_links = book_info.download_urls[:]
    
    # Add Anna's Archive donator link if available
    if AA_DONATOR_KEY and not book_info.id.startswith("oceanpdf_"):
        download_links.insert(0, f"{AA_BASE_URL}/dyn/api/fast_download.json?md5={book_info.id}&key={AA_DONATOR_KEY}")

    for link in download_links:
        try:
            if link.startswith("oceanofpdf://"):
                # Handle OceanofPDF downloads
                logger.info(f"Downloading `{book_info.title}` from OceanofPDF")
                data = downloader.download_oceanofpdf_file(link, progress_callback, cancel_flag)
                if data:
                    with open(book_path, "wb") as f:
                        f.write(data.getbuffer())
                    logger.info(f"Successfully downloaded EPUB: {book_info.title}")
                    return True, link
            else:
                # Existing Anna's Archive download logic
                download_url = _get_download_url(link, book_info.title, cancel_flag)
                if download_url:
                    logger.info(f"Downloading `{book_info.title}` from `{download_url}`")
                    
                    data = downloader.download_url(download_url, book_info.size or "", progress_callback, cancel_flag)
                    if data:
                        with open(book_path, "wb") as f:
                            f.write(data.getbuffer())
                        logger.info(f"Successfully downloaded: {book_info.title}")
                        return True, download_url
        except Exception as e:
            logger.error_trace(f"Failed to download from {link}: {e}")
            continue

    return False, None


def _is_valid_title(text: str) -> bool:
    """Check if text could be a valid book title."""
    if not text or len(text.strip()) < 3:
        return False
    
    text = text.strip()
    
    # Reject obvious non-titles using generic patterns
    reject_patterns = [
        r'^\d{4},                                    # Just a year
        r'^[A-Z][a-z]+ Books,?\s*\d{4},            # "Publisher Books, Year"
        r'^\w+\s+\[\w+\]',                            # "Language [code]"
        r'\b(epub|pdf|mobi|azw3|fb2|djvu|cbz|cbr)\b', # File formats
        r'\breport\b.*\bquality\b',                   # UI elements
        r'^\w+/.*/',                                  # File paths
        r'^[a-f0-9]{32},                            # MD5 hash
    ]
    
    return not any(re.search(pattern, text, re.IGNORECASE) for pattern in reject_patterns)


def _is_valid_author(text: str) -> bool:
    """Check if text could be a valid author name."""
    if not text or len(text.strip()) < 2:
        return False
    
    text = text.strip()
    words = text.split()
    
    # Reject obvious non-authors
    reject_patterns = [
        r'^\d{4},                                    # Just a year
        r'^[A-Z][a-z]+ Books,?\s*\d{4},            # "Publisher Books, Year"
        r'\b(epub|pdf|mobi|azw3|fb2|djvu|cbz|cbr)\b', # File formats
        r'\breport\b.*\bquality\b',                   # UI elements
        r'\bunknown\b',                               # "Unknown" placeholder
        r'^[a-f0-9]{32},                            # MD5 hash
    ]
    
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in reject_patterns):
        return False
    
    # Check if it looks like a proper name (1-4 words, proper capitalization)
    if 1 <= len(words) <= 4:
        name_pattern = r'^[A-Z][a-z]+\.?  # Allow initials
        return all(re.match(name_pattern, word) for word in words)
    
    return False


def _parse_book_info_page(soup: BeautifulSoup, book_id: str) -> BookInfo:
    """Parse the book info page HTML into a BookInfo object."""
    logger.info(f"=== PARSING BOOK INFO FOR {book_id} ===")
    
    page_text = soup.get_text()
    download_links = soup.find_all("a", href=True)
    
    # Initialize defaults
    title = "Unknown Title"
    author = "Unknown Author"
    publisher = "Unknown Publisher"
    format = "epub"
    size = ""
    isbn = ""
    year = ""
    
    # Debug: Log sample URLs
    epub_urls = [link.get('href', '') for link in download_links if 'epub' in link.get('href', '').lower()][:3]
    logger.info(f"Sample EPUB URLs: {epub_urls}")
    
    # Strategy 1: Extract from download URLs (most reliable)
    for link in download_links:
        href = link.get('href', '')
        if '.epub' in href.lower() and '%20' in href:
            # Generic URL patterns for title extraction
            title_patterns = [
                r'/([^/]+?)%20--%20[^/]+?%20--%20',  # Title -- Author --
                r'/([^/]+?)%20--%20',                # Title --
                r'/([^/]*%20[^/]*%20[^/]*)\.epub',  # Multi-word title.epub
            ]
            
            for pattern in title_patterns:
                match = re.search(pattern, href, re.IGNORECASE)
                if match:
                    url_title = match.group(1).replace('%20', ' ').replace('%3A', ':').replace('%28', '(').replace('%29', ')')
                    url_title = re.sub(r'\s+', ' ', url_title).strip()
                    if len(url_title) > 5 and _is_valid_title(url_title):
                        title = url_title
                        logger.info(f"Found title from URL: '{title}'")
                        break
            
            # Extract author from URLs
            author_patterns = [
                r'%20--%20([A-Z][a-z]+\s+[A-Z][a-z]+)%20--%20',
                r'%20--%20([A-Z][a-z]+\s+[A-Z][a-z]+)\.epub',
                r'/[^/]+?%20--%20([A-Z][a-z]+\s+[A-Z][a-z]+)',
            ]
            
            for pattern in author_patterns:
                match = re.search(pattern, href)
                if match:
                    url_author = match.group(1).replace('%20', ' ').strip()
                    if _is_valid_author(url_author):
                        author = url_author
                        logger.info(f"Found author from URL: '{author}'")
                        break
            
            if title != "Unknown Title" and author != "Unknown Author":
                break
    
    # Strategy 2: Extract from metadata patterns in page text
    if title == "Unknown Title":
        source_match = re.search(r'source title:\s*([^:]+?)(?:\s*date open sourced|\n|\r|$)', page_text, re.IGNORECASE)
        if source_match:
            raw_title = source_match.group(1).strip()
            if _is_valid_title(raw_title):
                title = raw_title.title()
                logger.info(f"Found title from source pattern: '{title}'")
    
    # Strategy 3: Extract from file path patterns
    if title == "Unknown Title" or author == "Unknown Author":
        filepath_match = re.search(r'([^/]+?)\s*\((?:retail|paperback|hardcover)\)?[\s\-]*([A-Z][a-z]+\s+[A-Z][a-z]+)\.epub', page_text, re.IGNORECASE)
        if filepath_match:
            path_title = filepath_match.group(1).strip()
            path_author = filepath_match.group(2).strip()
            if _is_valid_title(path_title) and title == "Unknown Title":
                title = path_title
                logger.info(f"Found title from filepath: '{title}'")
            if _is_valid_author(path_author) and author == "Unknown Author":
                author = path_author
                logger.info(f"Found author from filepath: '{author}'")
    
    # Extract additional metadata
    year_match = re.search(r'\b(19|20)\d{2}\b', page_text)
    if year_match:
        year = year_match.group(0)
    
    isbn_match = re.search(r'\b(97[89]\d{10}|\d{9}[\dX])\b', page_text)
    if isbn_match:
        isbn = isbn_match.group(0)
    
    size_match = re.search(r'(\d+(?:\.\d+)?\s*(?:mb|kb|gb))', page_text, re.IGNORECASE)
    if size_match:
        size = size_match.group(1).lower()
    
    # Extract preview image
    preview = ""
    preview_img = soup.select_one("body > main > div:nth-of-type(1) > div:nth-of-type(1) > img")
    if preview_img:
        preview = preview_img.get("src", "")
    
    # Extract download URLs
    urls = _extract_download_urls(soup, book_id)
    
    logger.info(f"=== EXTRACTION RESULTS ===")
    logger.info(f"Title: '{title}' | Author: '{author}' | Year: '{year}' | ISBN: '{isbn}'")
    
    return BookInfo(
        id=book_id,
        preview=preview,
        title=title,
        author=author,
        publisher=publisher,
        year=year,
        format=format,
        size=size,
        download_urls=urls,
        info={"ISBN": [isbn], "source": ["Anna's Archive"]} if isbn else {"source": ["Anna's Archive"]}
    )


def _extract_download_urls(soup: BeautifulSoup, book_id: str) -> List[str]:
    """Extract download URLs from the page."""
    download_links = soup.find_all("a", href=True)
    slow_urls_no_waitlist = set()
    slow_urls_with_waitlist = set()
    external_urls_libgen = set()
    external_urls_z_lib = set()

    for url in download_links:
        try:
            if url.text.strip().lower().startswith("slow partner server"):
                if url.next and url.next.next and "waitlist" in url.next.next.strip().lower():
                    if "no waitlist" in url.next.next.strip().lower():
                        slow_urls_no_waitlist.add(url["href"])
                    else:
                        slow_urls_with_waitlist.add(url["href"])
            elif (url.next and url.next.next and "click \"GET\" at the top" in url.next.next.text.strip()):
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
    urls += list(slow_urls_with_waitlist) if USE_CF_BYPASS else []
    urls += list(external_urls_z_lib)

    return [downloader.get_absolute_url(AA_BASE_URL, url) for url in urls if url]


def _get_download_urls_from_welib(book_id: str) -> set[str]:
    """Get download urls from welib.org."""
    try:
        url = f"https://welib.org/md5/{book_id}"
        html = downloader.html_get_page(url, use_bypasser=True)
        if not html:
            return set()
        
        soup = BeautifulSoup(html, "html.parser")
        download_links = [link["href"] for link in soup.find_all("a", href=True) if "/slow_download/" in link["href"]]
        return set(downloader.get_absolute_url(url, link) for link in download_links)
    except:
        return set()


def download_book(book_info: BookInfo, book_path: Path, progress_callback: Optional[Callable[[float], None]] = None, cancel_flag: Optional[Event] = None) -> bool:
    """Download a book from available sources."""
    success, _ = download_book_with_final_url(book_info, book_path, progress_callback, cancel_flag)
    return success


def _get_download_url(link: str, title: str, cancel_flag: Optional[Event] = None) -> str:
    """Extract actual download URL from various source pages."""
    if link.startswith(f"{AA_BASE_URL}/dyn/api/fast_download.json"):
        try:
            page = downloader.html_get_page(link)
            return json.loads(page).get("download_url", "")
        except:
            return ""
    
    html = downloader.html_get_page(link)
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    if link.startswith("https://z-lib."):
        download_link = soup.find_all("a", href=True, class_="addDownloadedBook")
        return download_link[0]["href"] if download_link else ""
    elif "/slow_download/" in link:
        download_links = soup.find_all("a", href=True, string="📚 Download now")
        if not download_links:
            countdown = soup.find_all("span", class_="js-partner-countdown")
            if countdown:
                sleep_time = int(countdown[0].text)
                logger.info(f"Waiting {sleep_time}s for {title}")
                if cancel_flag and cancel_flag.wait(timeout=sleep_time):
                    return ""
                return _get_download_url(link, title, cancel_flag)
        return download_links[0]["href"] if download_links else ""
    else:
        get_links = soup.find_all("a", string="GET")
        return get_links[0]["href"] if get_links else ""

    return downloader.get_absolute_url(link, "")
