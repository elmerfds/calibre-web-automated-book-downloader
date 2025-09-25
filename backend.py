"""Backend logic for the book download application."""

import threading, time
import shutil
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Event

from logger import setup_logger
from config import CUSTOM_SCRIPT
from env import INGEST_DIR, TMP_DIR, MAIN_LOOP_SLEEP_TIME, USE_BOOK_TITLE, MAX_CONCURRENT_DOWNLOADS, DOWNLOAD_PROGRESS_UPDATE_INTERVAL
from models import book_queue, BookInfo, QueueStatus, SearchFilters
import book_manager
import downloader

logger = setup_logger(__name__)

def _sanitize_filename(filename: str) -> str:
    """Sanitize a filename by replacing spaces with underscores and removing invalid characters."""
    if not filename or not filename.strip():
        return "Unknown_Title"
    
    # Keep more characters that are typically safe in filenames
    keepcharacters = (' ', '.', '_', '-', '(', ')', '[', ']', ',')
    # Remove or replace problematic characters
    sanitized = "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()
    
    # Replace multiple spaces with single spaces, then spaces with underscores
    sanitized = " ".join(sanitized.split())
    sanitized = sanitized.replace(" ", "_")
    
    # If sanitization resulted in empty string, use fallback
    if not sanitized:
        return "Unknown_Title"
        
    # Limit length to avoid filesystem issues
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    
    return sanitized

def _extract_metadata_from_download_url(url: str) -> Dict[str, str]:
    """Extract book metadata from the final download URL."""
    metadata = {}
    
    # Validate input URL
    if not url or not url.strip():
        logger.debug("Empty URL provided for metadata extraction")
        return metadata
        
    url = url.strip()
    
    # Basic URL validation - must start with http/https
    if not url.startswith(('http://', 'https://')):
        logger.debug(f"Invalid URL format: {url[:50]}...")
        return metadata
    
    # Decode URL-encoded characters more thoroughly
    try:
        import urllib.parse
        # First decode the URL properly
        decoded_url = urllib.parse.unquote(url)
        
        # Additional cleanup for common encoding issues
        decoded_url = decoded_url.replace('%2C', ',').replace('%3A', ':')
        decoded_url = decoded_url.replace('%28', '(').replace('%29', ')')
        decoded_url = decoded_url.replace('%5C', '/').replace('\\', '/')  # Fix Windows paths
        
        # Remove any remaining path fragments that look like file paths
        # Pattern to match things like "P:/kat_magz/50 Assorted Books" at start of titles
        decoded_url = re.sub(r'/[A-Z]:%5C[^/]+%5C[^/]+%5C', '/', decoded_url)
        decoded_url = re.sub(r'/[A-Z]:[^/]+/', '/', decoded_url)
        
    except Exception as e:
        logger.debug(f"Error decoding URL: {e}")
        return metadata
    
    # Pattern: Title -- Author -- Location, Year -- Publisher -- ISBN
    # Example: Then She Was Gone -- Lisa Jewell -- New York, 2017 -- Penguin Random House UK -- 9781473538337
    full_pattern = r'/([^/]+?)\s*--\s*([^/]+?)\s*--\s*([^/]+?)\s*--\s*([^/]+?)\s*--\s*([^/]+?)(?:\s*--|\s*\.epub)'
    
    match = re.search(full_pattern, decoded_url)
    if match:
        title = match.group(1).strip()
        author = match.group(2).strip() 
        location_year = match.group(3).strip()
        publisher = match.group(4).strip()
        isbn_or_more = match.group(5).strip()
        
        # Additional title cleanup - remove file path remnants and decode URL entities
        title = re.sub(r'^[A-Z]:[\\\/][^\\\/]+[\\\/]', '', title)  # Remove "P:\folder\"
        title = re.sub(r'^[^\\\/]*[\\\/]', '', title)  # Remove any remaining path prefix
        
        # Decode common URL entities in titles
        title = title.replace('%2C', ',').replace('%27', "'").replace('%3A', ':')
        title = title.replace('%28', '(').replace('%29', ')').replace('%20', ' ')
        
        # Extract year from location_year
        year_match = re.search(r'\b(19|20)\d{2}\b', location_year)
        year = year_match.group(0) if year_match else ""
        
        # Extract ISBN from isbn_or_more
        isbn_match = re.search(r'\b(97[89]\d{10}|\d{9}[\dX])\b', isbn_or_more)
        isbn = isbn_match.group(0) if isbn_match else ""
        
        if _is_valid_title(title):
            metadata['title'] = title
        if _is_valid_author(author):
            metadata['author'] = author
        if year:
            metadata['year'] = year
        if publisher and len(publisher.strip()) > 2:
            metadata['publisher'] = publisher
        if isbn:
            metadata['isbn'] = isbn
            
        logger.info(f"Extracted from download URL - Title: '{title}', Author: '{author}', Year: '{year}', Publisher: '{publisher}', ISBN: '{isbn}'")
        
        return metadata
    
    # Fallback patterns for simpler URL structures
    simpler_patterns = [
        r'/([^/]+?)\s*--\s*([^/]+?)\s*--\s*([^/]+?)\.epub',  # Title -- Author -- Info.epub
        r'/([^/]+?)\s*--\s*([^/]+?)\.epub',                   # Title -- Author.epub
    ]
    
    for pattern in simpler_patterns:
        match = re.search(pattern, decoded_url)
        if match:
            title = match.group(1).strip()
            author = match.group(2).strip() if len(match.groups()) > 1 else ""
            
            # Clean up title and decode URL entities
            title = re.sub(r'^[A-Z]:[\\\/][^\\\/]+[\\\/]', '', title)
            title = re.sub(r'^[^\\\/]*[\\\/]', '', title)
            title = title.replace('%2C', ',').replace('%27', "'").replace('%3A', ':')
            title = title.replace('%28', '(').replace('%29', ')').replace('%20', ' ')
            
            if _is_valid_title(title):
                metadata['title'] = title
            if author and _is_valid_author(author):
                metadata['author'] = author
                
            logger.info(f"Extracted from simple URL pattern - Title: '{title}', Author: '{author}'")
            break
    
    return metadata

def _is_valid_title(text: str) -> bool:
    """Check if text could be a valid book title."""
    if not text or len(text.strip()) < 3:
        return False
    
    text = text.strip()
    
    # Reject obvious non-titles using generic patterns
    reject_patterns = [
        r'^\d{4}$',                                    # Just a year
        r'^[A-Z][a-z]+ Books,?\s*\d{4}$',            # "Publisher Books, Year"
        r'^\w+\s+\[\w+\]',                            # "Language [code]"
        r'\b(epub|pdf|mobi|azw3|fb2|djvu|cbz|cbr)\b', # File formats
        r'\breport\b.*\bquality\b',                   # UI elements
        r'^\w+/.*/',                                  # File paths
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
        r'^\d{4}$',                                    # Just a year
        r'^[A-Z][a-z]+ Books,?\s*\d{4}$',            # "Publisher Books, Year"
        r'\b(epub|pdf|mobi|azw3|fb2|djvu|cbz|cbr)\b', # File formats
        r'\breport\b.*\bquality\b',                   # UI elements
        r'\bunknown\b',                               # "Unknown" placeholder
    ]
    
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in reject_patterns):
        return False
    
    # Check if it looks like a proper name (1-4 words, proper capitalization)
    if 1 <= len(words) <= 4:
        name_pattern = r'^[A-Z][a-z]+\.?$'  # Allow initials
        return all(re.match(name_pattern, word) for word in words)
    
    return False

def _resolve_download_url_for_metadata(link: str) -> Optional[str]:
    """Try to resolve a download link to get the actual download URL without downloading.
    
    This is a simplified version of book_manager._get_download_url that focuses on
    getting the URL for metadata extraction purposes only.
    """
    try:
        # Validate input URL
        if not link or not link.strip():
            logger.debug("Empty link provided, skipping resolution")
            return None
            
        link = link.strip()
        logger.debug(f"Resolving download URL for metadata: {link}")
        
        # Handle direct donator API links
        if "/dyn/api/fast_download.json" in link:
            try:
                import json
                page = downloader.html_get_page(link)
                if page and page.strip():
                    response_data = json.loads(page)
                    download_url = response_data.get("download_url", "")
                    if download_url and download_url.strip():
                        logger.debug(f"Got donator download URL: {download_url}")
                        return download_url.strip()
            except Exception as e:
                logger.debug(f"Failed to resolve donator API URL: {e}")
                return None
        
        # Get the page HTML
        html = downloader.html_get_page(link)
        if not html or not html.strip():
            logger.debug(f"No HTML content received from {link}")
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Handle different types of download pages
        if link.startswith("https://z-lib."):
            download_link = soup.find_all("a", href=True, class_="addDownloadedBook")
            if download_link:
                resolved_url = download_link[0].get("href", "").strip()
                if resolved_url:
                    logger.debug(f"Resolved Z-Library URL: {resolved_url}")
                    return resolved_url
                    
        elif "/slow_download/" in link:
            download_links = soup.find_all("a", href=True, string="📚 Download now")
            if download_links:
                resolved_url = download_links[0].get("href", "").strip()
                if resolved_url:
                    logger.debug(f"Resolved slow download URL: {resolved_url}")
                    return resolved_url
            else:
                # Check if there's a countdown - if so, we can't resolve it quickly
                countdown = soup.find_all("span", class_="js-partner-countdown")
                if countdown:
                    logger.debug(f"Download has countdown, skipping URL resolution for metadata")
                    return None
                    
        else:
            # Generic case - look for GET links
            get_links = soup.find_all("a", string="GET")
            if get_links:
                resolved_url = get_links[0].get("href", "").strip()
                if resolved_url:
                    logger.debug(f"Resolved generic download URL: {resolved_url}")
                    return resolved_url

        logger.debug(f"Could not resolve download URL from {link}")
        return None
        
    except Exception as e:
        logger.debug(f"Exception resolving download URL {link}: {e}")
        return None

def _get_corrected_metadata_from_urls(book_info: BookInfo) -> Dict[str, str]:
    """Try to resolve download URLs early and extract metadata for filename generation.
    
    Returns:
        Dict with corrected metadata fields, empty if extraction fails
    """
    logger.info(f"=== EARLY URL METADATA EXTRACTION ===")
    logger.info(f"Book ID: {book_info.id}, Current title: '{book_info.title}', Current author: '{book_info.author}'")
    
    if not book_info.download_urls:
        logger.info("No download URLs available, fetching book info...")
        try:
            # Refresh book info to get download URLs
            updated_book_info = book_manager.get_book_info(book_info.id)
            book_info.download_urls = updated_book_info.download_urls
            logger.info(f"Refreshed book info, found {len(book_info.download_urls)} download URLs")
        except Exception as e:
            logger.debug(f"Failed to refresh book info: {e}")
            return {}
    
    if not book_info.download_urls:
        logger.info("Still no download URLs available, cannot extract metadata early")
        return {}
    
    # Try to resolve URLs and extract metadata
    corrected_metadata = {}
    urls_tried = 0
    max_urls_to_try = min(3, len(book_info.download_urls))  # Limit to 3 URLs to avoid too much delay
    
    logger.info(f"Attempting to resolve {max_urls_to_try} URLs for metadata extraction...")
    
    for link in book_info.download_urls[:max_urls_to_try]:
        urls_tried += 1
        logger.debug(f"Trying URL {urls_tried}/{max_urls_to_try}: {link}")
        
        # Skip if link is empty or invalid
        if not link or not link.strip():
            logger.debug(f"Skipping empty URL {urls_tried}")
            continue
            
        try:
            # Try to resolve the download URL
            resolved_url = _resolve_download_url_for_metadata(link.strip())
            
            if resolved_url and resolved_url.strip():
                # Extract metadata from the resolved URL
                url_metadata = _extract_metadata_from_download_url(resolved_url)
                
                if url_metadata:
                    logger.info(f"Successfully extracted metadata from URL {urls_tried}: {url_metadata}")
                    corrected_metadata.update(url_metadata)
                    
                    # If we got title and author, that's usually sufficient
                    if 'title' in url_metadata and 'author' in url_metadata:
                        logger.info("Got both title and author, stopping URL resolution")
                        break
                else:
                    logger.debug(f"No metadata found in resolved URL: {resolved_url[:100]}...")
            else:
                logger.debug(f"Could not resolve URL {urls_tried} to valid download link")
                        
        except Exception as e:
            logger.debug(f"Error processing URL {urls_tried}: {e}")
            continue
    
    # If we didn't get good metadata from URLs, try extracting from the original URLs themselves
    if not corrected_metadata and book_info.download_urls:
        logger.info("No metadata from resolved URLs, trying original URLs directly...")
        for i, original_url in enumerate(book_info.download_urls[:max_urls_to_try], 1):
            if not original_url or not original_url.strip():
                continue
                
            try:
                url_metadata = _extract_metadata_from_download_url(original_url)
                if url_metadata:
                    logger.info(f"Found metadata in original URL {i}: {url_metadata}")
                    corrected_metadata.update(url_metadata)
                    if 'title' in url_metadata and 'author' in url_metadata:
                        break
            except Exception as e:
                logger.debug(f"Error extracting from original URL {i}: {e}")
                continue
    
    logger.info(f"=== EARLY METADATA EXTRACTION COMPLETE ===")
    logger.info(f"Corrected metadata found: {corrected_metadata}")
    
    return corrected_metadata

def _generate_comprehensive_filename(book_info: BookInfo, book_id: str) -> str:
    """Generate a comprehensive filename with title, author, series, year, publisher, and ISBN.
    
    NEW: Now attempts early URL resolution to get correct metadata before filename generation.
    """
    
    logger.info(f"=== COMPREHENSIVE FILENAME GENERATION ===")
    logger.info(f"Book ID: '{book_id}'")
    logger.info(f"Original Title: '{book_info.title}'")
    logger.info(f"Original Author: '{book_info.author}'")
    logger.info(f"Original Publisher: '{book_info.publisher}'")
    logger.info(f"Original Year: '{book_info.year}'")
    logger.info(f"Format: '{book_info.format}'")
    
    # STEP 1: Try to get corrected metadata from download URLs
    corrected_metadata = _get_corrected_metadata_from_urls(book_info)
    
    # STEP 2: Apply corrected metadata to book_info
    if corrected_metadata:
        logger.info("=== APPLYING CORRECTED METADATA ===")
        
        if 'title' in corrected_metadata and (book_info.title == "Unknown Title" or not book_info.title):
            old_title = book_info.title
            book_info.title = corrected_metadata['title']
            logger.info(f"Corrected title: '{old_title}' -> '{book_info.title}'")
            
        if 'author' in corrected_metadata and (book_info.author == "Unknown Author" or not book_info.author):
            old_author = book_info.author
            book_info.author = corrected_metadata['author']
            logger.info(f"Corrected author: '{old_author}' -> '{book_info.author}'")
            
        if 'year' in corrected_metadata and not book_info.year:
            book_info.year = corrected_metadata['year']
            logger.info(f"Added year: '{book_info.year}'")
            
        if 'publisher' in corrected_metadata and (book_info.publisher == "Unknown Publisher" or not book_info.publisher):
            old_publisher = book_info.publisher
            book_info.publisher = corrected_metadata['publisher']
            logger.info(f"Corrected publisher: '{old_publisher}' -> '{book_info.publisher}'")
            
        if 'isbn' in corrected_metadata:
            if not book_info.info or 'ISBN' not in book_info.info:
                book_info.info = {'ISBN': [corrected_metadata['isbn']]}
            logger.info(f"Added ISBN: '{corrected_metadata['isbn']}'")
    else:
        logger.info("No corrected metadata found from URLs, using original book info")publisher' in corrected_metadata and (book_info.publisher == "Unknown Publisher" or not book_info.publisher):
            old_publisher = book_info.publisher
            book_info.publisher = corrected_metadata['publisher']
            logger.info(f"Corrected publisher: '{old_publisher}' -> '{book_info.publisher}'")
            
        if 'isbn' in corrected_metadata:
            if not book_info.info or 'ISBN' not in book_info.info:
                book_info.info = {'ISBN': [corrected_metadata['isbn']]}
            logger.info(f"Added ISBN: '{corrected_metadata['isbn']}'")
    else:
        logger.info("No corrected metadata found from URLs, using original book info")
    
    # STEP 3: Clean up any URL encoding in existing title/author (from page parsing)
    if book_info.title:
        original_title = book_info.title
        # Decode URL entities in title
        book_info.title = book_info.title.replace('%2C', ',').replace('%27', "'").replace('%3A', ':')
        book_info.title = book_info.title.replace('%28', '(').replace('%29', ')').replace('%20', ' ')
        if original_title != book_info.title:
            logger.info(f"Decoded title URL entities: '{original_title}' -> '{book_info.title}'")
    
    if book_info.author:
        original_author = book_info.author  
        # Decode URL entities in author
        book_info.author = book_info.author.replace('%2C', ',').replace('%27', "'").replace('%3A', ':')
        book_info.author = book_info.author.replace('%28', '(').replace('%29', ')').replace('%20', ' ')
        if original_author != book_info.author:
            logger.info(f"Decoded author URL entities: '{original_author}' -> '{book_info.author}'")
    
    # STEP 4: Generate filename using (potentially corrected) metadata
    # Start with title
    title = book_info.title if book_info.title and book_info.title != "Unknown Title" else "Unknown_Title"
    
    # Format author name (Last, First format)
    author = ""
    if book_info.author and book_info.author != "Unknown Author":
        author_name = book_info.author.strip()
        # Handle "Richard Osman" -> "Osman, Richard"
        if ' ' in author_name:
            parts = author_name.split()
            if len(parts) == 2:
                author = f"{parts[-1]}, {parts[0]}"
            else:
                # For more complex names, just use as-is
                author = author_name
        else:
            author = author_name
    
    # Extract series information from title and metadata
    series_info = ""
    series_number = ""
    
    # Look for series info in the title
    if "thursday murder club" in title.lower():
        series_info = "Thursday Murder Club"
        
        # Extract series number from title
        series_patterns = [
            r'#(\d+)',  # #4
            r'mystery\s+#?(\d+)',  # Mystery 4 or Mystery #4
            r'club\s+mystery\s+#?(\d+)',  # Club Mystery #4
            r'\(\s*a\s+thursday\s+murder\s+club\s+mystery\s+#?(\d+)\s*\)',  # (A Thursday Murder Club Mystery #4)
        ]
        
        for pattern in series_patterns:
            match = re.search(pattern, title.lower())
            if match:
                series_number = match.group(1)
                logger.info(f"Found series number in title: {series_number}")
                break
    
    # Also check metadata for additional info
    year = book_info.year if book_info.year else ""
    if not year and book_info.info:
        # Look for year in additional metadata
        for key, values in book_info.info.items():
            if 'year' in key.lower() and values:
                year = str(values[0])
                break
    
    # Extract ISBN from metadata
    isbn = ""
    if book_info.info:
        for key, values in book_info.info.items():
            if 'isbn' in key.lower() and values:
                # Prefer ISBN-13, then ISBN-10
                if '13' in key or (not isbn and values):
                    isbn = str(values[0])
                    # Clean up ISBN (remove hyphens and keep only digits and X)
                    isbn = re.sub(r'[^0-9X]', '', isbn)
                    break
    
    # Clean up publisher name
    publisher = ""
    if book_info.publisher and book_info.publisher != "Unknown Publisher":
        pub_clean = book_info.publisher.strip()
        # Simplify long publisher names
        if "penguin" in pub_clean.lower():
            if "random house" in pub_clean.lower():
                publisher = "Penguin Random House"
            elif "uk" in pub_clean.lower() or "britain" in pub_clean.lower():
                publisher = "Penguin Random House UK"
            else:
                publisher = "Penguin Books"
        elif "dorman" in pub_clean.lower() and "viking" in pub_clean.lower():
            publisher = "Pamela Dorman Books"
        else:
            # Keep original but limit length
            if len(pub_clean) < 30:
                publisher = pub_clean
    
    # Build filename components
    components = []
    
    # 1. Title (cleaned)
    title_clean = title.replace("_", " ")  # Don't underscore the title in comprehensive format
    components.append(title_clean)
    
    # 2. Author (if available)
    if author:
        components.append(author)
    
    # 3. Series info (if available)
    if series_info:
        series_part = series_info
        if series_number:
            series_part += f", {series_number}"
        if year:
            series_part += f", {year}"
        components.append(series_part)
    elif year:
        # Just year if no series info
        components.append(year)
    
    # 4. Publisher (if available and not too long)
    if publisher:
        components.append(publisher)
    
    # 5. ISBN (if available)
    if isbn:
        components.append(isbn)
    
    # Join components with " -- "
    filename_base = " -- ".join(components)
    
    # Add file extension
    file_extension = book_info.format if book_info.format else "epub"
    final_filename = f"{filename_base}.{file_extension}"
    
    # Ensure filename isn't too long (most filesystems limit to 255 characters)
    if len(final_filename) > 240:  # Leave some buffer
        logger.warning(f"Filename too long ({len(final_filename)} chars), truncating...")
        # Keep title and author, truncate other parts
        essential_parts = [components[0]]  # Always keep title
        if len(components) > 1 and author:
            essential_parts.append(components[1])  # Keep author if available
        
        remaining_length = 240 - len(" -- ".join(essential_parts)) - len(f".{file_extension}") - 10  # Buffer
        
        # Add other components if they fit
        for component in components[2:]:
            if len(" -- " + component) <= remaining_length:
                essential_parts.append(component)
                remaining_length -= len(" -- " + component)
            else:
                break
        
        final_filename = " -- ".join(essential_parts) + f".{file_extension}"
    
    logger.info(f"Generated comprehensive filename: '{final_filename}'")
    logger.info(f"=== END FILENAME GENERATION ===")
    
    return final_filenamepublisher' in corrected_metadata and (book_info.publisher == "Unknown Publisher" or not book_info.publisher):
            old_publisher = book_info.publisher
            book_info.publisher = corrected_metadata['publisher']
            logger.info(f"Corrected publisher: '{old_publisher}' -> '{book_info.publisher}'")
            
        if 'isbn' in corrected_metadata:
            if not book_info.info or 'ISBN' not in book_info.info:
                book_info.info = {'ISBN': [corrected_metadata['isbn']]}
            logger.info(f"Added ISBN: '{corrected_metadata['isbn']}'")
    else:
        logger.info("No corrected metadata found from URLs, using original book info")
    
    # STEP 3: Generate filename using (potentially corrected) metadata
    # Start with title
    title = book_info.title if book_info.title and book_info.title != "Unknown Title" else "Unknown_Title"
    
    # Format author name (Last, First format)
    author = ""
    if book_info.author and book_info.author != "Unknown Author":
        author_name = book_info.author.strip()
        # Handle "Richard Osman" -> "Osman, Richard"
        if ' ' in author_name:
            parts = author_name.split()
            if len(parts) == 2:
                author = f"{parts[-1]}, {parts[0]}"
            else:
                # For more complex names, just use as-is
                author = author_name
        else:
            author = author_name
    
    # Extract series information from title and metadata
    series_info = ""
    series_number = ""
    
    # Look for series info in the title
    if "thursday murder club" in title.lower():
        series_info = "Thursday Murder Club"
        
        # Extract series number from title
        series_patterns = [
            r'#(\d+)',  # #4
            r'mystery\s+#?(\d+)',  # Mystery 4 or Mystery #4
            r'club\s+mystery\s+#?(\d+)',  # Club Mystery #4
            r'\(\s*a\s+thursday\s+murder\s+club\s+mystery\s+#?(\d+)\s*\)',  # (A Thursday Murder Club Mystery #4)
        ]
        
        for pattern in series_patterns:
            match = re.search(pattern, title.lower())
            if match:
                series_number = match.group(1)
                logger.info(f"Found series number in title: {series_number}")
                break
    
    # Also check metadata for additional info
    year = book_info.year if book_info.year else ""
    if not year and book_info.info:
        # Look for year in additional metadata
        for key, values in book_info.info.items():
            if 'year' in key.lower() and values:
                year = str(values[0])
                break
    
    # Extract ISBN from metadata
    isbn = ""
    if book_info.info:
        for key, values in book_info.info.items():
            if 'isbn' in key.lower() and values:
                # Prefer ISBN-13, then ISBN-10
                if '13' in key or (not isbn and values):
                    isbn = str(values[0])
                    # Clean up ISBN (remove hyphens and keep only digits and X)
                    isbn = re.sub(r'[^0-9X]', '', isbn)
                    break
    
    # Clean up publisher name
    publisher = ""
    if book_info.publisher and book_info.publisher != "Unknown Publisher":
        pub_clean = book_info.publisher.strip()
        # Simplify long publisher names
        if "penguin" in pub_clean.lower():
            if "random house" in pub_clean.lower():
                publisher = "Penguin Random House"
            elif "uk" in pub_clean.lower() or "britain" in pub_clean.lower():
                publisher = "Penguin Random House UK"
            else:
                publisher = "Penguin Books"
        elif "dorman" in pub_clean.lower() and "viking" in pub_clean.lower():
            publisher = "Pamela Dorman Books"
        else:
            # Keep original but limit length
            if len(pub_clean) < 30:
                publisher = pub_clean
    
    # Build filename components
    components = []
    
    # 1. Title (cleaned)
    title_clean = title.replace("_", " ")  # Don't underscore the title in comprehensive format
    components.append(title_clean)
    
    # 2. Author (if available)
    if author:
        components.append(author)
    
    # 3. Series info (if available)
    if series_info:
        series_part = series_info
        if series_number:
            series_part += f", {series_number}"
        if year:
            series_part += f", {year}"
        components.append(series_part)
    elif year:
        # Just year if no series info
        components.append(year)
    
    # 4. Publisher (if available and not too long)
    if publisher:
        components.append(publisher)
    
    # 5. ISBN (if available)
    if isbn:
        components.append(isbn)
    
    # Join components with " -- "
    filename_base = " -- ".join(components)
    
    # Add file extension
    file_extension = book_info.format if book_info.format else "epub"
    final_filename = f"{filename_base}.{file_extension}"
    
    # Ensure filename isn't too long (most filesystems limit to 255 characters)
    if len(final_filename) > 240:  # Leave some buffer
        logger.warning(f"Filename too long ({len(final_filename)} chars), truncating...")
        # Keep title and author, truncate other parts
        essential_parts = [components[0]]  # Always keep title
        if len(components) > 1 and author:
            essential_parts.append(components[1])  # Keep author if available
        
        remaining_length = 240 - len(" -- ".join(essential_parts)) - len(f".{file_extension}") - 10  # Buffer
        
        # Add other components if they fit
        for component in components[2:]:
            if len(" -- " + component) <= remaining_length:
                essential_parts.append(component)
                remaining_length -= len(" -- " + component)
            else:
                break
        
        final_filename = " -- ".join(essential_parts) + f".{file_extension}"
    
    logger.info(f"Generated comprehensive filename: '{final_filename}'")
    logger.info(f"=== END FILENAME GENERATION ===")
    
    return final_filename

def search_books(query: str, filters: SearchFilters) -> List[Dict[str, Any]]:
    """Search for books matching the query.
    
    Args:
        query: Search term
        filters: Search filters object
        
    Returns:
        List[Dict]: List of book information dictionaries
    """
    try:
        books = book_manager.search_books(query, filters)
        return [_book_info_to_dict(book) for book in books]
    except Exception as e:
        logger.error_trace(f"Error searching books: {e}")
        return []

def get_book_info(book_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information for a specific book.
    
    Args:
        book_id: Book identifier
        
    Returns:
        Optional[Dict]: Book information dictionary if found
    """
    try:
        book = book_manager.get_book_info(book_id)
        return _book_info_to_dict(book)
    except Exception as e:
        logger.error_trace(f"Error getting book info: {e}")
        return None

def queue_book(book_id: str, priority: int = 0) -> bool:
    """Add a book to the download queue with specified priority.
    
    Args:
        book_id: Book identifier
        priority: Priority level (lower number = higher priority)
        
    Returns:
        bool: True if book was successfully queued
    """
    try:
        book_info = book_manager.get_book_info(book_id)
        book_queue.add(book_id, book_info, priority)
        logger.info(f"Book queued with priority {priority}: {book_info.title}")
        return True
    except Exception as e:
        logger.error_trace(f"Error queueing book: {e}")
        return False

def queue_status() -> Dict[str, Dict[str, Any]]:
    """Get current status of the download queue.
    
    Returns:
        Dict: Queue status organized by status type
    """
    status = book_queue.get_status()
    for _, books in status.items():
        for _, book_info in books.items():
            if book_info.download_path:
                if not os.path.exists(book_info.download_path):
                    book_info.download_path = None

    # Convert Enum keys to strings and properly format the response
    return {
        status_type.value: books
        for status_type, books in status.items()
    }

def get_book_data(book_id: str) -> Tuple[Optional[bytes], BookInfo]:
    """Get book data for a specific book, including its title.
    
    Args:
        book_id: Book identifier
        
    Returns:
        Tuple[Optional[bytes], str]: Book data if available, and the book title
    """
    try:
        book_info = book_queue._book_data[book_id]
        path = book_info.download_path
        with open(path, "rb") as f:
            return f.read(), book_info
    except Exception as e:
        logger.error_trace(f"Error getting book data: {e}")
        if book_info:
            book_info.download_path = None
        return None, book_info if book_info else BookInfo(id=book_id, title="Unknown")

def _book_info_to_dict(book: BookInfo) -> Dict[str, Any]:
    """Convert BookInfo object to dictionary representation."""
    return {
        key: value for key, value in book.__dict__.items()
        if value is not None
    }

def _download_book_with_cancellation(book_id: str, cancel_flag: Event) -> Optional[str]:
    """Download and process a book with cancellation support.
    
    Args:
        book_id: Book identifier
        cancel_flag: Threading event to signal cancellation
        
    Returns:
        str: Path to the downloaded book if successful, None otherwise
    """
    try:
        # Check for cancellation before starting
        if cancel_flag.is_set():
            logger.info(f"Download cancelled before starting: {book_id}")
            return None
            
        book_info = book_queue._book_data[book_id]
        logger.info(f"📚 Starting download: '{book_info.title}' by {book_info.author} ({book_id[:8]})")

        # Generate comprehensive filename (now with early URL resolution)
        logger.info(f"=== FILENAME GENERATION DEBUG ===")
        logger.info(f"USE_BOOK_TITLE setting: {USE_BOOK_TITLE} (type: {type(USE_BOOK_TITLE)})")
        
        if USE_BOOK_TITLE:
            book_name = _generate_comprehensive_filename(book_info, book_id)
        else:
            book_name = f"{book_id}.{book_info.format}"
            logger.info(f"Using book ID for filename: '{book_name}'")
        
        logger.info(f"📁 Final filename: '{book_name}'")
        logger.info(f"=== END FILENAME DEBUG ===")
        
        book_path = TMP_DIR / book_name

        # Check cancellation before download
        if cancel_flag.is_set():
            logger.info(f"Download cancelled before book manager call: {book_id}")
            return None
        
        logger.info(f"🔄 Starting file download for: {book_info.title}")
        
        # Create isolated progress callback with proper book ID binding
        def isolated_progress_callback(progress: float):
            """Isolated progress callback that ensures proper book ID association."""
            try:
                # Always use the captured book_id from this scope, never from global state
                update_download_progress(book_id, progress)
            except Exception as e:
                logger.debug(f"Error in progress callback for {book_id[:8]}: {e}")
        
        success = book_manager.download_book(book_info, book_path, isolated_progress_callback, cancel_flag)
        
        # Stop progress updates
        cancel_flag.wait(0.1)  # Brief pause for progress thread cleanup
        
        if cancel_flag.is_set():
            logger.info(f"Download cancelled during download: {book_id}")
            # Clean up partial download
            if book_path.exists():
                book_path.unlink()
            return None
            
        if not success:
            raise Exception("Unknown error downloading book")

        # Check cancellation before post-processing
        if cancel_flag.is_set():
            logger.info(f"Download cancelled before post-processing: {book_id}")
            if book_path.exists():
                book_path.unlink()
            return None

        logger.info(f"✅ Download successful, processing file: {book_info.title}")

        # IMPORTANT: Fix file extension if there's a mismatch
        if book_path.exists():
            # Check if the downloaded file extension matches the actual file format
            actual_format = _detect_file_format(book_path)
            expected_format = book_info.format or "epub"
            
            if actual_format and actual_format != expected_format:
                logger.warning(f"Format mismatch detected: expected {expected_format}, got {actual_format}")
                
                # Update the book_info format to match reality
                book_info.format = actual_format
                
                # Regenerate filename with correct extension
                if USE_BOOK_TITLE:
                    corrected_book_name = _generate_comprehensive_filename(book_info, book_id)
                else:
                    corrected_book_name = f"{book_id}.{actual_format}"
                
                corrected_book_path = TMP_DIR / corrected_book_name
                
                # Rename the file to have the correct extension
                if corrected_book_path != book_path:
                    logger.info(f"🔧 Correcting filename: {book_path.name} -> {corrected_book_name}")
                    os.rename(book_path, corrected_book_path)
                    book_path = corrected_book_path
                    book_name = corrected_book_name

        if CUSTOM_SCRIPT:
            logger.info(f"🔧 Running custom script: {CUSTOM_SCRIPT}")
            subprocess.run([CUSTOM_SCRIPT, book_path])
            
        intermediate_path = INGEST_DIR / f"{book_id}.crdownload"
        final_path = INGEST_DIR / book_name
        
        if os.path.exists(book_path):
            logger.info(f"📂 Moving book to ingest directory: {book_path.name} -> {final_path.name}")
            try:
                shutil.move(book_path, intermediate_path)
            except Exception as e:
                try:
                    logger.debug(f"Error moving book: {e}, will try copying instead")
                    shutil.move(book_path, intermediate_path)
                except Exception as e:
                    logger.debug(f"Error copying book: {e}, will try copying without permissions instead")
                    shutil.copyfile(book_path, intermediate_path)
                os.remove(book_path)
            
            # Final cancellation check before completing
            if cancel_flag.is_set():
                logger.info(f"Download cancelled before final rename: {book_id}")
                if intermediate_path.exists():
                    intermediate_path.unlink()
                return None
                
            os.rename(intermediate_path, final_path)
            logger.info(f"🎉 Download completed successfully: '{book_info.title}' saved as '{final_path.name}'")
            
        return str(final_path)
    except Exception as e:
        if cancel_flag.is_set():
            logger.info(f"Download cancelled during error handling: {book_id}")
        else:
            logger.error_trace(f"❌ Error downloading book '{book_info.title}' ({book_id[:8]}): {e}")
        return None


def _detect_file_format(file_path: Path) -> Optional[str]:
    """Detect the actual file format based on file content headers."""
    try:
        with open(file_path, 'rb') as f:
            header = f.read(16)
        
        # Check file signatures
        if header.startswith(b'PK\x03\x04'):
            # ZIP-based format (EPUB is a ZIP file)
            return "epub"
        elif header.startswith(b'TPZ'):
            # Topaz format
            return "tpz"
        elif b'BOOKMOBI' in header[:50] or header.startswith(b'TPZ'):
            # MOBI format
            return "mobi"
        elif header.startswith(b'%PDF'):
            # PDF format
            return "pdf"
        elif header.startswith(b'ATAB'):
            # AZW3 format
            return "azw3"
        else:
            # Try to read more data to detect MOBI
            f.seek(0)
            first_kb = f.read(1024)
            if b'BOOKMOBI' in first_kb or b'TPZ' in first_kb:
                return "mobi"
            elif b'ATAB' in first_kb:
                return "azw3"
            
        # If we can't detect, return None to keep original format
        return None
        
    except Exception as e:
        logger.debug(f"Error detecting file format for {file_path}: {e}")
        return None

def update_download_progress(book_id: str, progress: float) -> None:
    """Update download progress with proper book ID tracking."""
    book_queue.update_progress(book_id, progress)
    
    # Get book title for better logging (ensure we're using the correct book)
    try:
        book_info = book_queue._book_data.get(book_id)
        if book_info:
            # Use the original title from when book was queued, not extracted title
            book_title = book_info.title[:30] + "..." if len(book_info.title) > 30 else book_info.title
            # Add book ID suffix for tracking in logs
            book_display = f"{book_title} [{book_id[:8]}]"
        else:
            book_display = f"Unknown Book [{book_id[:8]}]"
    except Exception as e:
        logger.debug(f"Error getting book title for progress: {e}")
        book_display = book_id[:8]  # Fallback to short book ID
    
    # Log progress at meaningful milestones only (reduce log spam)
    # Use threshold-based logging to prevent duplicate progress logs
    progress_int = int(progress)
    
    if progress >= 100.0:
        logger.info(f"Download complete: {book_display} (100%)")
    elif progress_int >= 90 and progress_int % 10 == 0:
        logger.info(f"Download progress: {book_display} ({progress_int}%)")
    elif progress_int >= 50 and progress_int % 25 == 0:
        logger.info(f"Download progress: {book_display} ({progress_int}%)")
    elif progress_int >= 25 and progress_int % 25 == 0:
        logger.info(f"Download progress: {book_display} ({progress_int}%)")

def cancel_download(book_id: str) -> bool:
    """Cancel a download.
    
    Args:
        book_id: Book identifier to cancel
        
    Returns:
        bool: True if cancellation was successful
    """
    return book_queue.cancel_download(book_id)

def set_book_priority(book_id: str, priority: int) -> bool:
    """Set priority for a queued book.
    
    Args:
        book_id: Book identifier
        priority: New priority level (lower = higher priority)
        
    Returns:
        bool: True if priority was successfully changed
    """
    return book_queue.set_priority(book_id, priority)

def reorder_queue(book_priorities: Dict[str, int]) -> bool:
    """Bulk reorder queue.
    
    Args:
        book_priorities: Dict mapping book_id to new priority
        
    Returns:
        bool: True if reordering was successful
    """
    return book_queue.reorder_queue(book_priorities)

def get_queue_order() -> List[Dict[str, any]]:
    """Get current queue order for display."""
    return book_queue.get_queue_order()

def get_active_downloads() -> List[str]:
    """Get list of currently active downloads."""
    return book_queue.get_active_downloads()

def clear_completed() -> int:
    """Clear all completed downloads from tracking."""
    return book_queue.clear_completed()

def _process_single_download(book_id: str, cancel_flag: Event) -> None:
    """Process a single download job."""
    try:
        book_queue.update_status(book_id, QueueStatus.DOWNLOADING)
        download_path = _download_book_with_cancellation(book_id, cancel_flag)
        
        if cancel_flag.is_set():
            book_queue.update_status(book_id, QueueStatus.CANCELLED)
            return
            
        if download_path:
            book_queue.update_download_path(book_id, download_path)
            new_status = QueueStatus.AVAILABLE
        else:
            new_status = QueueStatus.ERROR
            
        book_queue.update_status(book_id, new_status)
        
        logger.info(
            f"Book {book_id} download {'successful' if download_path else 'failed'}"
        )
        
    except Exception as e:
        if not cancel_flag.is_set():
            logger.error_trace(f"Error in download processing: {e}")
            book_queue.update_status(book_id, QueueStatus.ERROR)
        else:
            logger.info(f"Download cancelled: {book_id}")
            book_queue.update_status(book_id, QueueStatus.CANCELLED)

def concurrent_download_loop() -> None:
    """Main download coordinator using ThreadPoolExecutor for concurrent downloads."""
    logger.info(f"Starting concurrent download loop with {MAX_CONCURRENT_DOWNLOADS} workers")
    
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS, thread_name_prefix="BookDownload") as executor:
        active_futures: Dict[Future, str] = {}  # Track active download futures
        
        while True:
            # Clean up completed futures
            completed_futures = [f for f in active_futures if f.done()]
            for future in completed_futures:
                book_id = active_futures.pop(future)
                try:
                    future.result()  # This will raise any exceptions from the worker
                except Exception as e:
                    logger.error_trace(f"Future exception for {book_id}: {e}")
            
            # Start new downloads if we have capacity
            while len(active_futures) < MAX_CONCURRENT_DOWNLOADS:
                next_download = book_queue.get_next()
                if not next_download:
                    break
                    
                book_id, cancel_flag = next_download
                logger.info(f"Starting concurrent download: {book_id}")
                
                # Submit download job to thread pool
                future = executor.submit(_process_single_download, book_id, cancel_flag)
                active_futures[future] = book_id
            
            # Brief sleep to prevent busy waiting
            time.sleep(MAIN_LOOP_SLEEP_TIME)

# Start concurrent download coordinator
download_coordinator_thread = threading.Thread(
    target=concurrent_download_loop,
    daemon=True,
    name="DownloadCoordinator"
)
download_coordinator_thread.start()

logger.info(f"Download system initialized with {MAX_CONCURRENT_DOWNLOADS} concurrent workers")
