import logging
import os
from typing import Optional

import pypdf
import pytesseract
from pdf2image import convert_from_path

logger = logging.getLogger(__name__)

class PDFExtractionError(ValueError):
    pass

class PDFExtractor:
    """
    Extracts text from earnings PDFs, enforcing Rule 3 (OCR Fallback).
    """

    def __init__(self, threshold: int = 200):
        self.threshold = threshold

    def _extract_text_pypdf2(self, file_path: str) -> str:
        """Attempt fast text extraction using pypdf."""
        text = ""
        try:
            with open(file_path, "rb") as f:
                reader = pypdf.PdfReader(f)
                num_pages = len(reader.pages)
                for i in range(num_pages):
                    page_text = reader.pages[i].extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            logger.warning(f"pypdf2 extraction failed for {file_path}: {e}")
        return text.strip()

    def _extract_text_ocr(self, file_path: str) -> str:
        """Fallback to OCR using pdf2image and pytesseract."""
        logger.info(f"Using OCR fallback for {file_path}...")
        text = ""
        try:
            pages = convert_from_path(file_path, dpi=300)
            for i, page in enumerate(pages):
                logger.debug(f"OCR processing page {i+1}/{len(pages)}...")
                page_text = pytesseract.image_to_string(page)
                text += page_text + "\n"
        except Exception as e:
            logger.error(f"OCR extraction failed for {file_path}: {e}")
            
        return text.strip()

    def extract(self, file_path: str) -> str:
        """
        Rule 3 enforcement:
        1. Parse with pypdf2.
        2. Check if len(text) >= threshold (200).
        3. If not, use OCR.
        4. If OCR also fails (len < threshold), raise ValueError, log, skip.
        """
        logger.info(f"Extracting text from {file_path}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Try fast extraction
        text = self._extract_text_pypdf2(file_path)
        
        # Rule 3: Check len(text) >= 200 after pypdf2
        if len(text) < self.threshold:
            logger.warning(f"Extracted text length {len(text)} < {self.threshold}. Triggering OCR fallback.")
            # Trigger OCR fallback
            text = self._extract_text_ocr(file_path)
            
            # Rule 3: If OCR also fails
            if len(text) < self.threshold:
                error_msg = f"Extraction failed for {file_path}: length {len(text)} < {self.threshold} even after OCR."
                logger.error(error_msg)
                raise PDFExtractionError(error_msg)
                
        logger.info(f"Successfully extracted {len(text)} characters from {file_path}.")
        return text
