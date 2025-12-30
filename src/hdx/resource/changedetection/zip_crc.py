import re
import struct
from typing import Tuple, Dict, Optional

from hdx.resource.changedetection.retrieval_utilities import is_xlsx_file

EOCD_MIN_SIZE = 22
MAX_COMMENT_SIZE = 65535
EOCD_SIGNATURE = b"PK\x05\x06"
CD_HEADER_SIGNATURE = b"PK\x01\x02"
EXCEL_PATTERNS = (
    re.compile(r"^xl/worksheets/sheet\d+\.xml$"), # The Grid Data
    re.compile(r"^xl/sharedStrings\.xml$"),       # The Text Data
    re.compile(r"^xl/workbook\.xml$"),            # The Structure
)

def find_eocd_signature(tail_data: bytes) -> Tuple[int, int, int]:
    # Find EOCD Signature
    eocd_pos = tail_data.rfind(EOCD_SIGNATURE)
    if eocd_pos == -1:
        return -1, -1, -1

    # Unpack EOCD
    eocd = tail_data[eocd_pos : eocd_pos + 22]
    _, _, _, _, total_records, cd_size, cd_offset, _ = struct.unpack('<4sHHHHIIH', eocd)
    cd_end = cd_offset + cd_size
    return total_records, cd_offset, cd_end

def parse_central_directory(data: bytes, num_records: int) -> Dict[str, int]:
    results = {}
    offset = 0
    for _ in range(num_records):
        if offset + 46 > len(data):
            break
        if data[offset : offset + 4] != CD_HEADER_SIGNATURE:
            break

        fields = struct.unpack("<4sHHHHHHIIIHHHHHII", data[offset : offset + 46])
        crc32 = fields[7]
        filepath_len = fields[10]
        extra_len = fields[11]
        comment_len = fields[12]

        filepath = data[offset + 46 : offset + 46 + filepath_len].decode(
            "utf-8", "replace"
        )
        if not filepath.endswith("/"):
            results[filepath] = crc32

        offset += 46 + filepath_len + extra_len + comment_len
    return results

def get_zip_crcs(buffer: bytes, size: int) -> Dict[str, int]:
    read_size = min(size, MAX_COMMENT_SIZE + EOCD_MIN_SIZE)
    tail_data = buffer[size - read_size :]
    num_records, cd_offset, cd_end = find_eocd_signature(tail_data)
    if num_records == -1:
        return {}
    cd_data = buffer[cd_offset:cd_end]
    return parse_central_directory(cd_data, num_records)

def match_excel_patterns(filepath: str) -> bool:
    for pattern in EXCEL_PATTERNS:
        if pattern.match(filepath):
            return True
    return False

def get_crc_sum(url: str, resource_format: str, mimetype: str, file_crcs: Dict[str, int], xlsx_url_ignore: Optional[str]) -> str:
    crc_sum = 0
    if is_xlsx_file(url, resource_format, mimetype, xlsx_url_ignore):
        for filepath in file_crcs:
            if match_excel_patterns(filepath):
                crc_sum ^= file_crcs[filepath]
    else:
        for crc in file_crcs.values():
            crc_sum ^= crc
    if crc_sum:
        return f"{crc_sum:08x}"
    return ""

def get_zip_tail_header(size: int) -> Dict[str, str]:
    read_size = min(size, MAX_COMMENT_SIZE + EOCD_MIN_SIZE)
    return {'Range': f'bytes={size - read_size}-'}

def get_zip_cd_header(tail_data: bytes) -> Tuple[int, Dict]:
    total_records, cd_offset, cd_end = find_eocd_signature(tail_data)
    return total_records, {'Range': f'bytes={cd_offset}-{cd_end-1}'}
