import re
import struct
from typing import Tuple, List, Dict

EOCD_MIN_SIZE = 22
MAX_COMMENT_SIZE = 65535
EOCD_SIGNATURE = b"PK\x05\x06"
CD_HEADER_SIGNATURE = b"PK\x01\x02"
EXCEL_PATTERNS = (
    re.compile(r"^xl/worksheets/sheet\d+\.xml$"), # The Grid Data
    re.compile(r"^xl/sharedStrings\.xml$"),       # The Text Data
    re.compile(r"^xl/workbook\.xml$"),            # The Structure
    re.compile(r"^xl/_rels/workbook\.xml\.rels$") # The Map
)

async def fetch_zip_hash_tail(size):
    read_size = min(size, MAX_COMMENT_SIZE + EOCD_MIN_SIZE)
    headers_tail = {'Range': f'bytes={size - read_size}-'}

    async with session.get(url, headers=headers_tail) as resp:
        tail_data = await resp.read()

def get_zip_crcs(buffer: bytearray, size: int) -> Dict[str, int]:
    read_size = min(size, MAX_COMMENT_SIZE + EOCD_MIN_SIZE)
    tail_data = buffer[read_size:]
    total_records, cd_offset, cd_end = find_ecod_signature(tail_data)
    cd_data = buffer[cd_offset:cd_end]
    return parse_central_directory(cd_data, total_records)

def find_ecod_signature(tail_data: bytes) -> Tuple[int, int, int]:
    # Find EOCD Signature
    eocd_pos = tail_data.rfind(EOCD_SIGNATURE)
    if eocd_pos == -1:
        logger.error("ZIP End of Central Directory signature not found.")
        return None

    # Unpack EOCD
    eocd = tail_data[eocd_pos : eocd_pos + 22]
    _, _, _, _, total_records, cd_size, cd_offset, _ = struct.unpack('<4sHHHHIIH', eocd)
    cd_end = cd_offset + cd_size
    return total_records, cd_offset, cd_end

async def fetch_central_directory(cd_offset, cd_end):
    headers_cd = {'Range': f'bytes={cd_offset}-{cd_end-1}'}
    async with session.get(url, headers=headers_cd) as resp:
        cd_data = await resp.read()


def parse_full_file(data):
    """Fallback if server forces a full download."""
    eocd_pos = data.rfind(EOCD_SIGNATURE)
    if eocd_pos == -1: raise ValueError("Not a valid ZIP.")

    eocd = data[eocd_pos : eocd_pos + 22]
    _, _, _, _, total_records, cd_size, cd_offset, _ = struct.unpack('<4sHHHHIIH', eocd)

    cd_data = data[cd_offset : cd_offset + cd_size]
    return parse_central_directory(cd_data, total_records)

def parse_central_directory(data: bytes, num_records: int) -> Dict[str, int]:
    results = {}
    offset = 0
    for _ in range(num_records):
        if offset + 46 > len(data): break
        if data[offset:offset+4] != CD_HEADER_SIGNATURE: break

        fields = struct.unpack('<4sHHHHHHIIIHHHHHII', data[offset:offset+46])
        crc32 = fields[9]
        filepath_len = fields[12]
        extra_len = fields[13]
        comment_len = fields[14]

        filepath = data[offset+46 : offset+46+filepath_len].decode("utf-8", "replace")
        results[filepath] = crc32

        offset += 46 + filepath_len + extra_len + comment_len
    return results

def match_excel_patterns(filepath: str) -> bool:
    for pattern in EXCEL_PATTERNS:
        if pattern.match(filepath):
            return True
    return False