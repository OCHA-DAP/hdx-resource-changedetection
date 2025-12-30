from typing import Any, Optional

from pyasn1.type.namedtype import OptionalNamedType

zip_signature = b"PK\x03\x04"

signatures = {
    "json": [b"[", b" [", b"{", b" {"],
    "geojson": [b"[", b" [", b"{", b" {"],
    "shp": [zip_signature],
    "xls": [b"\xd0\xcf\x11\xe0"],
    "xlsx": [zip_signature],
}

ignore_mimetypes = ["application/octet-stream", "application/binary"]
mimetypes = {
    "json": ["application/json"],
    "geojson": ["application/json", "application/geo+json"],
    "shp": ["application/zip", "application/x-zip-compressed"],
    "csv": ["text/csv", "application/zip", "application/x-zip-compressed"],
    "xls": ["application/vnd.ms-excel"],
    "xlsx": ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
}


def check_signature(signature: bytes, resource_format: str) -> bool:
    expected_signatures = signatures.get(resource_format)
    if expected_signatures is not None:
        if any(signature[: len(x)] == x for x in expected_signatures):
            return True
    return False


def check_mimetype(mimetype: str, resource_format: str) -> bool:
    if mimetype not in ignore_mimetypes:
        expected_mimetypes = mimetypes.get(resource_format)
        if expected_mimetypes is not None:
            if any(x in mimetype for x in expected_mimetypes):
                return True
    return False


def get_http_size(headers: Any) -> Optional[int]:
    content_encoding = headers.get("Content-Encoding")
    if not content_encoding:
        size = headers.get("Content-Length")
        if size:
            return int(size)
    return None


def is_xlsx_file(
    url: str,
    resource_format: str,
    mimetype: str,
    xlsx_url_ignore: Optional[str]
) -> bool:
    if (
        resource_format == "xlsx"
        and (mimetype == mimetypes["xlsx"][0] or mimetype in ignore_mimetypes)
        and (xlsx_url_ignore not in url if xlsx_url_ignore else True)
    ):
        return True
    return False
