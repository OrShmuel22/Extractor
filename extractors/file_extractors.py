
from InterfaceABS.file_extractor_abs import FileExtractor
import tarfile
import bz2
import lzma
import gzip
import zipfile
import py7zr
import rarfile
import zstandard as zstd
import lz4


class PasswordRequired(Exception):
    pass


class DamagedArchive(Exception):
    pass


class TarExtractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with tarfile.open(compressed_file_path, "r") as source:
                source.extractall(path_to_uncompressed_file)
            return True
        except tarfile.ReadError:
            raise DamagedArchive("File is damaged")
        except tarfile.ExtractError:
            raise PasswordRequired("File is password-protected")


class Bzip2Extractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with bz2.open(compressed_file_path, "rb") as source, open(path_to_uncompressed_file, "wb") as target:
                target.write(source.read())
            return True
        except (IOError, OSError):
            raise DamagedArchive("File is damaged")
        # Implement password checking for Bzip2 files if necessary


class XzExtractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with lzma.open(compressed_file_path, "rb") as source, open(path_to_uncompressed_file, "wb") as target:
                target.write(source.read())
            return True
        except (lzma.LZMAError, OSError):
            raise DamagedArchive("File is damaged")
        # Implement password checking for Xz files if necessary


class GzipExtractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with gzip.open(compressed_file_path, "rb") as source, open(path_to_uncompressed_file, "wb") as target:
                target.write(source.read())
            return True
        except gzip.BadGzipFile:
            raise DamagedArchive("File is damaged")
        # Implement password checking for Gzip files if necessary


class ZipExtractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with zipfile.ZipFile(compressed_file_path, "r") as source:
                if source.testzip() is not None:
                    raise DamagedArchive("File is damaged")
                for zinfo in source.infolist():
                    if zinfo.flag_bits & 0x1:
                        raise PasswordRequired("File is password-protected")
                source.extractall(path_to_uncompressed_file)
            return True
        except zipfile.BadZipFile:
            raise DamagedArchive("File is damaged")


class SevenZipExtractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with py7zr.SevenZipFile(compressed_file_path, mode='r') as source:
                if source.needs_password():
                    raise PasswordRequired("File is password-protected")
                source.extractall(path_to_uncompressed_file)
            return True
        except py7zr.Bad7zFile:
            raise DamagedArchive("File is damaged")


class RarExtractor(FileExtractor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with rarfile.RarFile(compressed_file_path, "r") as source:
                if source.needs_password():
                    raise PasswordRequired("File is password-protected")
                source.extractall(path_to_uncompressed_file)
            return True
        except rarfile.BadRarFile:
            raise DamagedArchive("File is damaged")


class ZstdExtractor(FileExtractor):
    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with open(compressed_file_path, 'rb') as compressed_file, open(path_to_uncompressed_file,
                                                                           'wb') as output_file:
                dctx = zstd.ZstdDecompressor()
                dctx.copy_stream(compressed_file, output_file)
        except zstd.ZstdError as e:
            raise DamagedArchive("Archive is damaged") from e


class Lz4Extractor(FileExtractor):
    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        try:
            with open(compressed_file_path, 'rb') as compressed_file, open(path_to_uncompressed_file,
                                                                           'wb') as output_file:
                output_file.write(lz4.frame.decompress(compressed_file.read()))
        except lz4.frame.LZ4FrameError as e:
            raise DamagedArchive("Archive is damaged") from e

