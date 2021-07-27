from __future__ import print_function

import sys
import struct
import gzip
from typing import Optional


def create_index(tfrecord_file: str, index_file: str,
                 compression_type: Optional[str] = None) -> None:
    """Create index from the tfrecords file.

    Stores starting location (byte) and length (in bytes) of each
    serialized record.

    Params:
    -------
    tfrecord_file: str
        Path to the TFRecord file.

    index_file: str
        Path where to store the index file.
    """
    if compression_type == "gzip":
        infile = gzip.open(tfrecord_file, "rb")
    elif compression_type is None:
        infile = open(tfrecord_file, "rb")
    else:
        raise ValueError("compression_type should be either 'gzip' or None")
    outfile = open(index_file, "w")

    while True:
        current = infile.tell()
        try:
            byte_len = infile.read(8)
            if len(byte_len) == 0:
                break
            infile.read(4)
            proto_len = struct.unpack("q", byte_len)[0]
            infile.read(proto_len)
            infile.read(4)
            outfile.write(str(current) + " " + str(infile.tell() - current) + "\n")
        except:
            print("Failed to parse TFRecord.")
            break
    infile.close()
    outfile.close()


def main():
    if len(sys.argv) < 3:
        print("Usage: tfrecord2idx <tfrecord path> <index path> <optional compression_type>")
        sys.exit()

    if len(sys.argv) == 3:
        create_index(sys.argv[1], sys.argv[2], None)
    elif len(sys.argv) == 4:
        create_index(sys.argv[1], sys.argv[2], sys.argv[3])


if __name__ == "__main__":
    main()
