import sys
import time

def main():
    SEARCH  = b'),('
    REPLACE = b')\n('
    CHUNK_SIZE = 1024 * 1024 * 4  # 4MB

    overlap = b''
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    stderr = sys.stderr

    total_bytes = 0
    start_time = time.time()

    while True:
        chunk = stdin.read(CHUNK_SIZE)
        if not chunk:
            break

        total_bytes += len(chunk)

        # Simple Progress Report every ~100MB
        if total_bytes % (1024 * 1024 * 100) < CHUNK_SIZE:
            elapsed = time.time() - start_time
            mb = total_bytes / (1024 * 1024)
            speed = mb / elapsed if elapsed > 0 else 0
            stderr.write(f"\rProcessing: {mb:.1f} MB | Speed: {speed:.1f} MB/s")
            stderr.flush()

        data = overlap + chunk
        processed = data.replace(SEARCH, REPLACE)

        tail_len = len(SEARCH)
        if len(processed) >= tail_len:
            to_write = processed[:-tail_len]
            overlap = processed[-tail_len:]
            stdout.write(to_write)
        else:
            overlap = processed

    if overlap:
        stdout.write(overlap)

    stderr.write(f"\nDone! Processed {total_bytes / (1024*1024):.1f} MB.\n")

if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.stderr.close()
