import subprocess
import sys

class Scanner:
    @staticmethod
    def run_trufflehog(target_dir):
        print(f"Scanning {target_dir} with TruffleHog...")
        try:
            # Use Popen to stream output
            process = subprocess.Popen(
                ["trufflehog", "filesystem", target_dir, "--json", "--results=verified", "--concurrency", "8"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )

            # Read stdout line by line as it is generated
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None:
                    break
                if line:
                    line_str = line.strip()
                    print(line_str) # Stream to console
                    yield line_str

            # Check for errors
            stderr = process.stderr.read()
            if stderr:
                print(f"TruffleHog stderr: {stderr}", file=sys.stderr)
            
        except Exception as e:
            print(f"TruffleHog failed: {e}")
            yield ""
