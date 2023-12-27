import subprocess

def db(commands: list[str]) -> list[str]:
    # p = subprocess.Popen()
    p = subprocess.Popen(["./zig-out/bin/db-tutorial"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
    try:
        # commands.
        cs = ('\n'.join(commands)).encode()
        outs, errs = p.communicate(cs, timeout=5)
        return outs.decode().split('\n')
    except subprocess.TimeoutExpired:
        p.kill()
        return []

     
