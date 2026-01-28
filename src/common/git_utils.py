import subprocess

def get_git_commit_hash():
    """
    Returns the current short git commit hash.
    """
    try:
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip().decode('utf-8')
    except Exception:
        return "unknown_commit"
