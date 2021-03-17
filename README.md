# Search the filesystem for duplicate folders

It will report back the largest folders it can find, but none of those folders' children.

Search different physical devices concurrently

## Example:

> ./main.py -m 1G -x /media/backup

Report duplicate folders. Ignore folders below 1GB. Ignore the physical drive at /media/backup

## Usage:
- -e, --exclude: Comma separated list of root-level folders to exclude
- -m, --minsize: Smallest size to care about
- -x, --exclude-subfolders: Folders to exclude from search
    If you exclude a subfolder .e.g. /../..snapshot/ you might be duplicating a subfolder in there that you don't even want in the backup, so probably use sparingly.

## Output:
- List of duplicate folders, in reverse size order (largest first)

## Installation
If you know your way around python, just run the script. If you don't, not sure but you need to at least install python (3.something) and maybe use **pip install** _module_ on the modules that it complains are missing when you try to run it.
