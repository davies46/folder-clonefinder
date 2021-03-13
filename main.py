from __future__ import with_statement

import argparse
import contextlib
import operator
import os
import re
import threading

lock = threading.Lock()
paths = {}
sizes = {}
# mounted_filesystems = ['/', '/media/home', '/media/UserData', '/media/win7backup', '/media/4tb-ext', '/media/pdavies/SeagateV2', '/media/pdavies/Hitachi']
# mounted_filesystems = ['/media/4tb-ext',  '/media/pdavies/Hitachi']
threads = list()
subtree_visit_num = 0
no_access = []
not_found = []
not_folder = []


units = {"B": 1, "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}


class Args:
    def __init__(self):
        parser = argparse.ArgumentParser('Find duplicate folders in filesystem')
        # parser.add_argument('infile', metavar='input file', nargs=1, help='File to process')
        parser.add_argument('-e', '--exclude', default='/run/timeshift/backup', help='Comma separated list of root-level folders to exclude')
        parser.add_argument('-m', '--minsize', default='1G', help='Smallest size to care about')

        # parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
        # parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)

        parser_args = parser.parse_args()
        # print(parser_args)
        if parser_args.exclude:
            self.exclude = parser_args.exclude.split(',')
        else:
            self.exclude = []
        self.minsize = Args.parse_size(parser_args.minsize)

    @staticmethod
    def parse_size(size):
        # print('Parse', size)
        size = size.upper()
        if not size.endswith('B'):
            size += 'B'
        if not re.match(r' ', size):
            size = re.sub(r'([KMGT]?B)', r' \1', size)
        number, unit = [string.strip() for string in size.split()]
        return int(float(number)*units[unit])


args = Args()
print('Min folder size', args.minsize)
print('Exclude root folders', args.exclude)
exclude_folders = args.exclude

print("Filesystem\tMounted on\tUse%\tIUse%")
mounted_filesystems = []
with contextlib.closing(open('/etc/mtab')) as fp:
    for m in fp:
        fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno = m.split()
        if fs_spec.startswith('/') and '/loop' not in fs_spec:
            try:
                r = os.statvfs(fs_file)
                if r.f_files > 0:
                    block_usage_pct = 100.0 - (float(r.f_bavail) / float(r.f_blocks) * 100)
                    inode_usage_pct = 100.0 - (float(r.f_favail) / float(r.f_files) * 100)
                    if fs_file in exclude_folders:
                        print('Exclude folder', fs_file)
                        pass
                    else:
                        mounted_filesystems.append(fs_file)
                        print("%s\t%s\t\t%d%%\t%d%%" % (fs_spec, fs_file, block_usage_pct, inode_usage_pct))
            except PermissionError:
                pass


def isChild(p1, p2):
    return p1.startswith(p2) and p1 != p2


class Duplicate:
    def __init__(self, size, path1, path2):
        self.__path1 = path1
        self.__path2 = path2
        self.size = size

    def __str__(self):
        return '%d, <%s> and <%s>' % (self.size, self.__path1, self.__path2)

    def isChildOf(self, other_dupe):
        # Either path1 or path2 of either dupe might be a different device, so a different root, but they must have a root in common
        # so if we compare all permutations we should catch it
        # Comparison is a,b with c,d so a child would mean 'a' is a child of 'c' and 'b' of 'd' OR
        # 'a' of 'd' and 'b' of 'c'
        we_are_child = isChild(self.__path1, other_dupe.__path1) and isChild(self.__path2, other_dupe.__path2)
        we_are_child = we_are_child or (isChild(self.__path1, other_dupe.__path2) and isChild(self.__path2, other_dupe.__path1))
        return we_are_child


duplicates = []


def searchTree(path):
    global subtree_visit_num
    total_size = 0
    digest = 0
    subtree_visit_num += 1
    if subtree_visit_num % 100000 == 0:
        print('Visiting subtree #%d. Fat folder paths added: %d. Duplicates found: %d' % (subtree_visit_num, len(paths), len(duplicates)))
    try:
        # Examine every file and folder at this level
        with os.scandir(path) as dir_entries:
            for dir_entry in dir_entries:
                # Ignore symbolic links
                if not dir_entry.is_symlink():
                    basename = os.path.basename(dir_entry.path)
                    digest += hash(basename)
                    if dir_entry.is_file():
                        # print('File', dir_entry)
                        total_size += os.path.getsize(dir_entry.path)
                    else:
                        # print('Dir ', dir_entry)
                        folder_digest, folder_size = searchTree(dir_entry.path)
                        digest += folder_digest
                        total_size += folder_size

        if total_size > args.minsize and digest != 0:
            with lock:
                if digest in paths:
                    # print('Look at', path, '\nand', paths[digest], 'size:', sizes[digest])
                    # If either path of a duplicate wholly contains either path of another duplicate, then it's a subtree
                    new_dupe = Duplicate(total_size, path, paths[digest])
                    orphan = True
                    children = []
                    for dupe in duplicates:
                        if new_dupe.isChildOf(dupe):
                            # We have a parent, so our existence is pointless
                            assert not children, 'We have a child and a parent. How did the child survive here without the parent already deleting it?'
                            print('We have a parent')
                            orphan = False
                        if dupe.isChildOf(new_dupe):
                            # We're the parent of an existing duplicate, so that child is now useless and should be removed
                            assert orphan, 'We have a parent and a child. How did the child survive here without the parent already deleting it?'
                            print('We have a child')
                            children.append(dupe)
                    if orphan:
                        print('Add dupe', new_dupe)
                        duplicates.append(new_dupe)
                    for child in children:
                        print('Remove child', dupe)
                        duplicates.remove(child)
                else:
                    drive_threads = []
                    for th in threads:
                        if th.is_alive():
                            drive_threads.append(th.name)
                    # print('Threads ', len(ths))
                    paths[digest] = path
                    sizes[digest] = total_size
    except PermissionError:
        no_access.append(path)
    except FileNotFoundError:
        not_found.append(path)
    except NotADirectoryError:
        not_folder.append(path)

    return digest, total_size


if __name__ == "__main__":
    for mounted_filesystem in mounted_filesystems:
        x = threading.Thread(target=searchTree, args=(mounted_filesystem,), daemon=True, name=mounted_filesystem)
        threads.append(x)
        x.start()

    for index, thread in enumerate(threads):
        thread.join()

    print(len(paths), 'candidate paths added')

    sorted_duplicates = sorted(duplicates, key=operator.attrgetter('size'), reverse=True)
    for duplicate in sorted_duplicates:
        print(duplicate)
    print('All done')
